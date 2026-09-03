// tests/seatlink.test.mjs — the impure glue this file adds around bridge/lib/reconcile.mjs:
// fetchUnreadIds (the actual pot HTTP call) and formatPotMail's conditional ACK wording. The
// pure filtering/fail-open policy itself is covered exhaustively in tests/reconcile.test.mjs;
// this file proves the wiring around it, not the policy again.
//
// seatlink.mjs performs top-level work on import in production (fs.mkdirSync(STATE_DIR)),
// which is fine under test — it only creates a directory under HOME, matching the file's own
// production behavior. Nothing here imports node:net or touches the Herdr socket: fetchUnreadIds
// and formatPotMail don't need it, and drainDeferred's Herdr-socket dependency (via
// injectPotMail -> seatMap -> rpc) is exercised only far enough to prove the reconcile step
// runs and the queue is cleared — not full end-to-end delivery, which belongs to an operator
// smoke test against a real Herdr socket, not this unit suite.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  deferred,
  drainDeferred,
  fetchUnreadIds,
  formatPotMail,
  seatMupotConfig,
} from '../bridge/seatlink.mjs'

describe('formatPotMail', () => {
  it('asks for an ACK when the message carries a request_id and is not itself an ack', () => {
    const text = formatPotMail('muvps_kasra', { id: 'm1', seq: 10, request_id: 'req-1', kind: 'message' })
    expect(text).toContain('send the required correlated ACK')
  })

  it('does NOT ask for an ACK when the message has no request_id', () => {
    const text = formatPotMail('muvps_kasra', { id: 'm1', seq: 10, request_id: null, kind: 'message' })
    expect(text).not.toContain('send the required correlated ACK')
    expect(text).toContain('No ACK is required')
  })

  it('does NOT ask for an ACK when the message is itself an ack, even if it carries request_id', () => {
    // Defense in depth: the mupot server now refuses request_id on kind:"ack" at the source
    // (mumega-com#1179 defect A), but this offer text should not compound a hand-composed or
    // already-in-flight envelope that slipped through by demanding yet another reply.
    const text = formatPotMail('muvps_kasra', { id: 'm1', seq: 10, request_id: 'req-1', kind: 'ack' })
    expect(text).not.toContain('send the required correlated ACK')
    expect(text).toContain('No ACK is required')
  })

  it('asks for an ACK when expects_reply is true and reply_basis is request_id_field', () => {
    const text = formatPotMail('muvps_kasra', {
      id: 'm1',
      seq: 10,
      request_id: 'req-1',
      kind: 'message',
      expects_reply: true,
      reply_basis: 'request_id_field',
    })
    expect(text).toContain('send the required correlated ACK')
  })

  it('does NOT ask for an ACK when expects_reply is true but reply_basis is body_token (prose quote)', () => {
    const text = formatPotMail('muvps_kasra', {
      id: 'm1',
      seq: 10,
      request_id: null,
      kind: 'message',
      expects_reply: true,
      reply_basis: 'body_token',
    })
    expect(text).not.toContain('send the required correlated ACK')
    expect(text).toContain('No ACK is required')
  })

  it('does NOT ask for an ACK when expects_reply is false (e.g. terminal ack)', () => {
    const text = formatPotMail('muvps_kasra', {
      id: 'm1',
      seq: 10,
      request_id: 'ack-id-1',
      kind: 'ack',
      expects_reply: false,
      reply_basis: null,
    })
    expect(text).not.toContain('send the required correlated ACK')
    expect(text).toContain('No ACK is required')
  })
})

describe('fetchUnreadIds', () => {
  const SEAT = 'test-seat-fetch'

  beforeEach(() => {
    seatMupotConfig.set(SEAT, { token: 'tok-123', api: 'https://pot.test' })
  })
  afterEach(() => {
    seatMupotConfig.delete(SEAT)
    vi.unstubAllGlobals()
  })

  it('calls the peek endpoint with the seat bearer token and returns unread ids', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ ok: true, messages: [{ id: 'a' }, { id: 'b' }], remaining: 0 }),
    })
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchUnreadIds(SEAT)

    expect(result).toEqual({ ids: new Set(['a', 'b']), complete: true })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, opts] = fetchMock.mock.calls[0]
    expect(url).toBe('https://pot.test/api/inbox?peek=1&limit=100')
    expect(opts.headers.Authorization).toBe('Bearer tok-123')
  })

  it('throws on a non-OK HTTP response (caller fails open on this)', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 401 }))
    await expect(fetchUnreadIds(SEAT)).rejects.toThrow(/401/)
  })

  it('throws on a malformed (non-ok / non-array) JSON body', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => ({ ok: false }) }))
    await expect(fetchUnreadIds(SEAT)).rejects.toThrow(/malformed/)
  })

  it('throws when no seat config was ever registered', async () => {
    await expect(fetchUnreadIds('never-registered-seat')).rejects.toThrow(/no mupot config/)
  })

  it('pages deterministically across multiple pages when complete is false using since_seq', async () => {
    const fetchMock = vi.fn()
      // Page 1: 2 messages, complete: false, ends at seq 102
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ok: true,
          complete: false,
          remaining: 1,
          messages: [{ id: 'msg-1', seq: 101 }, { id: 'msg-2', seq: 102 }],
        }),
      })
      // Page 2: 1 message, complete: true, ends at seq 103
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ok: true,
          complete: true,
          remaining: 0,
          messages: [{ id: 'msg-3', seq: 103 }],
        }),
      })
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchUnreadIds(SEAT)

    expect(result).toEqual({ ids: new Set(['msg-1', 'msg-2', 'msg-3']), complete: true })
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock.mock.calls[0][0]).toBe('https://pot.test/api/inbox?peek=1&limit=100')
    expect(fetchMock.mock.calls[1][0]).toBe('https://pot.test/api/inbox?peek=1&limit=100&since_seq=102')
  })

  it('terminates immediately and does not page when complete is true on the first page', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        ok: true,
        complete: true,
        remaining: 0,
        messages: [{ id: 'single', seq: 50 }],
      }),
    })
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchUnreadIds(SEAT)

    expect(result).toEqual({ ids: new Set(['single']), complete: true })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('determines complete: true on un-upgraded producer when remaining is 0 without complete flag', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        ok: true,
        remaining: 0,
        messages: [{ id: 'legacy-msg', seq: 25 }],
      }),
    })
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchUnreadIds(SEAT)

    expect(result).toEqual({ ids: new Set(['legacy-msg']), complete: true })
  })

  it('marks complete: false when sequence cursor does not advance', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        ok: true,
        complete: false,
        remaining: 10,
        messages: [{ id: 'stuck', seq: 50 }],
      }),
    })
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchUnreadIds(SEAT)

    expect(result).toEqual({ ids: new Set(['stuck']), complete: false })
    // First page fetched, seen seq 50; second page returned same seq 50 -> breaks immediately
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('marks complete: false when the 20-page ceiling is reached', async () => {
    // Producer keeps returning messages with advancing seq, but complete stays false
    let pageCount = 0
    const fetchMock = vi.fn().mockImplementation(async () => {
      pageCount++
      return {
        ok: true,
        json: async () => ({
          ok: true,
          complete: false,
          remaining: 100,
          messages: [{ id: `msg-${pageCount}`, seq: pageCount }],
        }),
      }
    })
    vi.stubGlobal('fetch', fetchMock)

    const result = await fetchUnreadIds(SEAT)

    expect(result.complete).toBe(false)
    expect(result.ids.size).toBe(20)
    expect(fetchMock).toHaveBeenCalledTimes(20)
  })
})

describe('drainDeferred', () => {
  const SEAT = 'test-seat-drain'

  beforeEach(() => {
    seatMupotConfig.set(SEAT, { token: 'tok', api: 'https://pot.test' })
  })
  afterEach(() => {
    seatMupotConfig.delete(SEAT)
    deferred.delete(SEAT)
    vi.unstubAllGlobals()
  })

  it('clears the queue up front (re-entrancy guard) before the async reconcile settles', async () => {
    deferred.set(SEAT, [{ id: 'x', seq: 1 }])
    // fetch never resolves during this assertion window — proves the queue is emptied
    // synchronously at the top of drainDeferred, not after reconciliation completes.
    vi.stubGlobal('fetch', vi.fn(() => new Promise(() => {})))

    const p = drainDeferred(SEAT)
    await Promise.resolve() // let drainDeferred run to its first await
    expect(deferred.get(SEAT)).toEqual([])

    void p // intentionally left pending; nothing further to await in this test
  })

  it('does not deliver an entry the pot no longer shows as unread', async () => {
    deferred.set(SEAT, [{ id: 'already-consumed', seq: 1 }])
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ ok: true, messages: [] }), // nothing unread — it was consumed elsewhere
    }))
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {})

    await drainDeferred(SEAT)

    const staleLog = logSpy.mock.calls.some((args) =>
      args.some((a) => typeof a === 'string' && a.includes('stale: dropping already-consumed deferred mupot already-consumed')),
    )
    expect(staleLog).toBe(true)

    logSpy.mockRestore()
  })

  it('is a no-op when the queue is empty', async () => {
    deferred.delete(SEAT)
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await drainDeferred(SEAT)

    expect(fetchMock).not.toHaveBeenCalled()
  })
})
