// tests/reconcile.test.mjs — the deferred-delivery re-validation gate (mumega-com#1179).

import { describe, expect, it, vi } from 'vitest'
import { filterStillUnread, reconcileBeforeDeliver } from '../bridge/lib/reconcile.mjs'

describe('filterStillUnread', () => {
  it('keeps only candidates whose id is present in the unread set', () => {
    const candidates = [{ id: 'a' }, { id: 'b' }, { id: 'c' }]
    const unread = new Set(['a', 'c'])
    expect(filterStillUnread(candidates, unread)).toEqual([{ id: 'a' }, { id: 'c' }])
  })

  it('drops everything when the unread set is empty (all already consumed)', () => {
    const candidates = [{ id: 'a' }, { id: 'b' }]
    expect(filterStillUnread(candidates, new Set())).toEqual([])
  })

  it('keeps everything when every candidate is still unread', () => {
    const candidates = [{ id: 'a' }, { id: 'b' }]
    expect(filterStillUnread(candidates, new Set(['a', 'b']))).toEqual(candidates)
  })

  it('is defensive against malformed input rather than throwing', () => {
    expect(filterStillUnread(null, new Set(['a']))).toEqual([])
    expect(filterStillUnread([{ id: 'a' }], null)).toEqual([])
    expect(filterStillUnread([{}, { id: 1 }, null], new Set(['a']))).toEqual([])
  })
})

describe('reconcileBeforeDeliver', () => {
  it('drops a deferred message that was acked elsewhere while it sat in the queue', async () => {
    // This is the exact defect: msg 'stale-msg' was fetched while the seat was busy, deferred,
    // and consumed via a direct inbox_ack call before the seat went idle and the queue drained.
    const candidates = [{ id: 'stale-msg', seq: 3736 }, { id: 'still-fresh', seq: 3737 }]
    const fetchUnreadIds = vi.fn().mockResolvedValue({ ids: new Set(['still-fresh']), complete: true })
    const onStale = vi.fn()

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds, onStale)

    expect(result).toEqual([{ id: 'still-fresh', seq: 3737 }])
    expect(onStale).toHaveBeenCalledTimes(1)
    expect(onStale).toHaveBeenCalledWith({ id: 'stale-msg', seq: 3736 })
  })

  it('delivers everything when nothing has been consumed since it was queued', async () => {
    const candidates = [{ id: 'a' }, { id: 'b' }]
    const fetchUnreadIds = vi.fn().mockResolvedValue({ ids: new Set(['a', 'b']), complete: true })

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds)

    expect(result).toEqual(candidates)
  })

  it('fails OPEN — a fetch error delivers the queue as-is rather than dropping it', async () => {
    const candidates = [{ id: 'a' }, { id: 'b' }]
    const fetchUnreadIds = vi.fn().mockRejectedValue(new Error('network down'))

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds)

    // Silent message loss is worse than one stale re-offer — see module docstring.
    expect(result).toEqual(candidates)
  })

  it('fails OPEN on a bare Set because a bare Set cannot prove completeness', async () => {
    // Kasra finding 2026-09-03: A bare Set has no completeness marker. If accepted as
    // authoritative, a partial Set from a legacy producer drops unread messages.
    // reconcileBeforeDeliver MUST fail open on a bare Set.
    const candidates = [{ id: 'a' }, { id: 'b' }]
    const fetchUnreadIds = vi.fn().mockResolvedValue(new Set(['a'])) // would drop 'b' if accepted!

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds)

    expect(result).toEqual(candidates)
  })

  it('fails OPEN on a malformed (non-Set) response too', async () => {
    const candidates = [{ id: 'a' }]
    const fetchUnreadIds = vi.fn().mockResolvedValue(['a']) // an array, not a Set — wrong shape

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds)

    expect(result).toEqual(candidates)
  })

  it('does not call the check at all for an empty queue', async () => {
    const fetchUnreadIds = vi.fn()
    const result = await reconcileBeforeDeliver([], fetchUnreadIds)
    expect(result).toEqual([])
    expect(fetchUnreadIds).not.toHaveBeenCalled()
  })

  it('does not call onStale when nothing was dropped', async () => {
    const candidates = [{ id: 'a' }]
    const fetchUnreadIds = vi.fn().mockResolvedValue({ ids: new Set(['a']), complete: true })
    const onStale = vi.fn()

    await reconcileBeforeDeliver(candidates, fetchUnreadIds, onStale)

    expect(onStale).not.toHaveBeenCalled()
  })

  it('drops stale messages when { ids, complete: true } is returned by fetchUnreadIds', async () => {
    const candidates = [{ id: 'm1', seq: 1 }, { id: 'm2', seq: 2 }]
    const fetchUnreadIds = vi.fn().mockResolvedValue({ ids: new Set(['m2']), complete: true })
    const onStale = vi.fn()

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds, onStale)

    expect(result).toEqual([{ id: 'm2', seq: 2 }])
    expect(onStale).toHaveBeenCalledWith({ id: 'm1', seq: 1 })
  })

  it('fails OPEN when { ids, complete: false } is returned — never drops on partial reads', async () => {
    // Exact defect caught by Kasra: producer returned 100 of 150 rows with complete=false.
    // Deferred holds m120 (not in the first 100) and m5.
    // Because complete is false, reconcile MUST NOT drop m120!
    const candidates = [{ id: 'm120', seq: 120 }, { id: 'm5', seq: 5 }]
    const fetchUnreadIds = vi.fn().mockResolvedValue({ ids: new Set(['m5']), complete: false })
    const onStale = vi.fn()

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds, onStale)

    expect(result).toEqual(candidates)
    expect(onStale).not.toHaveBeenCalled()
  })

  it('correctly drops an 11-message burst of already-settled messages against complete: true', async () => {
    // Live incident 2026-09-03 (mumega-com#1179): 11 stale offers arrived in one burst
    // for seqs 3862, 3864, 3865, 3867, 3869, 3871, 3872, 3873, 3875, 3877 and 3878.
    // All 11 were already settled (read_at IS NOT NULL on server).
    // The producer reports complete: true and 0 unread messages for this seat.
    // Reconciler MUST drop all 11 and deliver 0.
    const candidates = [
      3862, 3864, 3865, 3867, 3869, 3871, 3872, 3873, 3875, 3877, 3878,
    ].map((seq) => ({ id: `msg-${seq}`, seq }))

    const fetchUnreadIds = vi.fn().mockResolvedValue({ ids: new Set(), complete: true })
    const onStale = vi.fn()

    const result = await reconcileBeforeDeliver(candidates, fetchUnreadIds, onStale)

    expect(result).toEqual([])
    expect(onStale).toHaveBeenCalledTimes(11)
  })
})
