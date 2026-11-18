import { Semaphore } from '../../utils/sempahore'
import {
    BatchCapacity,
    BatchSlot,
    SlotReply,
    TemplateBatchRequestSchema,
    runBatchSlots,
} from './transactional-email-batch'

describe('runBatchSlots', () => {
    const queued = (id: string): SlotReply => ({ outcome: 'queued', status: 202, body: { invocation_id: id } })

    const gate = () => {
        let open: () => void = () => {}
        const opened = new Promise<void>((resolve) => (open = resolve))
        return { open, opened }
    }

    const isSettled = async (promise: Promise<unknown>): Promise<boolean> => {
        let settled = false
        void promise.then(() => (settled = true))
        await new Promise((resolve) => setImmediate(resolve))
        return settled
    }

    it('answers at the deadline, and holds the permit of a recipient still running until it settles', async () => {
        const slots = new Semaphore(1)
        const deadline = new AbortController()
        const stuck = gate()
        const ran: string[] = []

        const answered = runBatchSlots(
            ['a', 'b', 'c'],
            async (item) => {
                ran.push(item)
                if (item === 'b') {
                    deadline.abort()
                    await stuck.opened
                }
                return queued(item)
            },
            { slots, fanOut: 1, deadline: deadline.signal }
        )
        const { replies, settled } = await answered

        expect(replies).toEqual([
            queued('a'),
            {
                outcome: 'deadline_running',
                status: 503,
                body: { error: 'delivery_unavailable', may_still_go_out: true },
            },
            { outcome: 'not_attempted', status: 503, body: { error: 'not_attempted' } },
        ])
        expect(await isSettled(settled)).toBe(false)
        expect(await isSettled(slots.run(() => Promise.resolve()))).toBe(false)

        stuck.open()

        expect(await isSettled(settled)).toBe(true)
        expect(ran).toEqual(['a', 'b'])
    })

    it('holds the permit of a finished recipient until its publish settles, across batches', async () => {
        const slots = new Semaphore(1)
        const publish = gate()
        const ran: string[] = []
        const run = (items: string[], sendOne: (item: string, slot: BatchSlot) => Promise<SlotReply>) =>
            runBatchSlots(items, sendOne, { slots, fanOut: 25, deadline: new AbortController().signal })

        const first = await run(['a'], (item, slot) => {
            ran.push(item)
            slot.settlement.track(publish.opened)
            return Promise.resolve(queued(item))
        })
        const second = run(['b'], (item) => {
            ran.push(item)
            return Promise.resolve(queued(item))
        })

        expect(first.replies).toEqual([queued('a')])
        expect(await isSettled(second)).toBe(false)
        expect(ran).toEqual(['a'])

        publish.open()

        expect((await second).replies).toEqual([queued('b')])
    })

    it.each([
        ['before it could queue the email', false, false],
        ['after it could have queued the email', true, true],
    ])('answers only its own recipient when a send throws %s', async (_case, committing, mayStillGoOut) => {
        const { replies } = await runBatchSlots(
            ['a', 'b'],
            (item, slot) => {
                if (item === 'b') {
                    return Promise.resolve(queued(item))
                }
                if (committing) {
                    slot.mayCommit()
                }
                return Promise.reject(new Error('connection reset'))
            },
            { slots: new Semaphore(100), fanOut: 25, deadline: new AbortController().signal }
        )

        expect(replies).toEqual([
            { outcome: 'error', status: 503, body: { error: 'delivery_unavailable', may_still_go_out: mayStillGoOut } },
            queued('b'),
        ])
    })

    it('runs at most fanOut recipients of one batch at once', async () => {
        let running = 0
        let mostRunning = 0
        const { replies } = await runBatchSlots(
            Array.from({ length: 60 }, (_, i) => String(i)),
            async (item) => {
                mostRunning = Math.max(mostRunning, ++running)
                await new Promise((resolve) => setImmediate(resolve))
                running--
                return queued(item)
            },
            { slots: new Semaphore(100), fanOut: 25, deadline: new AbortController().signal }
        )

        expect(replies).toHaveLength(60)
        expect(replies.every((reply) => reply.outcome === 'queued')).toBe(true)
        expect(mostRunning).toBe(25)
    })
})

describe('BatchCapacity', () => {
    it('refuses a request over the cap until an admitted one releases', () => {
        const capacity = new BatchCapacity(1, 100)
        const release = capacity.admit()

        expect(release).not.toBeNull()
        expect(capacity.admit()).toBeNull()

        release!()
        release!()

        expect(capacity.admit()).not.toBeNull()
        expect(capacity.admit()).toBeNull()
    })
})

describe('TemplateBatchRequestSchema', () => {
    it('refuses a recipient list over the cap with one issue, without reading any recipient', () => {
        const recipients: unknown[] = Array.from({ length: 1000 }, () => ({}))
        let reads = 0
        recipients.forEach((recipient, index) =>
            Object.defineProperty(recipients, index, {
                get: () => {
                    reads++
                    return recipient
                },
            })
        )

        const parsed = TemplateBatchRequestSchema.safeParse({ recipients })

        expect(parsed.error?.issues).toEqual([expect.objectContaining({ code: 'too_big', path: ['recipients'] })])
        expect(reads).toBe(0)
    })
})
