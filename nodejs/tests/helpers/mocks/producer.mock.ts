/**
 * NOTE: This file should only be imported for code that wants to fully mock the kafka producer
 *
 * If you just want to observe a real producer class then use `producer.spy.ts`
 */
import { KafkaProducerObserver } from './producer.spy'

import { HighLevelProducer } from 'node-rdkafka'

import { KafkaProducerWrapper } from '~/common/kafka/producer'

const ActualKafkaProducerWrapper = jest.requireActual('~/common/kafka/producer').KafkaProducerWrapper

jest.mock('~/common/kafka/producer', () => {
    const mockHighLevelProducer: jest.Mocked<HighLevelProducer> = {
        produce: jest.fn((...args) => args[args.length - 1]?.()),
        flush: jest.fn((...args) => args[args.length - 1]?.()),
        disconnect: jest.fn((...args) => args[args.length - 1]?.()),
        connect: jest.fn((...args) => args[args.length - 1]?.()),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        getMetadata: jest.fn((opts: any, cb: any) =>
            cb(null, {
                topics: opts?.topic
                    ? [{ name: opts.topic, partitions: [{ id: 0, leader: 0, replicas: [0], isrs: [0] }] }]
                    : [],
                brokers: [{ id: 0, host: 'mock', port: 9092 }],
                orig_broker_id: 0,
                orig_broker_name: 'mock',
            })
        ),
    } as any

    // Rather than calling create we just create a new instance with the underlying node-rdkafka producer mocked.
    const kafkaProducer = new ActualKafkaProducerWrapper(mockHighLevelProducer)

    class MockKafkaProducer {
        static create = jest.fn(() => Promise.resolve(kafkaProducer))
    }

    return {
        KafkaProducerWrapper: MockKafkaProducer,
        _producer: kafkaProducer,
        _broker: mockHighLevelProducer,
    }
})

export const mockProducer = require('~/common/kafka/producer')._producer as KafkaProducerWrapper
export const MockKafkaProducerWrapper = require('~/common/kafka/producer')
    .KafkaProducerWrapper as typeof KafkaProducerWrapper
export const mockProducerObserver = new KafkaProducerObserver(mockProducer)

const mockBroker = require('~/common/kafka/producer')._broker as { produce: jest.Mock }

export const kafkaRecordsOn = (topic: string): (() => Buffer[]) => {
    const from = mockBroker.produce.mock.calls.length
    return () =>
        mockBroker.produce.mock.calls
            .slice(from)
            .filter(([calledTopic]) => calledTopic === topic)
            .map(([, , value]) => value as Buffer)
}

// `releaseOne()` acks the oldest held record. `release()` acks every held record and makes the broker ack at once again.
export const holdKafkaAcks = (topic: string): { releaseOne: () => void; release: () => void } => {
    const deliver = mockBroker.produce.getMockImplementation()!
    const held: (() => void)[] = []
    mockBroker.produce.mockImplementation((...args: unknown[]) =>
        args[0] === topic ? void held.push(() => deliver(...args)) : deliver(...args)
    )
    return {
        releaseOne: () => held.shift()?.(),
        release: () => {
            mockBroker.produce.mockImplementation(deliver)
            held.splice(0).forEach((ack) => ack())
        },
    }
}
