/**
 * Kafka Producer - publishes events to topics
 *
 * Pattern: Fire-and-forget with at-least-once delivery
 *  - acks: "all" ensures message is written to all replicas
 *  - Retry on transient failures (network, broker unavailability)
 */
import { Kafka, Producer, CompressionTypes } from "kafkajs";
import { config } from "../config/env.js";
import type { KafkaEvent } from "./topics.js";

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  retry: {
    initialRetryTime: 300,
    retries: 5,
  },
});

let producer: Producer;

export async function ensureTopics(topics: string[]): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: topics.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 1 })),
  });
  await admin.disconnect();
}

export async function connectProducer(): Promise<void> {
  producer = kafka.producer({
    allowAutoTopicCreation: true,
  });
  await producer.connect();
  console.log("[Kafka Producer] Connected");
}

export async function disconnectProducer(): Promise<void> {
  await producer?.disconnect();
  console.log("[Kafka Producer] Disconnected");
}

export async function publishEvent<T>(
  topic: string,
  event: KafkaEvent<T>,
  key?: string
): Promise<void> {
  await producer.send({
    topic,
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key: key || null,
        value: JSON.stringify(event),
        headers: {
          eventType: event.eventType,
          timestamp: event.timestamp,
        },
      },
    ],
  });

  console.log(`[Kafka Producer] Published "${event.eventType}" to topic: ${topic}`);
}
