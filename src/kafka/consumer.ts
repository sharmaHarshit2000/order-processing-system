/**
 * Kafka Consumer Factory
 *
 * Each consumer group processes messages independently.
 * Multiple consumer groups = same message delivered to each group.
 *
 * Pattern: Consumer Group per service
 *  - inventory-service-group → handles inventory events
 *  - analytics-service-group → handles analytics events
 *  - notification-service-group → handles notification events
 */
import { Kafka, Consumer, EachMessagePayload } from "kafkajs";
import { config } from "../config/env.js";

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
});

export type MessageHandler = (payload: EachMessagePayload) => Promise<void>;

export async function createConsumer(
  groupId: string,
  topics: string[],
  handler: MessageHandler
): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId });

  await consumer.connect();
  console.log(`[Kafka Consumer] Connected - Group: ${groupId}`);

  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
    console.log(`[Kafka Consumer] Subscribed to: ${topic}`);
  }

  await consumer.run({
    eachMessage: async (payload) => {
      const { topic, partition, message } = payload;
      console.log(
        `[Kafka Consumer][${groupId}] Received message from ${topic}[${partition}]`
      );
      await handler(payload);
    },
  });

  return consumer;
}
