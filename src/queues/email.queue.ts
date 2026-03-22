/**
 * Email Queue using BullMQ
 *
 * Why BullMQ over direct email?
 *  - Retries on SMTP failure (automatic backoff)
 *  - Rate limiting (don't overwhelm mail server)
 *  - Job tracking (see which emails failed/succeeded)
 *  - Delay support (send 1h after order, not immediately)
 */
import { Queue } from "bullmq";
import { config } from "../config/env.js";

export interface EmailJobData {
  to: string;
  subject: string;
  template: string;
  data: Record<string, unknown>;
}

const connection = {
  host: config.redis.host,
  port: config.redis.port,
  password: config.redis.password,
};

export const emailQueue = new Queue<EmailJobData>("email", {
  connection,
  defaultJobOptions: {
    attempts: 3,                    // Retry up to 3 times
    backoff: {
      type: "exponential",          // Wait 2s, 4s, 8s between retries
      delay: 2000,
    },
    removeOnComplete: { count: 100 }, // Keep last 100 completed jobs
    removeOnFail: { count: 50 },      // Keep last 50 failed jobs for debugging
  },
});

export async function addEmailJob(data: EmailJobData, delayMs = 0): Promise<void> {
  await emailQueue.add("send-email", data, { delay: delayMs });
  console.log(`[Email Queue] Job added → ${data.to} (${data.template})`);
}
