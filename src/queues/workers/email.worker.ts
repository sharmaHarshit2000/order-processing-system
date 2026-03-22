/**
 * Email Worker - processes jobs from the email queue
 *
 * In production this would use nodemailer/SendGrid/SES.
 * Here we simulate with a delay to mimic real SMTP latency.
 */
import { Worker, Job } from "bullmq";
import { config } from "../../config/env.js";
import type { EmailJobData } from "../email.queue.js";

const connection = {
  host: config.redis.host,
  port: config.redis.port,
  password: config.redis.password,
};

async function processEmailJob(job: Job<EmailJobData>): Promise<{ sent: boolean }> {
  const { to, subject, template, data } = job.data;

  console.log(`[Email Worker] Processing job #${job.id}: ${template} → ${to}`);

  // Simulate SMTP delay (500ms - 1.5s)
  await new Promise((r) => setTimeout(r, 500 + Math.random() * 1000));

  // Simulate occasional failure (10% chance) to demo retry behavior
  if (Math.random() < 0.1) {
    throw new Error(`SMTP connection timeout for ${to}`);
  }

  console.log(`[Email Worker] ✓ Email sent to ${to} | Subject: "${subject}" | Data: ${JSON.stringify(data)}`);

  return { sent: true };
}

export function startEmailWorker(): Worker<EmailJobData> {
  const worker = new Worker<EmailJobData>("email", processEmailJob, {
    connection,
    concurrency: 5, // Process up to 5 emails simultaneously
  });

  worker.on("completed", (job) => {
    console.log(`[Email Worker] Job #${job.id} completed`);
  });

  worker.on("failed", (job, err) => {
    console.error(`[Email Worker] Job #${job?.id} failed (attempt ${job?.attemptsMade}): ${err.message}`);
  });

  worker.on("error", (err) => {
    console.log(`[Email Worker] Worker error: ${err.message}`);
  });

  console.log("[Email Worker] Started — concurrency: 5");
  return worker;
}
