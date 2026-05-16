import Redis from 'ioredis';
import { buildEmbed, sendToDiscord } from './discord';

const STREAM   = 'task:events';
const GROUP    = 'notification-service';
const CONSUMER = 'notifier-1';

function parseFields(raw: string[]): Record<string, string> {
  const obj: Record<string, string> = {};
  for (let i = 0; i < raw.length; i += 2) obj[raw[i]] = raw[i + 1];
  return obj;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export async function startConsumer(): Promise<void> {
  const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');

  redis.on('error', (err) => console.error('[consumer] Redis error:', err.message));

  // Create consumer group — idempotent (MKSTREAM creates the stream if absent)
  await redis
    .xgroup('CREATE', STREAM, GROUP, '$', 'MKSTREAM')
    .catch(() => { /* group already exists */ });

  console.log(`[consumer] Listening on stream "${STREAM}" as group "${GROUP}"`);

  while (true) {
    try {
      // BLOCK 5 seconds waiting for new messages; returns null on timeout
      const results = (await redis.xreadgroup(
        'GROUP', GROUP, CONSUMER,
        'COUNT', '10',
        'BLOCK', '5000',
        'STREAMS', STREAM, '>',
      )) as [string, [string, string[]][]][] | null;

      if (!results) continue;

      for (const [, messages] of results) {
        for (const [id, rawFields] of messages) {
          const fields = parseFields(rawFields);
          console.log('[consumer] Event:', fields.event, '| task:', fields.task_title);

          const embed = buildEmbed(fields);
          if (embed) await sendToDiscord(embed);

          // Acknowledge so the message is not re-delivered
          await redis.xack(STREAM, GROUP, id);
        }
      }
    } catch (err) {
      console.error('[consumer] Unexpected error:', err);
      await sleep(2000);
    }
  }
}
