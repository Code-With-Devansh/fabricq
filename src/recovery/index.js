import redis from '../config/redis.js'
import { HEARTBEAT_SET_KEY } from '../worker/worker.js'

const HEARTBEAT_TIMEOUT_MS = 30_000;

export async function getStaleExecutions(){
  const cutoff = Date.now() - HEARTBEAT_TIMEOUT_MS;

  return redis.zrangebyscore(
    "execution:heartbeats",
    0,
    cutoff
  );
}

const staleExecutions = await getStaleExecutions();

for (const executionId of staleExecutions) {
  console.log("Recover:", executionId);

  // 1. Acquire recovery lock
  // 2. Check execution status in Postgres
  // 3. Requeue / mark failed
  // 4. Remove heartbeat
}