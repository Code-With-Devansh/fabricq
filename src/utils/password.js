import argon2 from "argon2";
import config from "../config/index.js";

const { memoryCost, timeCost, parallelism } = config.auth.argon2;

export async function hashPassword(plain) {
  return argon2.hash(plain, {
    type: argon2.argon2id,
    memoryCost,
    timeCost,
    parallelism,
  });
}

export async function verifyPassword(hash, plain) {
  try {
    return await argon2.verify(hash, plain);
  } catch {
    // Malformed/foreign hash (e.g. corrupted row) - treat as no match,
    // never throw out of a login path.
    return false;
  }
}
