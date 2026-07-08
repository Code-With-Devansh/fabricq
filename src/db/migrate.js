import { readdir, readFile } from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { pool } from "../config/db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MIGRATIONS_DIR = path.join(__dirname, "migrations");

async function ensureMigrationsTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS migrations (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

async function getAppliedMigrations(client) {
  const res = await client.query("SELECT name FROM migrations ORDER BY id;");
  return new Set(res.rows.map((row) => row.name));
}

async function getMigrationFiles() {
  return (await readdir(MIGRATIONS_DIR))
    .filter((file) => file.endsWith(".sql"))
    .sort();
}
async function runMigration(client, file) {
  const filePath = path.join(MIGRATIONS_DIR, file);
  const sql = await readFile(filePath, "utf8");

  console.log(`Applying ${file}...`);

  try {
    await client.query("BEGIN");
    await client.query(sql);
    await client.query("INSERT INTO migrations (name) VALUES ($1)", [file]);
    await client.query("COMMIT");
    console.log(`✔ ${file} applied`);
  } catch (err) {
    await client.query("ROLLBACK");
    err.message = `Failed to apply ${file}: ${err.message}`;
    throw err;
  }
}

async function migrate() {
  const client = await pool.connect(); // borrow a connection from the pool

  try {
    await ensureMigrationsTable(client);

    const applied = await getAppliedMigrations(client);
    const files = await getMigrationFiles();
    const pending = files.filter((file) => !applied.has(file));

    if (pending.length === 0) {
      console.log("No pending migrations. Database is up to date.");
      return;
    }

    for (const file of pending) {
      await runMigration(client, file);
    }

    console.log(`Done. Applied ${pending.length} migration(s).`);
  } finally {
    client.release(); // return connection to pool, don't kill the pool
    await pool.end(); // close pool so the script actually exits
  }
}

migrate().catch(err => {
  console.error("Migration failed:");
  console.error(err);
  process.exit(1);
});