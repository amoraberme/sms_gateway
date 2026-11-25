import { PGlite } from "@electric-sql/pglite";
import { PGLiteSocketServer } from "@electric-sql/pglite-socket";

function getArg(flag, fallback) {
  const idx = process.argv.indexOf(flag);
  if (idx !== -1 && idx + 1 < process.argv.length) {
    return process.argv[idx + 1];
  }
  return fallback;
}

const port = Number(getArg("--port", "5432"));
const host = getArg("--host", "127.0.0.1");
const dataDir = getArg("--data-dir", "memory://");

const db = await PGlite.create(dataDir === "memory://" ? undefined : dataDir);
await db.waitReady;

const server = new PGLiteSocketServer({
  db,
  port,
  host,
});

await server.start();
console.log(`PGliteSocketServer listening on ${host}:${port} (dataDir=${dataDir})`);

const shutdown = async () => {
  await server.stop();
  await db.close();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
