// server.js
require("dotenv").config();
const express = require("express");
const cron = require("node-cron");
const { runMovementsScraper } = require("./scrapers/movements");
const { runItemsScraper } = require("./scrapers/items");

const app = express();
const PORT = Number(process.env.PORT || 8080);

// Timeout “cinturón de seguridad” (por default 25 min)
const SCRAPER_TIMEOUT_MS = Number(process.env.SCRAPER_TIMEOUT_MS || 25 * 60 * 1000);
// Para /run-all conviene más (por default 60 min)
const RUN_ALL_TIMEOUT_MS = Number(process.env.RUN_ALL_TIMEOUT_MS || 60 * 60 * 1000);

// Cron timezone (opcional; si no está, usa la del sistema)
const CRON_TZ = process.env.CRON_TZ; // ej: "America/Argentina/Buenos_Aires"

let currentJob = null;      // "movements" | "items" | "all"
let jobStartedAt = 0;

function msToHuman(ms) {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  const ss = s % 60;
  const mm = m % 60;
  if (h > 0) return `${h}h ${mm}m ${ss}s`;
  if (m > 0) return `${m}m ${ss}s`;
  return `${s}s`;
}

async function runExclusive(jobName, fn, { timeoutMs = SCRAPER_TIMEOUT_MS } = {}) {
  if (currentJob) {
    return {
      ok: false,
      skipped: true,
      running: currentJob,
      runningForMs: Date.now() - jobStartedAt,
    };
  }

  currentJob = jobName;
  jobStartedAt = Date.now();

  console.log(`[LOCK] START ${jobName}`);

  const timeoutPromise = new Promise((_, reject) => {
    const t = setTimeout(() => {
      clearTimeout(t);
      reject(new Error(`[TIMEOUT] ${jobName} superó ${timeoutMs}ms`));
    }, timeoutMs);
  });

  try {
    await Promise.race([fn(), timeoutPromise]);
    console.log(`[LOCK] OK ${jobName} (${msToHuman(Date.now() - jobStartedAt)})`);
    return { ok: true, skipped: false };
  } catch (e) {
    console.error(`[LOCK] ERROR ${jobName} (${msToHuman(Date.now() - jobStartedAt)}):`, e);
    return { ok: false, skipped: false, error: String(e?.message || e) };
  } finally {
    currentJob = null;
    jobStartedAt = 0;
  }
}

// Healthcheck
app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// Estado del lock
app.get("/status", (_req, res) => {
  res.status(200).json({
    ok: true,
    currentJob,
    runningFor: currentJob ? msToHuman(Date.now() - jobStartedAt) : null,
  });
});

// Ejecutar scrapers manualmente (respetando lock)
app.get("/run-movements", async (_req, res) => {
  const r = await runExclusive("movements", runMovementsScraper);
  if (r.skipped) return res.status(409).send(`Ocupado: corriendo ${r.running} hace ${msToHuman(r.runningForMs)}`);
  return r.ok ? res.status(200).send("Movements OK") : res.status(500).send("Error en movements");
});

app.get("/run-items", async (_req, res) => {
  const r = await runExclusive("items", runItemsScraper);
  if (r.skipped) return res.status(409).send(`Ocupado: corriendo ${r.running} hace ${msToHuman(r.runningForMs)}`);
  return r.ok ? res.status(200).send("Items OK") : res.status(500).send("Error en items");
});

app.get("/run-all", async (_req, res) => {
  const r = await runExclusive(
    "all",
    async () => {
      await runMovementsScraper();
      await runItemsScraper();
    },
    { timeoutMs: RUN_ALL_TIMEOUT_MS }
  );
  if (r.skipped) return res.status(409).send(`Ocupado: corriendo ${r.running} hace ${msToHuman(r.runningForMs)}`);
  return r.ok ? res.status(200).send("Movements + Items OK") : res.status(500).send("Error ejecutando ambos");
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor vivo en :${PORT} (GET /healthz, /status, /run-movements, /run-items, /run-all)`);
});

// Logs útiles
process.on("unhandledRejection", (reason) => console.error("[FATAL] UnhandledRejection:", reason));
process.on("uncaughtException", (err) => console.error("[FATAL] UncaughtException:", err));

// ==== CRON ====
// Movimientos cada 30 min (si hay otro corriendo, se saltea)
cron.schedule(
  "*/30 * * * *",
  async () => {
    console.log("[CRON] Movements (cada 30 min)...");
    const r = await runExclusive("movements", runMovementsScraper);
    if (r.skipped) console.log(`[CRON] Movements SKIP: ya corre ${r.running} hace ${msToHuman(r.runningForMs)}`);
  },
  CRON_TZ ? { timezone: CRON_TZ } : undefined
);

// Items 1 vez por hora al minuto 5 (si hay otro corriendo, se saltea)
cron.schedule(
  "5 * * * *",
  async () => {
    console.log("[CRON] Items (cada 1h)...");
    const r = await runExclusive("items", runItemsScraper);
    if (r.skipped) console.log(`[CRON] Items SKIP: ya corre ${r.running} hace ${msToHuman(r.runningForMs)}`);
  },
  CRON_TZ ? { timezone: CRON_TZ } : undefined
);
