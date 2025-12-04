// server.js
require("dotenv").config();
const express = require("express");
const cron = require("node-cron");
const { runMovementsScraper } = require("./scrapers/movements");
const { runItemsScraper } = require("./scrapers/items");

const app = express();
const PORT = process.env.PORT || 8080;

// Healthcheck simple
app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// Ejecutar cada scraper manualmente
app.get("/run-movements", async (_req, res) => {
  try {
    await runMovementsScraper();
    res.status(200).send("Movements OK");
  } catch (e) {
    console.error(e);
    res.status(500).send("Error en movements");
  }
});

app.get("/run-items", async (_req, res) => {
  try {
    await runItemsScraper();
    res.status(200).send("Items OK");
  } catch (e) {
    console.error(e);
    res.status(500).send("Error en items");
  }
});

// Ejecutar ambos en cadena
app.get("/run-all", async (_req, res) => {
  try {
    await runMovementsScraper();
    await runItemsScraper();
    res.status(200).send("Movements + Items OK");
  } catch (e) {
    console.error(e);
    res.status(500).send("Error ejecutando ambos");
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(
    `Servidor vivo en :${PORT} (GET /healthz, /run-movements, /run-items, /run-all)`
  );
});

// ==== CRON ====

/**
 * Ajustá los horarios como quieras.
 * Ejemplo:
 *  - Movements cada 30 minutos
 *  - Items una vez por hora al minuto 5
 */

// Movimientos cada 30 minutos
cron.schedule("*/30 * * * *", async () => {
  console.log("[CRON] Movements (cada 30 min)...");
  try {
    await runMovementsScraper();
    console.log("[CRON] Movements OK");
  } catch (e) {
    console.error("[CRON] Movements ERROR:", e);
  }
});

// Items 1 vez por hora al minuto 5 (00:05, 01:05, 02:05, ...)
cron.schedule("5 * * * *", async () => {
  console.log("[CRON] Items (cada 1h)...");
  try {
    await runItemsScraper();
    console.log("[CRON] Items OK");
  } catch (e) {
    console.error("[CRON] Items ERROR:", e);
  }
});
