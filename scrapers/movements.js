// scrapers/movements.js
require("dotenv").config();
const puppeteer = require("puppeteer");
const mongoose = require("mongoose");

// ==== ENV ====
const { AGR_EMAIL, AGR_PASSWORD, MONGO_URI } = process.env;
if (!AGR_EMAIL || !AGR_PASSWORD || !MONGO_URI) {
  console.error("Faltan AGR_EMAIL, AGR_PASSWORD o MONGO_URI");
  process.exit(1);
}

// Path de Chromium
const CHROME_EXECUTABLE_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  process.env.CHROME_PATH ||
  "/usr/bin/chromium";

// ==== Mongo: colecciones live ====
// Cafés agregados (por tipo y entidad)
const coffeeMovementSchema = new mongoose.Schema(
  {
    tipoCafe: String,
    egresos: [{ entidad: String, cantidad: Number }],
    scrapedAt: { type: Date, default: Date.now },
  },
  { versionKey: false }
);

// Otros ítems (fila a fila del portal)
const otherItemSchema = new mongoose.Schema(
  {
    fecha: String,
    entidad: String,
    movimiento: String,
    recompensa: String,
    depositoOrigen: String,
    depositoDestino: String,
    cantidad: Number,
    documento: String,
    scrapedAt: { type: Date, default: Date.now },
  },
  { versionKey: false }
);

const CoffeeMovement = mongoose.model("CoffeeMovement", coffeeMovementSchema);
const OtherItem = mongoose.model("OtherItem", otherItemSchema);

// ==== Mongo: colecciones de BACKUP (histórico mensual) ====
const coffeeMovementHistorySchema = new mongoose.Schema(
  {
    tipoCafe: String,
    egresos: [{ entidad: String, cantidad: Number }],
    scrapedAt: Date,
    periodMonth: String, // ej: "2025-11"
  },
  { versionKey: false }
);

const otherItemHistorySchema = new mongoose.Schema(
  {
    fecha: String,
    entidad: String,
    movimiento: String,
    recompensa: String,
    depositoOrigen: String,
    depositoDestino: String,
    cantidad: Number,
    documento: String,
    scrapedAt: Date,
    periodMonth: String, // ej: "2025-11"
  },
  { versionKey: false }
);

const CoffeeMovementHistory = mongoose.model(
  "CoffeeMovementHistory",
  coffeeMovementHistorySchema
);
const OtherItemHistory = mongoose.model(
  "OtherItemHistory",
  otherItemHistorySchema
);

// ==== Mongo: colección global movements (histórico completo) ====
const movementSchema = new mongoose.Schema(
  {
    _id: String, // clave única armada con los datos de la fila
    fechaRaw: String,
    date: Date,
    entidad: String,
    movimiento: String,
    recompensa: String,
    depositoOrigen: String,
    depositoDestino: String,
    cantidad: Number,
    documento: String,
    isCafeCombo: { type: Boolean, default: false },
    periodMonth: String, // "YYYY-MM"
    scrapedAt: Date,
  },
  { versionKey: false }
);

movementSchema.index({ date: -1 });
movementSchema.index({ periodMonth: -1, date: -1 });

const Movement = mongoose.model("Movement", movementSchema, "movements");

// ==== Mongo: metadatos para saber qué mes está en las colecciones live ====
const scraperMetaSchema = new mongoose.Schema(
  {
    _id: { type: String, default: "scraper-meta" },
    currentMonth: String, // ej: "2025-11"
  },
  { versionKey: false }
);

const ScraperMeta = mongoose.model("ScraperMeta", scraperMetaSchema);

// ==== Normalización / negocio ====
const CAFES = [
  "(1064) GASEOSA + ALFAJOR",
  "(1063) CANJE CAFE + ALFAJOR",
  "(1062) CAFE CHICO PARA LLEVAR + 2 FACTURAS",
  "(1062) CAFE + FACTURA O ALFAJOR",
];

const ENTIDADES = ["Monteverde", "Tobago SA 1", "Grupo GEN", "Bettica SA"];

const norm = (s = "") =>
  s
    .toString()
    .normalize("NFKC")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();

const CAFES_UP = CAFES.map((c) => norm(c));

const isCafe = (recompensa = "") => {
  const R = norm(recompensa);
  return CAFES_UP.some((c) => R.includes(c));
};

// Acepta EGRESO (ES), EGRESS (EN), SALIDA/OUT/EXIT, etc.
const isEgreso = (mov = "") => {
  const T = norm(mov);
  return /(EGRES|EGRESS|SALID|OUT|EXIT)/.test(T);
};

// ==== Fechas ====
function firstOfThisMonth(baseDate = new Date()) {
  const d = new Date(baseDate);
  d.setDate(1);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function todayEndStamp(baseDate = new Date()) {
  const d = new Date(baseDate);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}T23:59:59`;
}

function currentMonthKey(baseDate = new Date()) {
  const d = new Date(baseDate);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

function parseAgrDate(fechaStr = "") {
  if (!fechaStr) return null;
  const [datePart, timePart = "00:00:00"] = fechaStr.split(" ");
  const [dd, mm, yyyy] = datePart.split("/").map((x) => parseInt(x, 10));
  if (!yyyy || !mm || !dd) return null;
  const [HH, MM, SS] = timePart.split(":").map((x) => parseInt(x, 10) || 0);
  return new Date(yyyy, mm - 1, dd, HH, MM, SS);
}

// ==== Helpers ====
async function gotoWithRetries(page, url, { retries = 2, ...opts } = {}) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      await page.goto(url, opts);
      return;
    } catch (e) {
      lastErr = e;
      console.log(`[MOV] goto retry ${i + 1}/${retries + 1} ->`, e.message);
      await page.waitForTimeout(1500);
    }
  }
  throw lastErr;
}

// ==== Login ====
async function login(page) {
  await gotoWithRetries(page, "https://adm.agrcloud.com.ar", {
    waitUntil: "domcontentloaded",
    timeout: 240000,
  });

  await page.waitForSelector("input#Username.form-control.form-icon-input", {
    timeout: 30000,
  });

  await page.type("input#Username.form-control.form-icon-input", AGR_EMAIL);
  await page.type("input#Password.form-control.form-icon-input", AGR_PASSWORD);

  await Promise.all([
    page.click("button[type='submit']"),
    page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 240000 }),
  ]);

  const stillLogin = await page.$(
    "input#Username.form-control.form-icon-input"
  );
  if (stillLogin) throw new Error("Login falló (sigue el formulario).");

  console.log("[MOV] Login OK");
}

// ==== Scraping movements ====
async function scrapeAllMovements(page, { startDate, endDate }) {
  const base =
    "https://adm.agrcloud.com.ar/filtered/items/movements/details/service/2";
  const pageSize = 20;
  const all = [];

  for (let pageNum = 1; ; pageNum++) {
    const url = `${base}?startDate=${encodeURIComponent(
      startDate
    )}&endDate=${encodeURIComponent(
      endDate
    )}&orderBy=date-desc&page=${pageNum}&pageSize=${pageSize}`;

    console.log(`[MOV] Cargando página ${pageNum}`);
    await gotoWithRetries(page, url, {
      waitUntil: "domcontentloaded",
      timeout: 240000,
    });

    try {
      await page.waitForSelector("tbody tr", { timeout: 15000 });
    } catch {
      break;
    }

    const rows = await page.$$eval("tbody tr", (trs) =>
      Array.from(trs).map((row) => {
        const clean = (s = "") =>
          s
            .toString()
            .normalize("NFKC")
            .replace(/\u00A0/g, " ")
            .replace(/\s+/g, " ")
            .trim();

        const pick = (n) => {
          const td = row.querySelector(`td:nth-child(${n})`);
          return clean(td ? td.innerText || "" : "");
        };
        const pickLink = (n) => {
          const td = row.querySelector(`td:nth-child(${n})`);
          if (!td) return "";
          const a = td.querySelector("a");
          return clean((a ? a.innerText : td.innerText) || "");
        };

        const cantidadText = pick(8);
        const cantidadNum = parseInt(
          cantidadText.replace(/\./g, "").replace(/,/g, ""),
          10
        ) || 0;

        return {
          fecha: pick(1),
          entidad: pick(2),
          movimiento: pick(3),
          documento: pickLink(4),
          recompensa: pickLink(5),
          depositoOrigen: pick(6),
          depositoDestino: pick(7),
          cantidad: cantidadNum,
        };
      })
    );

    if (!rows.length) break;
    all.push(...rows);
    if (rows.length < pageSize) break;
  }

  console.log(`[MOV] Total movimientos extraídos: ${all.length}`);
  return all;
}

// ==== LOCK para evitar ejecuciones concurrentes ====
let isMovementsRunning = false;

// ==== Proceso principal movimientos ====
async function runMovementsScraper() {
  if (isMovementsRunning) {
    console.log("[MOV] Ya hay una ejecución en curso, se cancela esta.");
    return;
  }
  isMovementsRunning = true;

  let browser;
  let mongoConnected = false;

  const now = new Date();
  const monthKey = currentMonthKey(now);

  try {
    console.log("[MOV] Conectando a Mongo…");
    await mongoose.connect(MONGO_URI, {
      serverSelectionTimeoutMS: 30000,
      socketTimeoutMS: 240000,
    });
    mongoConnected = true;

    console.log("[MOV] Usando Chromium en:", CHROME_EXECUTABLE_PATH);

    const meta = await ScraperMeta.findById("scraper-meta").lean();
    const previousMonthInMeta = meta?.currentMonth || null;

    // Si cambió el mes: archivar live del mes anterior (evitando duplicados)
    if (previousMonthInMeta && previousMonthInMeta !== monthKey) {
      console.log(
        `[MOV] Mes cambió de ${previousMonthInMeta} a ${monthKey}. Archivando datos anteriores…`
      );

      const [coffeeLive, otherLive] = await Promise.all([
        CoffeeMovement.find().lean(),
        OtherItem.find().lean(),
      ]);

      console.log(
        `[MOV] Live a archivar: CoffeeMovement=${coffeeLive.length}, OtherItem=${otherLive.length}`
      );

      // Evitar duplicados: limpiar el histórico del mes anterior antes de insertar
      await Promise.all([
        CoffeeMovementHistory.deleteMany({ periodMonth: previousMonthInMeta }),
        OtherItemHistory.deleteMany({ periodMonth: previousMonthInMeta }),
      ]);

      if (coffeeLive.length) {
        const coffeeHistoryDocs = coffeeLive.map((doc) => ({
          ...doc,
          periodMonth: previousMonthInMeta,
        }));
        await CoffeeMovementHistory.insertMany(coffeeHistoryDocs, {
          ordered: false,
        });
        console.log(
          `[MOV] Archivados ${coffeeLive.length} docs → CoffeeMovementHistory (${previousMonthInMeta})`
        );
      }

      if (otherLive.length) {
        const otherHistoryDocs = otherLive.map((doc) => ({
          ...doc,
          periodMonth: previousMonthInMeta,
        }));
        await OtherItemHistory.insertMany(otherHistoryDocs, { ordered: false });
        console.log(
          `[MOV] Archivados ${otherLive.length} docs → OtherItemHistory (${previousMonthInMeta})`
        );
      }
    }

    // Guardar meta del mes actual (upsert)
    await ScraperMeta.findByIdAndUpdate(
      "scraper-meta",
      { currentMonth: monthKey },
      { upsert: true }
    );

    // Lanzar browser
    const launchOptions = {
      headless: true,
      executablePath: CHROME_EXECUTABLE_PATH,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-extensions",
        "--disable-software-rasterizer",
        "--disable-background-networking",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--no-zygote",
        // Si Chromium vuelve a romper, probá sacarlo:
        "--single-process",
        "--renderer-process-limit=1",
      ],
      protocolTimeout: 240000,
    };

    console.log("[MOV] Lanzando Puppeteer...");
    browser = await puppeteer.launch(launchOptions);

    const page = await browser.newPage();
    page.setDefaultTimeout(240000);
    page.setDefaultNavigationTimeout(240000);

    await page.setRequestInterception(true);
    page.on("request", (req) =>
      req.resourceType() === "image" ? req.abort() : req.continue()
    );

    await login(page);

    const startDate = firstOfThisMonth(now);
    const endDate = todayEndStamp(now);

    const movimientos = await scrapeAllMovements(page, { startDate, endDate });

    // === CAFÉS ===
    const cafesByRecompensa = movimientos.filter((m) => isCafe(m.recompensa));
    console.log(
      `[MOV] Cafés por recompensa (sin filtrar EGRESO): ${cafesByRecompensa.length}`
    );

    const cafesEgreso = cafesByRecompensa.filter((m) => isEgreso(m.movimiento));
    console.log(`[MOV] Cafés (EGRESO) detectados: ${cafesEgreso.length}`);

    if (cafesByRecompensa.length) {
      const sample = cafesByRecompensa.slice(0, 10).map((x) => ({
        mov: x.movimiento,
        rec: x.recompensa,
        cant: x.cantidad,
        ent: x.entidad,
      }));
      console.log("[MOV DEBUG] Muestras cafés (primeros 10):", sample);
    }

    const buckets = new Map();
    for (const m of cafesEgreso) {
      const tipoCafeUp = CAFES_UP.find((c) => norm(m.recompensa).includes(c));
      if (!tipoCafeUp) continue;

      const tipoCafeOriginal =
        CAFES[CAFES_UP.findIndex((c) => c === tipoCafeUp)] || m.recompensa;

      if (!buckets.has(tipoCafeOriginal)) {
        const initialCounts = {};
        for (const ent of ENTIDADES) initialCounts[ent] = 0;
        buckets.set(tipoCafeOriginal, initialCounts);
      }

      const byEnt = buckets.get(tipoCafeOriginal);

      const entUp = norm(m.entidad);
      let label = "Grupo GEN";
      if (entUp.includes("MONTEVERDE")) label = "Monteverde";
      else if (entUp.includes("TOBAGO")) label = "Tobago SA 1";
      else if (entUp.includes("BETTICA")) label = "Bettica SA";
      else if (entUp.includes("GRUPO") && entUp.includes("GEN"))
        label = "Grupo GEN";

      byEnt[label] += m.cantidad || 0;
    }

    const nowDate = new Date();

    const coffeeDocs = Array.from(buckets.entries()).map(
      ([tipoCafe, counts]) => ({
        tipoCafe,
        egresos: ENTIDADES.map((ent) => ({
          entidad: ent,
          cantidad: counts[ent] || 0,
        })),
        scrapedAt: nowDate,
      })
    );

    // === Otros ítems (NO cafés) live ===
    const otherItems = movimientos.filter((m) => !isCafe(m.recompensa));

    // ✅ IMPORTANTE: recién acá reemplazamos colecciones live (evita quedarte vacío si falla antes)
    await CoffeeMovement.deleteMany({});
    await OtherItem.deleteMany({});

    if (coffeeDocs.length) await CoffeeMovement.insertMany(coffeeDocs);
    console.log(`[MOV] Insertados en coffeemovements: ${coffeeDocs.length}`);

    if (otherItems.length) {
      const docs = otherItems.map((item) => ({ ...item, scrapedAt: nowDate }));
      await OtherItem.insertMany(docs, { ordered: false });
    }
    console.log(`[MOV] Insertados en otheritems: ${otherItems.length}`);

    // ✅ Upsert global movements (TODOS: cafés + no cafés) con flag isCafeCombo
    if (movimientos.length) {
      const bulkOps = movimientos.map((m) => {
        const rowId = [
          m.fecha,
          m.entidad,
          m.movimiento,
          m.documento,
          m.recompensa,
          m.depositoOrigen,
          m.depositoDestino,
          m.cantidad,
        ].join("|");

        const parsedDate = parseAgrDate(m.fecha);

        return {
          updateOne: {
            filter: { _id: rowId },
            update: {
              $set: {
                fechaRaw: m.fecha,
                date: parsedDate,
                entidad: m.entidad,
                movimiento: m.movimiento,
                documento: m.documento,
                recompensa: m.recompensa,
                depositoOrigen: m.depositoOrigen,
                depositoDestino: m.depositoDestino,
                cantidad: m.cantidad,
                isCafeCombo: isCafe(m.recompensa),
                periodMonth: monthKey,
                scrapedAt: nowDate,
              },
            },
            upsert: true,
          },
        };
      });

      const result = await Movement.bulkWrite(bulkOps, { ordered: false });
      console.log(
        `[MOV] Movements upsertados: upserted=${result.upsertedCount}, modified=${result.modifiedCount}`
      );
    } else {
      console.log("[MOV] No hay movimientos para upsert en movements.");
    }
  } catch (err) {
    console.error("[MOV] ERROR general:", err);
    throw err;
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch (e) {
        console.error("[MOV] Error al cerrar browser:", e.message);
      }
    }
    if (mongoConnected) await mongoose.disconnect().catch(() => {});
    isMovementsRunning = false;
    console.log("[MOV] Proceso completado.");
  }
}

module.exports = { runMovementsScraper };
