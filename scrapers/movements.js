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

// ==== Login ====
async function login(page) {
  await page.goto("https://adm.agrcloud.com.ar", { waitUntil: "networkidle2" });
  await page.type("input#Username.form-control.form-icon-input", AGR_EMAIL);
  await page.type("input#Password.form-control.form-icon-input", AGR_PASSWORD);
  await Promise.all([
    page.click("button[type='submit']"),
    page.waitForNavigation({ waitUntil: "networkidle2" }),
  ]);
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

    console.log(`[MOV] Cargando página ${pageNum} → ${url}`);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 240000 });

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
        const cantidadNum = parseInt(cantidadText.replace(/\./g, ""), 10) || 0;

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

// ==== Proceso principal movimientos ====
async function runMovementsScraper() {
  console.log("[MOV] Conectando a Mongo…");
  await mongoose.connect(MONGO_URI);

  const now = new Date();
  const monthKey = currentMonthKey(now);

  let browser;

  try {
    const meta = await ScraperMeta.findById("scraper-meta").lean();
    const previousMonthInMeta = meta?.currentMonth || null;

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

      if (coffeeLive.length) {
        const coffeeHistoryDocs = coffeeLive.map((doc) => ({
          ...doc,
          periodMonth: previousMonthInMeta,
        }));

        await CoffeeMovementHistory.insertMany(coffeeHistoryDocs);
        console.log(
          `[MOV] Archivados ${coffeeLive.length} docs de CoffeeMovement → CoffeeMovementHistory (${previousMonthInMeta})`
        );
      }

      if (otherLive.length) {
        const otherHistoryDocs = otherLive.map((doc) => ({
          ...doc,
          periodMonth: previousMonthInMeta,
        }));

        await OtherItemHistory.insertMany(otherHistoryDocs);
        console.log(
          `[MOV] Archivados ${otherLive.length} docs de OtherItem → OtherItemHistory (${previousMonthInMeta})`
        );
      }
    }

    await CoffeeMovement.deleteMany({});
    await OtherItem.deleteMany({});
    console.log("[MOV] Colecciones live limpiadas.");

    await ScraperMeta.findByIdAndUpdate(
      "scraper-meta",
      { currentMonth: monthKey },
      { upsert: true }
    );

    browser = await puppeteer.launch({
      headless: "new",
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
      ],
      executablePath:
        process.env.PUPPETEER_EXECUTABLE_PATH ||
        process.env.CHROME_PATH ||
        undefined,
      protocolTimeout: 240000,
    });

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
      else if (entUp.includes("GRUPO") && entUp.includes("GEN")) label = "Grupo GEN";

      byEnt[label] += m.cantidad || 0;
    }

    const coffeeDocs = Array.from(buckets.entries()).map(
      ([tipoCafe, counts]) => ({
        tipoCafe,
        egresos: ENTIDADES.map((ent) => ({
          entidad: ent,
          cantidad: counts[ent] || 0,
        })),
        scrapedAt: new Date(),
      })
    );

    if (coffeeDocs.length) {
      await CoffeeMovement.insertMany(coffeeDocs);
    }
    console.log(`[MOV] Insertados en coffeemovements: ${coffeeDocs.length}`);

    // === Otros ítems (NO cafés) ===
    const otherItems = movimientos.filter((m) => !isCafe(m.recompensa));
    const nowDate = new Date();
    const currentPeriod = monthKey;

    if (otherItems.length) {
      const docs = otherItems.map((item) => ({
        ...item,
        scrapedAt: nowDate,
      }));
      await OtherItem.insertMany(docs);
    }
    console.log(`[MOV] Insertados en otheritems: ${otherItems.length}`);

    if (otherItems.length) {
      const bulkOps = otherItems.map((m) => {
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
                isCafeCombo: false,
                periodMonth: currentPeriod,
                scrapedAt: nowDate,
              },
            },
            upsert: true,
          },
        };
      });

      const result = await Movement.bulkWrite(bulkOps, { ordered: false });
      console.log(
        `[MOV] Movements upsertados: insertados=${result.upsertedCount}, modificados=${result.modifiedCount}`
      );
    } else {
      console.log("[MOV] No hay otros ítems para upsert en movements.");
    }
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch (e) {
        console.error("[MOV] Error al cerrar browser:", e.message);
      }
    }
    await mongoose.disconnect().catch(() => {});
    console.log("[MOV] Proceso completado.");
  }
}

module.exports = { runMovementsScraper };
