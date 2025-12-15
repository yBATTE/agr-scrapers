// scrapers/items.js
require("dotenv").config();
const puppeteer = require("puppeteer");
const mongoose = require("mongoose");

const email = process.env.AGR_EMAIL;
const password = process.env.AGR_PASSWORD;
const mongoUri = process.env.MONGO_URI;

// Path de Chromium dentro del contenedor / VPS
const CHROME_EXECUTABLE_PATH =
  process.env.CHROME_PATH ||
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  "/usr/bin/chromium";

if (!email || !password || !mongoUri) {
  console.error("[ITEMS] Faltan AGR_EMAIL, AGR_PASSWORD o MONGO_URI");
  process.exit(1);
}

// ================== SCHEMA DE MONGO ==================
const productSchema = new mongoose.Schema(
  {
    description: { type: String, index: true },
    category: String,
    stock_bettica: { type: Number, default: 0 },
    stock_grupogen: { type: Number, default: 0 },
    stock_monteverde: { type: Number, default: 0 },
    stock_tobago1: { type: Number, default: 0 },
    stock_global: { type: Number, default: 0 },
    cost: String,
    price: String,
    points: String,
    status: String,
    scrapedAt: { type: Date, default: Date.now },
  },
  { versionKey: false }
);

const AgrItem = mongoose.model("AgrItem", productSchema);

// ================== HELPERS ==================
const norm = (s = "") =>
  s
    .toString()
    .normalize("NFKC")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();

const parseStock = (s = "") => {
  // "1.234" -> 1234, "1,234" -> 1234
  const cleaned = s.toString().replace(/\./g, "").replace(/,/g, "").trim();
  const n = parseInt(cleaned, 10);
  return Number.isFinite(n) ? n : 0;
};

async function gotoWithRetries(page, url, { retries = 2, ...opts } = {}) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      await page.goto(url, opts);
      return;
    } catch (e) {
      lastErr = e;
      console.log(`[ITEMS] goto retry ${i + 1}/${retries + 1} ->`, e.message);
      await page.waitForTimeout(1500);
    }
  }
  throw lastErr;
}

// ================== SCRAPING ==================
async function extractAllProducts(page) {
  const baseUrl =
    "https://adm.agrcloud.com.ar/filtered/stocks/2?orderBy=description-asc";
  const pageSize = 50;
  const buildUrl = (pageNumber) =>
    `${baseUrl}&page=${pageNumber}&pageSize=${pageSize}`;

  const allProducts = [];

  await gotoWithRetries(page, buildUrl(1), {
    waitUntil: "domcontentloaded",
    timeout: 240000,
  });

  // Intentar esperar tabla (sin romper si tarda)
  try {
    await page.waitForSelector("tbody tr", { timeout: 15000 });
  } catch {}

  const totalPages = await page.evaluate(() => {
    const buttons = document.querySelectorAll("ul.pagination button.page");
    const nums = [];
    buttons.forEach((btn) => {
      const n = parseInt(btn.textContent.trim(), 10);
      if (!isNaN(n)) nums.push(n);
    });
    return nums.length ? Math.max(...nums) : 1;
  });

  console.log(`[ITEMS] Productos: se detectaron ${totalPages} páginas.`);

  for (let pageNumber = 1; pageNumber <= totalPages; pageNumber++) {
    console.log(`[ITEMS] Extrayendo productos de la página ${pageNumber}...`);
    await gotoWithRetries(page, buildUrl(pageNumber), {
      waitUntil: "domcontentloaded",
      timeout: 240000,
    });

    try {
      await page.waitForSelector("tbody tr", { timeout: 15000 });
    } catch {
      console.log("[ITEMS]   → No se encontraron filas en esta página.");
      continue;
    }

    const itemsOnPage = await page.evaluate(() => {
      const rows = document.querySelectorAll("tbody tr.news-item");
      const lista = [];

      rows.forEach((row) => {
        const description =
          row.querySelector("td:nth-child(3) p")?.innerText.trim() || "";
        const category =
          row.querySelector("td:nth-child(4)")?.innerText.trim() || "";
        const estacion =
          row.querySelector("td:nth-child(5)")?.innerText.trim() || "";
        const location =
          row.querySelector("td:nth-child(7)")?.innerText.trim() || "";
        const stock =
          row.querySelector("td:nth-child(9)")?.innerText.trim() || "";

        if (description && category && estacion && location && stock) {
          lista.push({ description, category, estacion, location, stock });
        }
      });

      return lista;
    });

    console.log(
      `[ITEMS]   → ${itemsOnPage.length} productos encontrados en esta página.`
    );
    allProducts.push(...itemsOnPage);
  }

  return allProducts;
}

async function extractAllRewards(page) {
  const baseUrl =
    "https://adm.agrcloud.com.ar/filtered/items/2?minCost=0.00&maxCost=100000.00&minPoints=0.00&maxPoints=100000.00";
  const pageSize = 50;
  const buildUrl = (pageNumber) =>
    `${baseUrl}&page=${pageNumber}&pageSize=${pageSize}`;

  const allRewards = [];

  await gotoWithRetries(page, buildUrl(1), {
    waitUntil: "domcontentloaded",
    timeout: 240000,
  });

  try {
    await page.waitForSelector("tbody tr", { timeout: 15000 });
  } catch {}

  const totalPages = await page.evaluate(() => {
    const buttons = document.querySelectorAll("ul.pagination button.page");
    const nums = [];
    buttons.forEach((btn) => {
      const n = parseInt(btn.textContent.trim(), 10);
      if (!isNaN(n)) nums.push(n);
    });
    return nums.length ? Math.max(...nums) : 1;
  });

  console.log(`[ITEMS] Recompensas: se detectaron ${totalPages} páginas.`);

  for (let pageNumber = 1; pageNumber <= totalPages; pageNumber++) {
    console.log(`[ITEMS] Extrayendo recompensas de la página ${pageNumber}...`);
    await gotoWithRetries(page, buildUrl(pageNumber), {
      waitUntil: "domcontentloaded",
      timeout: 240000,
    });

    try {
      await page.waitForSelector("tbody tr", { timeout: 15000 });
    } catch {
      console.log("[ITEMS]   → No se encontraron filas en esta página.");
      continue;
    }

    const rewardsOnPage = await page.evaluate(() => {
      const rows = document.querySelectorAll("tbody tr");
      const lista = [];

      rows.forEach((row) => {
        const tds = row.querySelectorAll("td");
        if (tds.length === 0) return;

        let description = "";
        const descP =
          row.querySelector("td:nth-child(3) p") ||
          row.querySelector("td:nth-child(2) p") ||
          row.querySelector("p");

        if (descP) description = descP.innerText.trim();
        if (!description) return;

        const category = tds[3]?.innerText.trim() || "";
        const cost = tds[5]?.innerText.trim() || "";
        const price = tds[6]?.innerText.trim() || "";
        const points = tds[7]?.innerText.trim() || "";
        const status = tds[8]?.innerText.trim() || "";

        lista.push({ description, category, cost, price, points, status });
      });

      return lista;
    });

    console.log(
      `[ITEMS]   → ${rewardsOnPage.length} recompensas encontradas en esta página.`
    );
    allRewards.push(...rewardsOnPage);
  }

  return allRewards;
}

// ================== MAIN ==================
let isItemsRunning = false;

async function runItemsScraper() {
  if (isItemsRunning) {
    console.log("[ITEMS] Ya hay una ejecución en curso, se cancela esta.");
    return;
  }
  isItemsRunning = true;

  let browser;
  let mongoConnected = false;

  try {
    console.log("[ITEMS] Conectando a Mongo...");
    await mongoose.connect(mongoUri, {
      serverSelectionTimeoutMS: 30000,
      socketTimeoutMS: 240000,
    });
    mongoConnected = true;

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
        // Si volviera a fallar Chromium, probá sacarlo:
        "--single-process",
        "--renderer-process-limit=1",
      ],
      protocolTimeout: 240000,
    };

    console.log(
      `[ITEMS] Lanzando Puppeteer usando executablePath=${CHROME_EXECUTABLE_PATH}`
    );
    browser = await puppeteer.launch(launchOptions);

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(240000);
    page.setDefaultTimeout(240000);

    await page.setRequestInterception(true);
    page.on("request", (request) => {
      if (request.resourceType() === "image") request.abort();
      else request.continue();
    });

    await gotoWithRetries(page, "https://adm.agrcloud.com.ar", {
      waitUntil: "domcontentloaded",
      timeout: 240000,
    });

    await page.waitForSelector("input#Username.form-control.form-icon-input", {
      timeout: 30000,
    });

    await page.type("input#Username.form-control.form-icon-input", email);
    await page.type("input#Password.form-control.form-icon-input", password);

    await Promise.all([
      page.click("button[type='submit']"),
      page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 240000 }),
    ]);

    // Si sigue el login form, el login falló
    const stillLogin = await page.$(
      "input#Username.form-control.form-icon-input"
    );
    if (stillLogin) throw new Error("Login falló (sigue el formulario).");

    console.log("[ITEMS] Login OK, arrancamos scraping...");

    const products = await extractAllProducts(page);
    const rewards = await extractAllRewards(page);

    console.log(
      `[ITEMS] Productos totales: ${products.length}, recompensas totales: ${rewards.length}`
    );

    // Map por description normalizada (evita O(n^2) y problemas por espacios)
    const rewardMap = new Map(rewards.map((r) => [norm(r.description), r]));
    const stockMap = new Map();

    for (const p of products) {
      const key = norm(p.description);
      const stockVal = parseStock(p.stock);

      if (!stockMap.has(key)) {
        stockMap.set(key, {
          description: p.description,
          category: p.category,
          stock_bettica: 0,
          stock_grupogen: 0,
          stock_monteverde: 0,
          stock_tobago1: 0,
          stock_global: 0,
        });
      }

      const item = stockMap.get(key);

      if (p.location === "DEPOSITO BETTICA") item.stock_bettica += stockVal;
      if (p.location === "DEPOSITO GRUPO GEN") item.stock_grupogen += stockVal;
      if (p.location === "DEPOSITO MONTEVERDE")
        item.stock_monteverde += stockVal;
      if (p.location === "DEPOSITO TOBAGO 1") item.stock_tobago1 += stockVal;

      item.stock_global =
        item.stock_bettica +
        item.stock_grupogen +
        item.stock_monteverde +
        item.stock_tobago1;
    }

    const combinedData = Array.from(stockMap.entries()).map(([key, base]) => {
      const reward = rewardMap.get(key) || {};
      return { ...base, ...reward, scrapedAt: new Date() };
    });

    console.log(`[ITEMS] Items combinados: ${combinedData.length}`);

    await AgrItem.deleteMany({});
    if (combinedData.length) {
      await AgrItem.insertMany(combinedData, { ordered: false });
    }
    console.log("[ITEMS] Datos guardados en Mongo correctamente.");
  } catch (err) {
    console.error("[ITEMS] ERROR general:", err);
    throw err;
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch (e) {
        console.error("[ITEMS] Error al cerrar browser:", e.message);
      }
    }
    if (mongoConnected) await mongoose.disconnect().catch(() => {});
    isItemsRunning = false;
    console.log("[ITEMS] Proceso terminado.");
  }
}

module.exports = { runItemsScraper };
