// scrapers/items.js
require("dotenv").config();
const puppeteer = require("puppeteer");
const mongoose = require("mongoose");

const email = process.env.AGR_EMAIL;
const password = process.env.AGR_PASSWORD;
const mongoUri = process.env.MONGO_URI;

const executablePathFromEnv =
  process.env.CHROME_PATH || process.env.PUPPETEER_EXECUTABLE_PATH || null;

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

// ================== HELPERS DE SCRAPING ==================

async function extractAllProducts(page) {
  const baseUrl =
    "https://adm.agrcloud.com.ar/filtered/stocks/2?orderBy=description-asc";
  const pageSize = 50;
  const buildUrl = (pageNumber) =>
    `${baseUrl}&page=${pageNumber}&pageSize=${pageSize}`;

  const allProducts = [];

  await page.goto(buildUrl(1), { waitUntil: "networkidle2" });

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
    await page.goto(buildUrl(pageNumber), { waitUntil: "networkidle2" });

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

  await page.goto(buildUrl(1), { waitUntil: "networkidle2" });

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
    await page.goto(buildUrl(pageNumber), { waitUntil: "networkidle2" });

    try {
      await page.waitForSelector("tbody tr", { timeout: 10000 });
    } catch {
      console.log("[ITEMS]   → No se encontraron filas en esta página.");
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

        if (descP) {
          description = descP.innerText.trim();
        }
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

async function runItemsScraper() {
  console.log("[ITEMS] Conectando a Mongo...");
  await mongoose.connect(mongoUri);

  const launchOptions = {
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
    protocolTimeout: 240000,
  };

  if (executablePathFromEnv) {
    console.log(
      `[ITEMS] Lanzando Puppeteer usando executablePath=${executablePathFromEnv}`
    );
    launchOptions.executablePath = executablePathFromEnv;
  } else {
    console.log("[ITEMS] Lanzando Puppeteer con executablePath por defecto");
  }

  console.log("[ITEMS] Lanzando Puppeteer...");
  const browser = await puppeteer.launch(launchOptions);
  const page = await browser.newPage();

  page.setDefaultNavigationTimeout(240000);
  page.setDefaultTimeout(240000);

  await page.setRequestInterception(true);
  page.on("request", (request) => {
    if (request.resourceType() === "image") {
      request.abort();
    } else {
      request.continue();
    }
  });

  await page.goto("https://adm.agrcloud.com.ar", {
    waitUntil: "networkidle2",
    timeout: 240000,
  });

  await page.type("input#Username.form-control.form-icon-input", email);
  await page.type("input#Password.form-control.form-icon-input", password);

  await Promise.all([
    page.click("button[type='submit']"),
    page.waitForNavigation({ waitUntil: "networkidle2", timeout: 240000 }),
  ]);

  console.log("[ITEMS] Login OK, arrancamos scraping...");

  const products = await extractAllProducts(page);
  const rewards = await extractAllRewards(page);

  console.log(
    `[ITEMS] Productos totales: ${products.length}, recompensas totales: ${rewards.length}`
  );

  const combinedData = [];

  for (const product of products) {
    const stockVal = parseInt(product.stock, 10) || 0;
    let existing = combinedData.find(
      (item) => item.description === product.description
    );

    if (!existing) {
      const base = {
        description: product.description,
        category: product.category,
        stock_bettica:
          product.location === "DEPOSITO BETTICA" ? stockVal : 0,
        stock_grupogen:
          product.location === "DEPOSITO GRUPO GEN" ? stockVal : 0,
        stock_monteverde:
          product.location === "DEPOSITO MONTEVERDE" ? stockVal : 0,
        stock_tobago1:
          product.location === "DEPOSITO TOBAGO 1" ? stockVal : 0,
      };
      base.stock_global =
        base.stock_bettica +
        base.stock_grupogen +
        base.stock_monteverde +
        base.stock_tobago1;

      const reward =
        rewards.find((r) => r.description === product.description) || {};

      combinedData.push({ ...base, ...reward });
    } else {
      if (product.location === "DEPOSITO BETTICA")
        existing.stock_bettica += stockVal;
      if (product.location === "DEPOSITO GRUPO GEN")
        existing.stock_grupogen += stockVal;
      if (product.location === "DEPOSITO MONTEVERDE")
        existing.stock_monteverde += stockVal;
      if (product.location === "DEPOSITO TOBAGO 1")
        existing.stock_tobago1 += stockVal;

      existing.stock_global =
        existing.stock_bettica +
        existing.stock_grupogen +
        existing.stock_monteverde +
        existing.stock_tobago1;
    }
  }

  console.log(`[ITEMS] Items combinados: ${combinedData.length}`);

  await AgrItem.deleteMany({});
  await AgrItem.insertMany(combinedData);
  console.log("[ITEMS] Datos guardados en Mongo correctamente.");

  await browser.close();
  await mongoose.disconnect();
  console.log("[ITEMS] Proceso terminado.");
}

module.exports = { runItemsScraper };
