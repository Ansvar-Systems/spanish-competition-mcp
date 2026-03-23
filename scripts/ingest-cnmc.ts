/**
 * Ingestion crawler for the CNMC (Comisión Nacional de los Mercados y la
 * Competencia) website.
 *
 * Scrapes competition decisions (resoluciones de competencia), merger-control
 * decisions (concentraciones), and sanction proceedings (expedientes
 * sancionadores) from cnmc.es and inserts them into the SQLite database.
 *
 * Usage:
 *   npx tsx scripts/ingest-cnmc.ts                  # full crawl
 *   npx tsx scripts/ingest-cnmc.ts --resume         # skip already-ingested case numbers
 *   npx tsx scripts/ingest-cnmc.ts --dry-run        # parse but do not write to DB
 *   npx tsx scripts/ingest-cnmc.ts --force          # delete DB and start fresh
 *   npx tsx scripts/ingest-cnmc.ts --max-pages 5    # limit listing pages (for testing)
 *   npx tsx scripts/ingest-cnmc.ts --mergers-only   # only crawl merger decisions
 *   npx tsx scripts/ingest-cnmc.ts --decisions-only # only crawl competition decisions
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";
import * as cheerio from "cheerio";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = "https://www.cnmc.es";

/**
 * CNMC expedientes listing endpoint.
 *
 * Key query parameters:
 *   idambito  — area of activity (2 = Competencia)
 *   idprocedim — procedure type:
 *     35 = Conductas (antitrust enforcement — cartels, abuse of dominance)
 *     37 = Concentraciones (merger control)
 *     All = all types
 *   idtipoexp — case subtype (All for everything)
 *   page — zero-based pagination
 *   t — free-text search term
 */
const DECISIONS_LIST_PATH = "/expedientes";
const DECISIONS_PARAMS = "t=&idambito=2&idprocedim=35&hidprocedim=35&idtipoexp=All";
const MERGERS_PARAMS = "t=&idambito=2&idprocedim=37&hidprocedim=37&idtipoexp=All";

const DB_PATH = process.env["CNMC_DB_PATH"] ?? "data/cnmc.db";
const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3_000;
const REQUEST_TIMEOUT_MS = 30_000;

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME = args.includes("--resume");
const FLAG_DRY_RUN = args.includes("--dry-run");
const FLAG_FORCE = args.includes("--force");
const FLAG_MERGERS_ONLY = args.includes("--mergers-only");
const FLAG_DECISIONS_ONLY = args.includes("--decisions-only");

function getFlagValue(flag: string): string | undefined {
  const idx = args.indexOf(flag);
  if (idx === -1 || idx + 1 >= args.length) return undefined;
  return args[idx + 1];
}

const MAX_PAGES = getFlagValue("--max-pages")
  ? parseInt(getFlagValue("--max-pages")!, 10)
  : Infinity;

// ---------------------------------------------------------------------------
// Spanish month map
// ---------------------------------------------------------------------------

const SPANISH_MONTHS: Record<string, string> = {
  enero: "01",
  febrero: "02",
  marzo: "03",
  abril: "04",
  mayo: "05",
  junio: "06",
  julio: "07",
  agosto: "08",
  septiembre: "09",
  setiembre: "09",
  octubre: "10",
  noviembre: "11",
  diciembre: "12",
};

/**
 * Parse a Spanish date string like "28 de noviembre de 2023" or
 * "28 noviembre 2023" into "2023-11-28". Also handles "DD/MM/YYYY".
 * Returns null if unparseable.
 */
function parseSpanishDate(raw: string): string | null {
  const cleaned = raw.trim().toLowerCase();

  // Try "DD de monthName de YYYY" or "DD monthName YYYY"
  const longMatch = cleaned.match(
    /(\d{1,2})\s+(?:de\s+)?(\S+)\s+(?:de\s+)?(\d{4})/,
  );
  if (longMatch) {
    const [, day, monthName, year] = longMatch;
    const month = SPANISH_MONTHS[monthName!];
    if (month && day && year) {
      return `${year}-${month}-${day.padStart(2, "0")}`;
    }
  }

  // Try "DD/MM/YYYY"
  const slashMatch = cleaned.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slashMatch) {
    const [, day, month, year] = slashMatch;
    if (day && month && year) {
      return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
    }
  }

  // Try "YYYY-MM-DD" (already ISO)
  const isoMatch = cleaned.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) return cleaned;

  return null;
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimit(): Promise<void> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }
  lastRequestTime = Date.now();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url: string, retries = MAX_RETRIES): Promise<string> {
  await rateLimit();
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
      const res = await fetch(url, {
        signal: controller.signal,
        headers: {
          "User-Agent":
            "AnsvarCnmcIngester/1.0 (+https://ansvar.eu; competition-law-research)",
          Accept: "text/html,application/xhtml+xml",
          "Accept-Language": "es-ES,es;q=0.9,en;q=0.5",
        },
      });
      clearTimeout(timeoutId);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}`);
      }
      return await res.text();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt < retries) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        log(`  WARN: attempt ${attempt}/${retries} failed for ${url}: ${msg} — retrying in ${backoff}ms`);
        await sleep(backoff);
      } else {
        throw new Error(`Failed after ${retries} attempts for ${url}: ${msg}`);
      }
    }
  }
  // Unreachable, but TypeScript wants it
  throw new Error("fetchWithRetry fell through");
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

const stats = {
  decisionsScraped: 0,
  decisionsInserted: 0,
  decisionsSkipped: 0,
  mergersScraped: 0,
  mergersInserted: 0,
  mergersSkipped: 0,
  errors: 0,
  sectorsUpserted: 0,
};

function log(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19).replace("T", " ");
  console.log(`[${ts}] ${msg}`);
}

// ---------------------------------------------------------------------------
// Database setup
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    log(`Created directory ${dir}`);
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  log(`Database ready at ${DB_PATH}`);
  return db;
}

// ---------------------------------------------------------------------------
// Sector normalisation
// ---------------------------------------------------------------------------

/** Map common Spanish sector names from the CNMC to our sector IDs. */
const SECTOR_MAP: Record<string, { id: string; name: string; name_en: string }> = {
  telecomunicaciones: { id: "telecomunicaciones", name: "Telecomunicaciones", name_en: "Telecommunications" },
  telecom: { id: "telecomunicaciones", name: "Telecomunicaciones", name_en: "Telecommunications" },
  energía: { id: "energia", name: "Energía", name_en: "Energy" },
  energia: { id: "energia", name: "Energía", name_en: "Energy" },
  "energía eléctrica": { id: "energia", name: "Energía", name_en: "Energy" },
  electricidad: { id: "energia", name: "Energía", name_en: "Energy" },
  "gas natural": { id: "energia", name: "Energía", name_en: "Energy" },
  hidrocarburos: { id: "energia", name: "Energía", name_en: "Energy" },
  "distribución alimentaria": { id: "distribucion_alimentaria", name: "Distribución Alimentaria", name_en: "Food Distribution and Retail" },
  "distribucion alimentaria": { id: "distribucion_alimentaria", name: "Distribución Alimentaria", name_en: "Food Distribution and Retail" },
  alimentación: { id: "distribucion_alimentaria", name: "Distribución Alimentaria", name_en: "Food Distribution and Retail" },
  alimentacion: { id: "distribucion_alimentaria", name: "Distribución Alimentaria", name_en: "Food Distribution and Retail" },
  "gran consumo": { id: "distribucion_alimentaria", name: "Distribución Alimentaria", name_en: "Food Distribution and Retail" },
  supermercados: { id: "distribucion_alimentaria", name: "Distribución Alimentaria", name_en: "Food Distribution and Retail" },
  digital: { id: "digital", name: "Economía Digital y Plataformas", name_en: "Digital Economy and Platforms" },
  "economía digital": { id: "digital", name: "Economía Digital y Plataformas", name_en: "Digital Economy and Platforms" },
  "economia digital": { id: "digital", name: "Economía Digital y Plataformas", name_en: "Digital Economy and Platforms" },
  plataformas: { id: "digital", name: "Economía Digital y Plataformas", name_en: "Digital Economy and Platforms" },
  internet: { id: "digital", name: "Economía Digital y Plataformas", name_en: "Digital Economy and Platforms" },
  transporte: { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics" },
  "transporte y logística": { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics" },
  logística: { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics" },
  logistica: { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics" },
  ferrocarril: { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics" },
  "transporte aéreo": { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics" },
  "transporte aereo": { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics" },
  "servicios financieros": { id: "servicios_financieros", name: "Servicios Financieros", name_en: "Financial Services" },
  banca: { id: "servicios_financieros", name: "Servicios Financieros", name_en: "Financial Services" },
  seguros: { id: "servicios_financieros", name: "Servicios Financieros", name_en: "Financial Services" },
  finanzas: { id: "servicios_financieros", name: "Servicios Financieros", name_en: "Financial Services" },
  sanidad: { id: "sanidad", name: "Sanidad y Farmacia", name_en: "Healthcare and Pharmaceuticals" },
  salud: { id: "sanidad", name: "Sanidad y Farmacia", name_en: "Healthcare and Pharmaceuticals" },
  farmacia: { id: "sanidad", name: "Sanidad y Farmacia", name_en: "Healthcare and Pharmaceuticals" },
  "industria farmacéutica": { id: "sanidad", name: "Sanidad y Farmacia", name_en: "Healthcare and Pharmaceuticals" },
  "industria farmaceutica": { id: "sanidad", name: "Sanidad y Farmacia", name_en: "Healthcare and Pharmaceuticals" },
  construcción: { id: "construccion", name: "Construcción", name_en: "Construction" },
  construccion: { id: "construccion", name: "Construcción", name_en: "Construction" },
  "obras públicas": { id: "construccion", name: "Construcción", name_en: "Construction" },
  "obras publicas": { id: "construccion", name: "Construcción", name_en: "Construction" },
  automoción: { id: "automocion", name: "Automoción", name_en: "Automotive" },
  automocion: { id: "automocion", name: "Automoción", name_en: "Automotive" },
  automóvil: { id: "automocion", name: "Automoción", name_en: "Automotive" },
  automovil: { id: "automocion", name: "Automoción", name_en: "Automotive" },
  audiovisual: { id: "audiovisual", name: "Audiovisual y Medios", name_en: "Audiovisual and Media" },
  medios: { id: "audiovisual", name: "Audiovisual y Medios", name_en: "Audiovisual and Media" },
  "medios de comunicación": { id: "audiovisual", name: "Audiovisual y Medios", name_en: "Audiovisual and Media" },
  publicidad: { id: "audiovisual", name: "Audiovisual y Medios", name_en: "Audiovisual and Media" },
  postal: { id: "postal", name: "Servicios Postales", name_en: "Postal Services" },
  agricultura: { id: "agricultura", name: "Agricultura y Alimentación", name_en: "Agriculture" },
  agroalimentario: { id: "agricultura", name: "Agricultura y Alimentación", name_en: "Agriculture" },
  inmobiliario: { id: "inmobiliario", name: "Inmobiliario", name_en: "Real Estate" },
  turismo: { id: "turismo", name: "Turismo", name_en: "Tourism" },
  hostelería: { id: "turismo", name: "Turismo y Hostelería", name_en: "Tourism and Hospitality" },
  hosteleria: { id: "turismo", name: "Turismo y Hostelería", name_en: "Tourism and Hospitality" },
  industria: { id: "industria", name: "Industria", name_en: "Industry" },
  "servicios profesionales": { id: "servicios_profesionales", name: "Servicios Profesionales", name_en: "Professional Services" },
  "profesiones reguladas": { id: "servicios_profesionales", name: "Servicios Profesionales", name_en: "Professional Services" },
  deporte: { id: "deporte", name: "Deporte", name_en: "Sport" },
  medioambiente: { id: "medioambiente", name: "Medioambiente", name_en: "Environment" },
  "medio ambiente": { id: "medioambiente", name: "Medioambiente", name_en: "Environment" },
  residuos: { id: "medioambiente", name: "Medioambiente", name_en: "Environment" },
};

function normaliseSector(rawSector: string): { id: string; name: string; name_en: string } {
  const key = rawSector.trim().toLowerCase();
  const mapped = SECTOR_MAP[key];
  if (mapped) return mapped;
  // Generate a slug from the raw text
  const id = key
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  return { id, name: rawSector.trim(), name_en: rawSector.trim() };
}

// ---------------------------------------------------------------------------
// Case type classification
// ---------------------------------------------------------------------------

/**
 * Determine the decision type from the CNMC case number.
 *
 * CNMC case number patterns:
 *   S/NNNN/YY or S/DC/NNNN/YY  — Sancionador (cartel, abuse, restrictive conduct)
 *   SNC/NNNN/YY                 — Sancionador nacional competencia
 *   SDCR/NNNN/YY                — Subdirección conductas restrictivas
 *   VS/NNNN/YY                  — Vigilancia sancionador
 *   IPN/CNMC/NNN/YY             — Informe / opinion
 *   C/NNNN/YY                   — Concentración (merger control)
 *   G-YYYY-NN                   — Guía (guidance)
 *   SNC/DE/NNN/YY               — Sancionador energía
 *   RDC/DE/NNN/YY               — Resolución dirección competencia
 */
function classifyCaseType(caseNumber: string): string {
  const upper = caseNumber.toUpperCase();
  if (/^C\//.test(upper)) return "merger";
  if (/^IPN\//.test(upper)) return "sector_inquiry";
  if (/^G-/.test(upper)) return "guidance";
  if (/^VS\//.test(upper)) return "surveillance";
  if (/^SNC\/DE\//.test(upper)) return "sanction_energy";
  if (/^SNC\//.test(upper)) return "abuse_of_dominance";
  if (/^SDCR\//.test(upper)) return "abuse_of_dominance";
  if (/^S\/DC\//.test(upper)) return "cartel";
  if (/^S\//.test(upper)) return "cartel";
  if (/^RDC\//.test(upper)) return "regulatory_decision";
  return "decision";
}

// ---------------------------------------------------------------------------
// Outcome normalisation
// ---------------------------------------------------------------------------

/** Normalise Spanish disposition text into our outcome taxonomy. */
function normaliseOutcome(raw: string): string {
  const lower = raw.toLowerCase();
  if (/multa|sanción pecuniaria|sancion pecuniaria|sanción económica/.test(lower)) return "fine";
  if (/compromiso/.test(lower)) return "commitments";
  if (/orden de cesación|cesación|cesacion|orden de cese/.test(lower)) return "injunction";
  if (/archivo|archivado|desestim|inadmisi/.test(lower)) return "dismissed";
  if (/medidas cautelares/.test(lower)) return "interim_measures";
  if (/transacción|transaccion/.test(lower)) return "settlement";
  if (/terminación convencional|terminacion convencional/.test(lower)) return "settlement";
  if (/autorización con(?:diciones| compromisos)|autorizacion con(?:diciones| compromisos)|condicionad/.test(lower))
    return "cleared_with_conditions";
  if (/autorización en primera fase|autorizacion en primera fase|autorizada? fase 1|primera fase/.test(lower))
    return "cleared_phase1";
  if (/autorización en segunda fase|autorizacion en segunda fase|autorizada? fase 2|segunda fase/.test(lower))
    return "cleared_phase2";
  if (/autorización|autorizacion|autoriz/.test(lower)) return "cleared";
  if (/prohibición|prohibicion|prohibid|denegad/.test(lower)) return "prohibited";
  if (/segunda fase|fase\s*2|examen en profundidad/.test(lower)) return "phase_2_referral";
  if (/infracción|infraccion|declaración de infracción/.test(lower)) return "infringement_found";
  if (/no infracción|no infraccion|no se aprecia/.test(lower)) return "no_infringement";
  return raw.trim();
}

// ---------------------------------------------------------------------------
// Case number URL slug
// ---------------------------------------------------------------------------

/**
 * Convert a CNMC case number to its URL slug.
 *
 * The CNMC website maps case numbers to URL slugs by removing slashes and
 * lowercasing. Examples:
 *   S/0020/19     → s002019
 *   SNC/0025/12   → snc002512
 *   C/1520/24     → c152024
 *   S/DC/0598/16  → sdc059816
 *   SNC/DE/024/17 → sncde02417
 *   IPN/CNMC/023/24 → ipncnmc02324
 */
function caseNumberToSlug(caseNumber: string): string {
  return caseNumber.replace(/\//g, "").toLowerCase();
}

// ---------------------------------------------------------------------------
// Listing page parser: decisions (conductas)
// ---------------------------------------------------------------------------

interface ListingItem {
  caseNumber: string;
  title: string;
  date: string | null;
  type: string;
  sectors: string[];
  detailUrl: string;
}

/**
 * Parse one page of the CNMC expedientes listing.
 *
 * The CNMC website (Drupal-based) renders expediente results as items in a
 * view. Each result contains:
 *   - A link to the case detail page: /expedientes/{slug}
 *   - The case number (e.g. "S/0020/19") in the link text or title
 *   - A title describing the case
 *   - Metadata: date, sector, expediente type
 *
 * The listing uses Drupal Views with items in .view-content, each row
 * typically wrapped in .views-row or similar container divs. We look for
 * links matching the /expedientes/ pattern and extract metadata from the
 * surrounding context.
 */
function parseDecisionListing(html: string): ListingItem[] {
  const $ = cheerio.load(html);
  const items: ListingItem[] = [];

  // Look for links to expediente detail pages
  const links = $('a[href*="/expedientes/"]');

  links.each((_i, el) => {
    const $a = $(el);
    const href = $a.attr("href");
    if (!href) return;

    // Skip non-case links (breadcrumbs, nav, search form, pagination)
    if ($a.closest("nav, footer, .menu, .breadcrumb, .pager, .tabs").length > 0) return;

    // Skip links to the listing page itself
    if (href === "/expedientes" || href === "/expedientes/" || href.includes("?")) return;

    const rawText = $a.text().trim();
    if (!rawText) return;

    // Extract case number from the text. CNMC case numbers follow patterns
    // like S/0020/19, SNC/0025/12, C/1520/24, S/DC/0598/16, IPN/CNMC/023/24
    const caseMatch = rawText.match(
      /^((?:S\/DC|SNC\/DE|SNC|SDCR|IPN\/CNMC|VS|RDC\/DE|S|C|G)[\s/\-][\w/\-]+)/i,
    );

    let caseNumber: string;
    let title: string;

    if (caseMatch) {
      caseNumber = caseMatch[1]!.trim();
      title = rawText.replace(caseMatch[0], "").replace(/^\s*[-–—:]\s*/, "").trim();
    } else {
      // The link text might just be a title; try to extract case number from href
      const slugMatch = href.match(/\/expedientes\/([a-z0-9]+)$/i);
      if (!slugMatch) return;
      // Reconstruct case number from slug is unreliable; use full text as title
      caseNumber = rawText.split(/\s*[-–—]\s*/)[0]?.trim() || rawText;
      title = rawText;
    }

    if (!caseNumber || caseNumber.length < 3) return;

    // Determine type from case number
    const type = classifyCaseType(caseNumber);

    // Skip merger cases when we are parsing the decisions listing
    // (they will be handled by the merger listing parser)
    if (type === "merger") return;

    // Walk the surrounding context for date and sector info
    let dateStr: string | null = null;
    const sectors: string[] = [];

    const $parent = $a.closest(".views-row, .view-row, tr, li, article, .node");
    const $container = $parent.length > 0 ? $parent : $a.parent().parent();
    const containerText = $container.text();

    // Extract date from container text — Spanish date patterns
    const dateMatch = containerText.match(
      /(\d{1,2})\s+(?:de\s+)?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?(\d{4})/i,
    );
    if (dateMatch) {
      dateStr = parseSpanishDate(dateMatch[0]);
    }

    // Also try DD/MM/YYYY format
    if (!dateStr) {
      const slashDate = containerText.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      if (slashDate) {
        dateStr = parseSpanishDate(slashDate[0]);
      }
    }

    // Extract sectors from links or text with sector-related classes
    $container.find('a[href*="sector"], a[href*="ambito"], .field--name-field-sector').each((_j, sectorEl) => {
      const sectorText = $(sectorEl).text().trim();
      if (sectorText && sectorText.length < 100) {
        sectors.push(sectorText);
      }
    });

    // Build full detail URL
    const detailUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;

    items.push({
      caseNumber,
      title: title || rawText,
      date: dateStr,
      type,
      sectors,
      detailUrl,
    });
  });

  // Deduplicate by case number
  const seen = new Set<string>();
  return items.filter((item) => {
    if (seen.has(item.caseNumber)) return false;
    seen.add(item.caseNumber);
    return true;
  });
}

// ---------------------------------------------------------------------------
// Listing page parser: mergers (concentraciones)
// ---------------------------------------------------------------------------

interface MergerListingItem {
  caseNumber: string;
  title: string;
  date: string | null;
  sectors: string[];
  detailUrl: string;
}

function parseMergerListing(html: string): MergerListingItem[] {
  const $ = cheerio.load(html);
  const items: MergerListingItem[] = [];

  const links = $('a[href*="/expedientes/"]');

  links.each((_i, el) => {
    const $a = $(el);
    const href = $a.attr("href");
    if (!href) return;

    if ($a.closest("nav, footer, .menu, .breadcrumb, .pager, .tabs").length > 0) return;
    if (href === "/expedientes" || href === "/expedientes/" || href.includes("?")) return;

    const rawText = $a.text().trim();
    if (!rawText) return;

    // Merger case numbers start with C/
    const caseMatch = rawText.match(/^(C\/\d{3,4}\/\d{2})\b/i);
    if (!caseMatch) {
      // Also accept case numbers without the full format if the slug starts with 'c'
      const slugMatch = href.match(/\/expedientes\/(c\d{4,6})$/i);
      if (!slugMatch) return;
      // Skip non-concentration cases
      return;
    }
    const caseNumber = caseMatch[1]!;
    const title = rawText.replace(caseMatch[0], "").replace(/^\s*[-–—:]\s*/, "").trim();

    let dateStr: string | null = null;
    const sectors: string[] = [];

    const $parent = $a.closest(".views-row, .view-row, tr, li, article, .node");
    const $container = $parent.length > 0 ? $parent : $a.parent().parent();
    const containerText = $container.text();

    const dateMatch = containerText.match(
      /(\d{1,2})\s+(?:de\s+)?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?(\d{4})/i,
    );
    if (dateMatch) {
      dateStr = parseSpanishDate(dateMatch[0]);
    }

    if (!dateStr) {
      const slashDate = containerText.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      if (slashDate) dateStr = parseSpanishDate(slashDate[0]);
    }

    $container.find('a[href*="sector"], a[href*="ambito"], .field--name-field-sector').each((_j, sectorEl) => {
      const sectorText = $(sectorEl).text().trim();
      if (sectorText && sectorText.length < 100) sectors.push(sectorText);
    });

    const detailUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;

    items.push({
      caseNumber,
      title: title || rawText,
      date: dateStr,
      sectors,
      detailUrl,
    });
  });

  const seen = new Set<string>();
  return items.filter((item) => {
    if (seen.has(item.caseNumber)) return false;
    seen.add(item.caseNumber);
    return true;
  });
}

// ---------------------------------------------------------------------------
// Detail page parser: decisions
// ---------------------------------------------------------------------------

interface DecisionDetail {
  caseNumber: string;
  title: string;
  date: string | null;
  type: string;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  fullText: string;
  outcome: string | null;
  fineAmount: number | null;
  ldcArticles: string | null;
  status: string;
}

function parseDecisionDetail(html: string, fallback: ListingItem): DecisionDetail {
  const $ = cheerio.load(html);

  // --- Case number & date from <h1> ---
  const h1Text = $("h1").first().text().trim();
  let caseNumber = fallback.caseNumber;
  let date = fallback.date;

  // h1 patterns: "S/0020/19 - TÍTULO ELECTRÓNICO SIGNE" or similar
  const h1CaseMatch = h1Text.match(
    /((?:S\/DC|SNC\/DE|SNC|SDCR|IPN\/CNMC|VS|RDC\/DE|S|C)[\s/\-][\w/\-]+)/i,
  );
  if (h1CaseMatch) caseNumber = h1CaseMatch[1]!.trim();

  // Try to extract date from the page metadata
  const h1DateMatch = h1Text.match(
    /(\d{1,2})\s+(?:de\s+)?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?(\d{4})/i,
  );
  if (h1DateMatch) {
    date = parseSpanishDate(h1DateMatch[0]);
  }

  // --- Title ---
  let title = h1Text || fallback.title;
  // Strip case number prefix from title
  title = title
    .replace(
      /^(?:S\/DC|SNC\/DE|SNC|SDCR|IPN\/CNMC|VS|RDC\/DE|S|C)[\s/\-][\w/\-]+\s*[-–—:]\s*/i,
      "",
    )
    .trim();
  if (!title) title = fallback.title;

  // --- Metadata from Drupal fields ---
  const pageText = $.text();

  // Parties / Empresas
  let parties: string | null = null;
  const companiesText = extractFieldValue($, pageText, [
    "Empresas",
    "Empresa(s)",
    "Partes",
    "Parte(s)",
    "Empresas implicadas",
    "Empresas afectadas",
    "Denunciada",
    "Denunciado",
  ]);
  if (companiesText) {
    parties = companiesText.trim();
  }

  // If no structured parties, try extracting from h1 — CNMC titles often
  // include the company name after the case number
  if (!parties && title) {
    parties = title.split(/\s*[-–—]\s*/)[0]?.trim() || null;
  }

  // Fine amount
  let fineAmount: number | null = null;
  const fineText = extractFieldValue($, pageText, [
    "Sanción",
    "Sancion",
    "Sanciones",
    "Multa",
    "Importe de la sanción",
    "Importe de la multa",
  ]);
  if (fineText) {
    fineAmount = parseSpanishFineAmount(fineText);
  }

  // Also search the page text for fine amounts if not found in structured fields
  if (fineAmount === null) {
    fineAmount = extractFineFromText(pageText);
  }

  // Legal basis / LDC articles
  let ldcArticles: string | null = null;
  const legalBasis = extractFieldValue($, pageText, [
    "Artículos",
    "Articulos",
    "Fundamento jurídico",
    "Fundamento juridico",
    "Base jurídica",
    "Base juridica",
    "Artículos LDC",
    "Preceptos infringidos",
  ]);
  if (legalBasis) {
    ldcArticles = legalBasis.trim();
  }

  // Outcome / Resolución
  let outcome: string | null = null;
  const resolution = extractFieldValue($, pageText, [
    "Resolución",
    "Resolucion",
    "Sentido de la resolución",
    "Tipo de resolución",
    "Resultado",
    "Fallo",
  ]);
  if (resolution) {
    outcome = normaliseOutcome(resolution);
  }

  // Status — check for appeal / recurso mentions
  let status = "final";
  const recurso = extractFieldValue($, pageText, [
    "Recurso",
    "Recursos",
    "Impugnación",
    "Impugnacion",
    "Estado",
    "Situación procesal",
  ]);
  if (recurso) {
    const recursoLower = recurso.toLowerCase();
    if (/recurso|impugna|pendiente/.test(recursoLower)) {
      status = "appealed";
    }
    if (/en tramitación|en tramitacion|en curso/.test(recursoLower)) {
      status = "pending";
    }
  }

  // Sector — from listing data or page metadata
  let sector: string | null = null;
  if (fallback.sectors.length > 0) {
    const norm = normaliseSector(fallback.sectors[0]!);
    sector = norm.id;
  } else {
    const sectorText = extractFieldValue($, pageText, [
      "Sector",
      "Sector(es)",
      "Sectores",
      "Ámbito",
      "Ambito",
    ]);
    if (sectorText) {
      const norm = normaliseSector(sectorText.split(",")[0]!.trim());
      sector = norm.id;
    }
  }

  // Summary — look for "Resumen" section or description meta fields
  let summary: string | null = null;

  // Try meta description first
  const metaDesc = $('meta[name="description"]').attr("content");
  if (metaDesc && metaDesc.length > 50) {
    summary = metaDesc.trim();
  }

  // Try headings containing "Resumen" or "Descripción"
  if (!summary) {
    $("h2, h3, h4").each((_i, heading) => {
      if (summary) return; // already found
      const headingText = $(heading).text().trim().toLowerCase();
      if (headingText.includes("resumen") || headingText.includes("descripción") || headingText.includes("descripcion")) {
        const parts: string[] = [];
        let $next = $(heading).next();
        while ($next.length > 0 && !$next.is("h1, h2, h3, h4")) {
          const text = $next.text().trim();
          if (text) parts.push(text);
          $next = $next.next();
        }
        if (parts.length > 0) {
          summary = parts.join("\n\n");
        }
      }
    });
  }

  // Full text — collect the main body content
  let fullText = "";
  if (summary) fullText = summary;

  // Drupal content area selectors — try the most specific first
  const contentSelectors = [
    ".node__content",
    ".field--name-body",
    "article .content",
    ".field--name-field-contenido",
    ".field--name-field-descripcion",
    "#block-cnmc-content",
    "main article",
    ".region-content",
    "main",
  ];

  for (const sel of contentSelectors) {
    const $content = $(sel);
    if ($content.length > 0) {
      $content.find("nav, .menu, .breadcrumb, script, style, .visually-hidden, .tabs, .pager").remove();
      const bodyText = $content.text().trim();
      if (bodyText.length > fullText.length) {
        fullText = bodyText;
      }
      break;
    }
  }

  // Last resort: use entire body minus boilerplate
  if (!fullText || fullText.length < 100) {
    $("nav, footer, header, script, style, .menu, .breadcrumb, .visually-hidden, .tabs, .pager").remove();
    fullText = $("body").text().trim();
  }

  // Clean up whitespace
  fullText = fullText.replace(/\s{3,}/g, "\n\n").trim();

  // Determine type from case number
  const type = classifyCaseType(caseNumber);

  return {
    caseNumber,
    title,
    date,
    type,
    sector,
    parties,
    summary,
    fullText,
    outcome,
    fineAmount,
    ldcArticles,
    status,
  };
}

// ---------------------------------------------------------------------------
// Detail page parser: mergers
// ---------------------------------------------------------------------------

interface MergerDetail {
  caseNumber: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiringParty: string | null;
  target: string | null;
  summary: string | null;
  fullText: string;
  outcome: string | null;
  turnover: number | null;
}

function parseMergerDetail(html: string, fallback: MergerListingItem): MergerDetail {
  const $ = cheerio.load(html);

  const h1Text = $("h1").first().text().trim();
  let caseNumber = fallback.caseNumber;
  let date = fallback.date;

  const h1CaseMatch = h1Text.match(/(C\/\d{3,4}\/\d{2})/i);
  if (h1CaseMatch) caseNumber = h1CaseMatch[1]!;

  const h1DateMatch = h1Text.match(
    /(\d{1,2})\s+(?:de\s+)?(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+(?:de\s+)?(\d{4})/i,
  );
  if (h1DateMatch) {
    date = parseSpanishDate(h1DateMatch[0]);
  }

  let title = h1Text || fallback.title;
  // Strip case number prefix
  title = title.replace(/^C\/\d{3,4}\/\d{2}\s*[-–—:]\s*/i, "").trim();
  if (!title) title = fallback.title;

  const pageText = $.text();

  // Acquiring party and target — CNMC merger titles typically follow:
  // "EMPRESA A / EMPRESA B" or "EMPRESA A - EMPRESA B"
  let acquiringParty: string | null = null;
  let target: string | null = null;

  // Try structured fields first
  const notificante = extractFieldValue($, pageText, [
    "Parte notificante",
    "Partes notificantes",
    "Adquirente",
    "Compradora",
    "Empresa adquirente",
  ]);
  if (notificante) acquiringParty = notificante.trim();

  const cible = extractFieldValue($, pageText, [
    "Empresa adquirida",
    "Sociedad adquirida",
    "Objeto de la operación",
    "Empresa objetivo",
    "Target",
  ]);
  if (cible) target = cible.trim();

  // If no structured parties, try parsing from title
  // Merger titles typically use "/" or " - " to separate parties
  if (!acquiringParty && !target) {
    const titleParts = title.split(/\s*[/]\s*/);
    if (titleParts.length >= 2) {
      acquiringParty = titleParts[0]?.trim() || null;
      target = titleParts.slice(1).join(" / ").trim() || null;
    }
  }

  // Also try "adquisición de X por Y" / "toma de control de X por Y"
  if (!acquiringParty && !target) {
    const partyMatch = title.match(
      /(?:adquisición|adquisicion|toma\s+de\s+control)\s+(?:exclusiv[oa]\s+)?(?:de\s+)?(?:la\s+(?:sociedad|empresa)\s+)?(.+?)\s+por\s+(?:parte\s+de\s+)?(?:la\s+(?:sociedad|empresa)\s+)?(.+?)(?:\.|$)/i,
    );
    if (partyMatch) {
      target = partyMatch[1]?.trim() || null;
      acquiringParty = partyMatch[2]?.trim() || null;
    }
  }

  // Outcome
  let outcome: string | null = null;
  const sens = extractFieldValue($, pageText, [
    "Resolución",
    "Resolucion",
    "Sentido de la resolución",
    "Tipo de resolución",
    "Resultado",
    "Tipo de control",
    "Fase de análisis",
  ]);
  if (sens) {
    outcome = normaliseOutcome(sens);
  }

  // Sector
  let sector: string | null = null;
  if (fallback.sectors.length > 0) {
    sector = normaliseSector(fallback.sectors[0]!).id;
  } else {
    const sectorText = extractFieldValue($, pageText, [
      "Sector",
      "Sector(es)",
      "Sectores",
      "Ámbito",
      "Mercado afectado",
    ]);
    if (sectorText) {
      sector = normaliseSector(sectorText.split(",")[0]!.trim()).id;
    }
  }

  // Summary
  let summary: string | null = null;
  const metaDesc = $('meta[name="description"]').attr("content");
  if (metaDesc && metaDesc.length > 50) {
    summary = metaDesc.trim();
  }

  if (!summary) {
    $("h2, h3, h4").each((_i, heading) => {
      if (summary) return;
      const headingText = $(heading).text().trim().toLowerCase();
      if (headingText.includes("resumen") || headingText.includes("descripción")) {
        const parts: string[] = [];
        let $next = $(heading).next();
        while ($next.length > 0 && !$next.is("h1, h2, h3, h4")) {
          const text = $next.text().trim();
          if (text) parts.push(text);
          $next = $next.next();
        }
        if (parts.length > 0) summary = parts.join("\n\n");
      }
    });
  }

  // Full text
  let fullText = "";
  if (summary) fullText = summary;

  const contentSelectors = [
    ".node__content",
    ".field--name-body",
    "article .content",
    ".field--name-field-contenido",
    ".field--name-field-descripcion",
    "#block-cnmc-content",
    "main article",
    ".region-content",
    "main",
  ];

  for (const sel of contentSelectors) {
    const $content = $(sel);
    if ($content.length > 0) {
      $content.find("nav, .menu, .breadcrumb, script, style, .visually-hidden, .tabs, .pager").remove();
      const bodyText = $content.text().trim();
      if (bodyText.length > fullText.length) {
        fullText = bodyText;
      }
      break;
    }
  }

  if (!fullText || fullText.length < 100) {
    $("nav, footer, header, script, style, .menu, .breadcrumb, .visually-hidden, .tabs, .pager").remove();
    fullText = $("body").text().trim();
  }

  fullText = fullText.replace(/\s{3,}/g, "\n\n").trim();

  // Turnover — look for "cifra de negocios" or "volumen de negocio"
  let turnover: number | null = null;
  const caMatch = pageText.match(
    /(?:cifra\s+de\s+negocios?|volumen\s+de\s+negocio|facturación|facturacion)\s*(?:total|global|mundial|combinad[oa])?\s*(?:de\s+)?([\d\s.,]+)\s*(millones?|mil\s*millones?)?\s*(?:de\s+)?euros?/i,
  );
  if (caMatch) {
    let raw = caMatch[1]!.replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
    let amount = parseFloat(raw);
    if (!isNaN(amount)) {
      const unit = (caMatch[2] || "").toLowerCase();
      if (unit.startsWith("millon")) amount *= 1_000_000;
      if (/mil\s*millon/.test(unit)) amount *= 1_000_000_000;
      turnover = amount;
    }
  }

  return {
    caseNumber,
    title,
    date,
    sector,
    acquiringParty,
    target,
    summary,
    fullText,
    outcome,
    turnover,
  };
}

// ---------------------------------------------------------------------------
// Field extraction helper
// ---------------------------------------------------------------------------

/**
 * Extract the value of a metadata field from the page.
 *
 * Looks for label text in the DOM and returns the adjacent content.
 * Tries multiple strategies:
 *   1. Drupal field wrappers (.field--label + .field--item)
 *   2. Definition lists (dt + dd)
 *   3. Table rows (th + td)
 *   4. Regex on raw page text
 */
function extractFieldValue(
  $: cheerio.CheerioAPI,
  pageText: string,
  labels: string[],
): string | null {
  for (const label of labels) {
    // Strategy 1: Drupal field labels
    const $labels = $("*").filter(function () {
      const t = $(this).text().trim();
      return (
        t === label ||
        t === label + ":" ||
        t === label + " :" ||
        t.toLowerCase() === label.toLowerCase() ||
        t.toLowerCase() === label.toLowerCase() + ":" ||
        t.toLowerCase() === label.toLowerCase() + " :"
      );
    });

    for (let i = 0; i < $labels.length; i++) {
      const $label = $labels.eq(i);
      // Check sibling
      const $next = $label.next();
      if ($next.length > 0) {
        const val = $next.text().trim();
        if (val && val.length < 2000) return val;
      }
      // Check parent for .field--item / .field__item
      const $parentItems = $label.parent().find(".field--item, .field__item");
      if ($parentItems.length > 0) {
        const vals = $parentItems
          .map((_j, el) => $(el).text().trim())
          .get()
          .filter(Boolean);
        if (vals.length > 0) return vals.join(", ");
      }
    }

    // Strategy 2: Table rows — look for th containing label, return adjacent td
    $("th").each((_i, th) => {
      const thText = $(th).text().trim().toLowerCase();
      if (thText === label.toLowerCase() || thText === label.toLowerCase() + ":") {
        const td = $(th).next("td");
        if (td.length > 0) {
          const val = td.text().trim();
          if (val && val.length < 2000) return val;
        }
      }
    });

    // Strategy 3: Regex on raw text — "Label: value" or "Label\nvalue"
    const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(escaped + "\\s*:?\\s*([^\\n]{3,200})", "i");
    const match = pageText.match(re);
    if (match?.[1]) {
      return match[1].trim();
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Fine amount parsing helpers
// ---------------------------------------------------------------------------

/**
 * Parse a Spanish fine amount string.
 *
 * Handles formats like:
 *   "203,6 millones de euros"
 *   "34.500.000 euros"
 *   "128 millones euros"
 *   "12.200.000€"
 */
function parseSpanishFineAmount(text: string): number | null {
  // Try "N millones de euros"
  const millMatch = text.match(/([\d\s.,]+)\s*millones?\s*(?:de\s+)?euros?/i);
  if (millMatch) {
    let raw = millMatch[1]!.replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
    const amount = parseFloat(raw);
    if (!isNaN(amount)) return amount * 1_000_000;
  }

  // Try "NNN.NNN.NNN euros" or "NNN.NNN.NNN€"
  const euroMatch = text.match(/([\d.,]+)\s*(?:euros?|€)/i);
  if (euroMatch) {
    // Spanish number format uses . as thousands separator and , as decimal
    let raw = euroMatch[1]!.replace(/\s/g, "");
    // If it has dots and a comma (e.g. "34.500.000,00"), it's Spanish format
    if (/\.\d{3}/.test(raw)) {
      raw = raw.replace(/\./g, "").replace(",", ".");
    } else {
      raw = raw.replace(",", ".");
    }
    const amount = parseFloat(raw);
    if (!isNaN(amount)) return amount;
  }

  return null;
}

/**
 * Search the full page text for fine amount mentions.
 * Finds patterns like "multa de 203 millones de euros" or
 * "sanción de 34.500.000 euros".
 */
function extractFineFromText(text: string): number | null {
  const patterns = [
    /(?:multa|sanción|sancion|sanción pecuniaria)\s+(?:de\s+)?([\d.,]+)\s*millones?\s*(?:de\s+)?euros?/i,
    /(?:multa|sanción|sancion)\s+(?:de\s+)?([\d.,]+)\s*(?:euros?|€)/i,
    /([\d.,]+)\s*millones?\s*(?:de\s+)?euros?\s*(?:de\s+)?(?:multa|sanción|sancion)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return parseSpanishFineAmount(match[0]);
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Pagination crawler
// ---------------------------------------------------------------------------

async function crawlListingPages<T extends { caseNumber: string }>(
  queryParams: string,
  parser: (html: string) => T[],
  label: string,
): Promise<T[]> {
  const allItems: T[] = [];
  let page = 0;

  while (page < MAX_PAGES) {
    const url = `${BASE_URL}${DECISIONS_LIST_PATH}?${queryParams}&page=${page}`;
    log(`Fetching ${label} listing page ${page}: ${url}`);

    let html: string;
    try {
      html = await fetchWithRetry(url);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`  ERROR fetching listing page ${page}: ${msg}`);
      stats.errors++;
      break;
    }

    const items = parser(html);
    if (items.length === 0) {
      log(`  No items found on page ${page} — reached end of listing`);
      break;
    }

    log(`  Found ${items.length} items on page ${page}`);
    allItems.push(...items);
    page++;
  }

  log(`Total ${label} items collected from listing: ${allItems.length}`);
  return allItems;
}

// ---------------------------------------------------------------------------
// Main ingestion: decisions
// ---------------------------------------------------------------------------

async function ingestDecisions(db: Database.Database): Promise<void> {
  log("=== Ingesting competition decisions (conductas) ===");

  const existingCases = new Set<string>();
  if (FLAG_RESUME) {
    const rows = db
      .prepare("SELECT case_number FROM decisions")
      .all() as Array<{ case_number: string }>;
    for (const r of rows) existingCases.add(r.case_number);
    log(`Resume mode: ${existingCases.size} existing decisions in DB`);
  }

  const items = await crawlListingPages(
    DECISIONS_PARAMS,
    parseDecisionListing,
    "decisions",
  );

  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, '', 1, 0)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = decision_count + 1
  `);

  for (const item of items) {
    if (FLAG_RESUME && existingCases.has(item.caseNumber)) {
      stats.decisionsSkipped++;
      continue;
    }

    log(`  Scraping decision ${item.caseNumber}: ${item.detailUrl}`);
    stats.decisionsScraped++;

    let detail: DecisionDetail;
    try {
      const html = await fetchWithRetry(item.detailUrl);
      detail = parseDecisionDetail(html, item);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`    ERROR scraping detail for ${item.caseNumber}: ${msg}`);
      stats.errors++;
      continue;
    }

    if (FLAG_DRY_RUN) {
      log(`    [DRY RUN] Would insert: ${detail.caseNumber} | ${detail.title.slice(0, 60)}... | outcome=${detail.outcome} | fine=${detail.fineAmount}`);
      continue;
    }

    try {
      insertDecision.run(
        detail.caseNumber,
        detail.title,
        detail.date,
        detail.type,
        detail.sector,
        detail.parties,
        detail.summary,
        detail.fullText,
        detail.outcome,
        detail.fineAmount,
        detail.ldcArticles,
        detail.status,
      );
      stats.decisionsInserted++;

      // Upsert sector
      if (detail.sector) {
        const sectorName = item.sectors[0] || detail.sector;
        const norm = normaliseSector(sectorName);
        upsertSector.run(norm.id, norm.name, norm.name_en);
        stats.sectorsUpserted++;
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`    ERROR inserting ${detail.caseNumber}: ${msg}`);
      stats.errors++;
    }
  }
}

// ---------------------------------------------------------------------------
// Main ingestion: mergers
// ---------------------------------------------------------------------------

async function ingestMergers(db: Database.Database): Promise<void> {
  log("=== Ingesting merger control decisions (concentraciones) ===");

  const existingCases = new Set<string>();
  if (FLAG_RESUME) {
    const rows = db
      .prepare("SELECT case_number FROM mergers")
      .all() as Array<{ case_number: string }>;
    for (const r of rows) existingCases.add(r.case_number);
    log(`Resume mode: ${existingCases.size} existing mergers in DB`);
  }

  const items = await crawlListingPages(
    MERGERS_PARAMS,
    parseMergerListing,
    "mergers",
  );

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, '', 0, 1)
    ON CONFLICT(id) DO UPDATE SET
      merger_count = merger_count + 1
  `);

  for (const item of items) {
    if (FLAG_RESUME && existingCases.has(item.caseNumber)) {
      stats.mergersSkipped++;
      continue;
    }

    log(`  Scraping merger ${item.caseNumber}: ${item.detailUrl}`);
    stats.mergersScraped++;

    let detail: MergerDetail;
    try {
      const html = await fetchWithRetry(item.detailUrl);
      detail = parseMergerDetail(html, item);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`    ERROR scraping detail for ${item.caseNumber}: ${msg}`);
      stats.errors++;
      continue;
    }

    if (FLAG_DRY_RUN) {
      log(`    [DRY RUN] Would insert: ${detail.caseNumber} | ${detail.title.slice(0, 60)}... | outcome=${detail.outcome} | acquirer=${detail.acquiringParty}`);
      continue;
    }

    try {
      insertMerger.run(
        detail.caseNumber,
        detail.title,
        detail.date,
        detail.sector,
        detail.acquiringParty,
        detail.target,
        detail.summary,
        detail.fullText,
        detail.outcome,
        detail.turnover,
      );
      stats.mergersInserted++;

      if (detail.sector) {
        const sectorName = item.sectors[0] || detail.sector;
        const norm = normaliseSector(sectorName);
        upsertSector.run(norm.id, norm.name, norm.name_en);
        stats.sectorsUpserted++;
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`    ERROR inserting ${detail.caseNumber}: ${msg}`);
      stats.errors++;
    }
  }
}

// ---------------------------------------------------------------------------
// Sector count refresh
// ---------------------------------------------------------------------------

function refreshSectorCounts(db: Database.Database): void {
  if (FLAG_DRY_RUN) return;

  log("Refreshing sector counts...");

  db.exec(`
    UPDATE sectors SET
      decision_count = COALESCE((
        SELECT COUNT(*) FROM decisions WHERE decisions.sector = sectors.id
      ), 0),
      merger_count = COALESCE((
        SELECT COUNT(*) FROM mergers WHERE mergers.sector = sectors.id
      ), 0)
  `);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("CNMC (Comisión Nacional de los Mercados y la Competencia) ingestion crawler");
  log(`  DB_PATH:         ${DB_PATH}`);
  log(`  --resume:        ${FLAG_RESUME}`);
  log(`  --dry-run:       ${FLAG_DRY_RUN}`);
  log(`  --force:         ${FLAG_FORCE}`);
  log(`  --max-pages:     ${MAX_PAGES === Infinity ? "unlimited" : MAX_PAGES}`);
  log(`  --mergers-only:  ${FLAG_MERGERS_ONLY}`);
  log(`  --decisions-only: ${FLAG_DECISIONS_ONLY}`);
  log("");

  const db = FLAG_DRY_RUN ? null! : initDb();

  try {
    if (!FLAG_MERGERS_ONLY) {
      await ingestDecisions(FLAG_DRY_RUN ? null! : db);
    }

    if (!FLAG_DECISIONS_ONLY) {
      await ingestMergers(FLAG_DRY_RUN ? null! : db);
    }

    if (!FLAG_DRY_RUN) {
      refreshSectorCounts(db);
    }
  } finally {
    if (db) db.close();
  }

  // Print summary
  log("");
  log("=== Ingestion complete ===");
  log(`  Decisions scraped:  ${stats.decisionsScraped}`);
  log(`  Decisions inserted: ${stats.decisionsInserted}`);
  log(`  Decisions skipped:  ${stats.decisionsSkipped}`);
  log(`  Mergers scraped:    ${stats.mergersScraped}`);
  log(`  Mergers inserted:   ${stats.mergersInserted}`);
  log(`  Mergers skipped:    ${stats.mergersSkipped}`);
  log(`  Sectors upserted:   ${stats.sectorsUpserted}`);
  log(`  Errors:             ${stats.errors}`);

  if (stats.errors > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exitCode = 2;
});
