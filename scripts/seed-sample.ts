/**
 * Seed the CNMC database with sample decisions, mergers, and sectors.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["CNMC_DB_PATH"] ?? "data/cnmc.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

interface SectorRow { id: string; name: string; name_en: string; description: string; decision_count: number; merger_count: number; }

const sectors: SectorRow[] = [
  { id: "telecomunicaciones", name: "Telecomunicaciones", name_en: "Telecommunications", description: "Sector de telecomunicaciones móviles y fijas, internet de banda ancha, servicios de televisión por cable y satélite. Regulado por la CNMC y la Ley General de Telecomunicaciones.", decision_count: 45, merger_count: 18 },
  { id: "energia", name: "Energía", name_en: "Energy", description: "Sector energético incluyendo electricidad, gas natural, energías renovables y combustibles. Mercados regulados por la CNMC bajo la Ley del Sector Eléctrico y la Ley del Sector de Hidrocarburos.", decision_count: 38, merger_count: 22 },
  { id: "distribucion_alimentaria", name: "Distribución Alimentaria", name_en: "Food Distribution and Retail", description: "Distribución y comercialización de productos de alimentación a través de supermercados, hipermercados e industria alimentaria. Sector con alta actividad de supervisión por conductas restrictivas.", decision_count: 29, merger_count: 15 },
  { id: "digital", name: "Economía Digital y Plataformas", name_en: "Digital Economy and Platforms", description: "Plataformas digitales, mercados en línea, servicios de publicidad en internet, y servicios digitales regulados por el DMA/DSA. Sector de creciente actividad de supervisión por parte de la CNMC.", decision_count: 22, merger_count: 8 },
  { id: "transporte", name: "Transporte y Logística", name_en: "Transport and Logistics", description: "Transporte por carretera, ferroviario, aéreo y marítimo. Infraestructura de transporte y servicios logísticos. Investigaciones por restricciones horizontales y abuso de posición dominante.", decision_count: 19, merger_count: 12 },
];

const insertSector = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

interface DecisionRow { case_number: string; title: string; date: string; type: string; sector: string; parties: string; summary: string; full_text: string; outcome: string; fine_amount: number | null; gwb_articles: string; status: string; }

const decisions: DecisionRow[] = [
  {
    case_number: "SNC/0003/21",
    title: "Mercadona — Abusos en materia de condiciones comerciales a proveedores",
    date: "2023-11-28",
    type: "abuse_of_dominance",
    sector: "distribucion_alimentaria",
    parties: "Mercadona S.A.",
    summary: "La CNMC impuso una multa de 203,6 millones de euros a Mercadona por abuso de posición de dependencia económica frente a sus proveedores. La empresa impuso condiciones comerciales desleales: descuentos sin justificación, pagos retroactivos y modificación unilateral de contratos. Infracción del artículo 16 bis LDC.",
    full_text: "SNC/0003/21 Mercadona abuso posición dominante frente a proveedores. Hechos: Mercadona impuso a sus proveedores condiciones comerciales abusivas durante el período 2018-2021: (1) descuentos especiales no previstos en contratos (hasta 10% retroactivo); (2) pagos por servicios inexistentes o sobredimensionados; (3) modificación unilateral de plazos de pago sin justificación; (4) exigencia de exclusividad de facto bajo amenaza de exclusión. Posición de dependencia económica: proveedores con >30% de sus ventas con Mercadona carecían de alternativa comercial equivalente. Artículo LDC: Art. 16 bis — abuso de posición de dependencia económica; Art. 1 — conductas colusorias (descuentos uniformes acordados con proveedores vinculados). Sanción: 203,6 millones de euros; condición correctora — prohibición de repetir prácticas; publicación de la resolución. Mercadona interpuso recurso ante la Audiencia Nacional.",
    outcome: "fine",
    fine_amount: 203600000,
    gwb_articles: "Art. 16 bis LDC, Art. 1 LDC",
    status: "final",
  },
  {
    case_number: "S/0045/10",
    title: "Cártel de fabricantes de sobres — Acuerdos de precios y reparto de clientes",
    date: "2013-06-25",
    type: "cartel",
    sector: "distribucion_alimentaria",
    parties: "Logista S.A.; Adveo Group International S.A.; GPV España S.A.; Enrique Giró Espax S.A.",
    summary: "La CNC (predecesora de la CNMC) impuso multas por un total de 34,5 millones de euros a cuatro fabricantes de sobres por participar en un cártel de fijación de precios, reparto de clientes y coordinación de ofertas a licitaciones públicas durante más de 10 años.",
    full_text: "S/0045/10 Cártel fabricantes de sobres. Periodo de infracción: 2000-2012 (más de 10 años). Conductas: (1) fijación colectiva de precios y condiciones de venta de sobres estandarizados; (2) asignación de clientes entre competidores (reparto del mercado); (3) coordinación de ofertas en licitaciones públicas (bid rigging). Pruebas: correos electrónicos y actas de reuniones periódicas; inspecciones en sedes; delación de uno de los participantes (Adveo) que aportó pruebas a cambio de reducción. Artículos LDC: Art. 1(1)(a) — fijación de precios; Art. 1(1)(c) — reparto de mercado; Art. 1(1)(d) — conductas colusorias en licitaciones. Sanciones: Logista 12,2M€; Adveo 8,9M€ (reducción 30% por colaboración); GPV 8,4M€; Giró 5,0M€. Jurisprudencia: sentencia confirmatoria del Tribunal Supremo 2016.",
    outcome: "fine",
    fine_amount: 34500000,
    gwb_articles: "Art. 1(1)(a)(c)(d) LDC",
    status: "final",
  },
  {
    case_number: "SDCR/0027/22",
    title: "Google — Posición dominante en mercado de búsqueda y publicidad en línea",
    date: "2024-01-15",
    type: "abuse_of_dominance",
    sector: "digital",
    parties: "Google LLC; Alphabet Inc.",
    summary: "La CNMC abrió investigación formal contra Google por posible abuso de posición dominante en los mercados de búsqueda en línea y publicidad search en España. La investigación evalúa si Google favorece sus propios servicios de comparación de precios sobre competidores en sus resultados de búsqueda.",
    full_text: "SDCR/0027/22 Google investigación posición dominante búsqueda. Mercados afectados: búsqueda general en línea (Google con >93% cuota en España); publicidad de búsqueda (search advertising). Conductas investigadas: (1) self-preferencing: Google posiciona sus servicios (Google Shopping, Google Flights, Google Hotels) por encima de competidores en resultados orgánicos; (2) discriminación: competidores de comparación de precios (Rastreator, Kelkoo) degradados algorítmicamente; (3) publicidad: condiciones de acceso a Google Ads presuntamente discriminatorias. Marco normativo: Art. 2 LDC (abuso posición dominante); Art. 102 TFUE; Reglamento DMA (aplicable a gatekeepers desde marzo 2024). La CNMC coordina con Comisión Europea y otras autoridades NCA en el marco del ECN. Estado: investigación en curso; pliego de cargos previsto 2024.",
    outcome: "cleared",
    fine_amount: null,
    gwb_articles: "Art. 2 LDC, Art. 102 TFUE",
    status: "pending",
  },
  {
    case_number: "S/0120/18",
    title: "Cártel en el mercado de automoción — Recambios originales y talleres",
    date: "2021-03-10",
    type: "cartel",
    sector: "transporte",
    parties: "Asociación Nacional de Importadores de Automóviles (ANIACAM); Fabricantes de automóviles (múltiples)",
    summary: "La CNMC sancionó con 128 millones de euros a varias asociaciones de importadores de automóviles y distribuidores por acuerdos que restringían el acceso a recambios originales y datos de reparación a talleres independientes, en infracción del Reglamento de Exención por Categorías del Sector del Automóvil.",
    full_text: "S/0120/18 Cártel sector automoción recambios. Conductas: (1) restricción de suministro de recambios originales a talleres independientes no pertenecientes a la red oficial; (2) denegación de acceso a datos técnicos de reparación (OBD, manuales); (3) acuerdos horizontales entre importadores para mantener sistema de distribución cerrado. Perjuicio: consumidores sin acceso a talleres independientes (generalmente más económicos); eliminación de competencia en mercado secundario de reparación y mantenimiento. Marco normativo: Art. 1 LDC; Reglamento (UE) 461/2010 (Reglamento BER Automóvil); Art. 101 TFUE. Sanción total: 128 millones euros. Reincidencia agravante para varios participantes. Medidas correctoras: acceso a datos técnicos de reparación; suministro de recambios a talleres independientes sin restricciones injustificadas.",
    outcome: "fine",
    fine_amount: 128000000,
    gwb_articles: "Art. 1 LDC, Art. 101 TFUE",
    status: "final",
  },
  {
    case_number: "IPN/CNMC/102/22",
    title: "Investigación sector gasístico — Distribución gas natural a industria",
    date: "2023-05-22",
    type: "sector_inquiry",
    sector: "energia",
    parties: "Sector de distribución de gas natural en España",
    summary: "La CNMC publicó el informe de investigación sectorial sobre el mercado de distribución de gas natural a clientes industriales. Identifica barreras a la competencia: alta concentración en distribución, dificultades de cambio de proveedor y opacidad en precios. Recomendaciones regulatorias al Gobierno.",
    full_text: "IPN/CNMC/102/22 Investigación sectorial gas natural industrial. Hallazgos principales: (1) Alta concentración: 3 distribuidoras (Naturgy, Endesa, Repsol) controlan >75% del mercado; (2) Barreras de cambio: costes de cambio elevados, contratos take-or-pay con penalizaciones, acceso dificultado a infraestructuras; (3) Opacidad de precios: diferencias de precio no justificadas entre clientes similares; (4) Contratación bilateral sin transparencia suficiente. Recomendaciones: publicación obligatoria de tarifas de referencia; simplificación proceso de cambio de proveedor; acceso no discriminatorio a infraestructura de distribución; mayor supervisión de contratos de suministro. Seguimiento: CNMC monitorizará implementación de recomendaciones; posibles expedientes sancionadores si persisten restricciones.",
    outcome: "cleared",
    fine_amount: null,
    gwb_articles: "Art. 5 LDC (investigación sectorial)",
    status: "final",
  },
];

const insertDecision = db.prepare(`INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
for (const d of decisions) insertDecision.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status);
console.log(`Inserted ${decisions.length} decisions`);

interface MergerRow { case_number: string; title: string; date: string; sector: string; acquiring_party: string; target: string; summary: string; full_text: string; outcome: string; turnover: number | null; }

const mergers: MergerRow[] = [
  {
    case_number: "C/0943/22",
    title: "MásMóvil / Orange España — Fusión telecomunicaciones",
    date: "2024-02-19",
    sector: "telecomunicaciones",
    acquiring_party: "MásMóvil Ibercom S.A.",
    target: "Orange España S.A.U.",
    summary: "La CNMC autorizó condicionalmente la fusión entre MásMóvil y Orange España, creando el segundo operador de telecomunicaciones en España. Condiciones: cesión de capacidad de red; acuerdos de itinerancia con operadores virtuales; compromisos de inversión en fibra óptica y 5G.",
    full_text: "C/0943/22 MásMóvil / Orange España. Operación: fusión mediante absorción de Orange España por MásMóvil; creación de entidad combinada con ~22 millones de clientes móviles y ~5 millones de clientes de banda ancha. Segunda posición en mercado español tras Telefónica. Autoridad competente: CNMC con revisión de Comisión Europea (Art. 22 Reglamento Concentraciones). Análisis competencia: mercado móvil (oligopolio 4→3); mercado banda ancha fija; mercado empresas. Condiciones impuestas: (1) cesión de 1 bloque de espectro 3,5 GHz a un nuevo entrante o OMV; (2) oferta de acceso al por mayor a OMV durante 10 años a tarifas reguladas; (3) acuerdo de itinerancia con OMV durante 7 años; (4) compromisos de inversión: EUR 1.500M en fibra y 5G (2024-2028); (5) mantenimiento de tarifas para clientes de ambas marcas durante 3 años. Supervisión: CNMC monitorizará cumplimiento durante 10 años.",
    outcome: "cleared_with_conditions",
    turnover: 4200000000,
  },
  {
    case_number: "C/0673/19",
    title: "Iberdrola / Neoenergia — Control concentración sector eléctrico",
    date: "2020-06-10",
    sector: "energia",
    acquiring_party: "Iberdrola S.A.",
    target: "Neoenergia S.A. (Brasil)",
    summary: "La CNMC autorizó en Fase I la adquisición del control exclusivo de Neoenergia (empresa distribuidora eléctrica brasileña) por Iberdrola. La operación no generó solapamientos competitivos en España. Declarada compatible con el mercado interior.",
    full_text: "C/0673/19 Iberdrola / Neoenergia. Operación: Iberdrola adquiere 50% adicional de Neoenergia (distribuidora eléctrica en Brasil, ya controlada al 50%). Resultado: control exclusivo. Mercados analizados: (1) producción y venta de electricidad en España — sin solapamiento (Neoenergia opera únicamente en Brasil); (2) distribución eléctrica en España — sin solapamiento. Conclusión CNMC: la operación no es susceptible de impedir o falsear la competencia en España. Autorización en Fase I sin condiciones. Umbral de notificación: ambas partes con volúmenes de negocio superiores a 240M€ en España.",
    outcome: "cleared_phase1",
    turnover: 7800000000,
  },
  {
    case_number: "C/0887/21",
    title: "El Corte Inglés / Sfera (marcas de moda) — Adquisición activos",
    date: "2021-09-14",
    sector: "distribucion_alimentaria",
    acquiring_party: "El Corte Inglés S.A.",
    target: "Sfera Moda S.L. (marcas de moda Inditex)",
    summary: "La CNMC autorizó en Fase I la adquisición por El Corte Inglés de determinadas marcas de moda de Inditex (Sfera). La operación no presenta solapamientos significativos en el mercado de distribución de moda en España. Sin condiciones.",
    full_text: "C/0887/21 El Corte Inglés / Sfera. Operación: El Corte Inglés adquiere la marca Sfera y sus activos asociados (tiendas, inventario, diseños) de Inditex Group. Inditex abandona este segmento para centrarse en sus marcas principales (Zara, Pull&Bear, Massimo Dutti, etc.). Mercados analizados: (1) distribución de moda en España — cuotas post-fusión inferiores al 15%; (2) alquiler de espacios comerciales — no relevant. Conclusión: concentración compatible con el mercado; cuotas resultantes no generan posición dominante. Autorización Fase I.",
    outcome: "cleared_phase1",
    turnover: 980000000,
  },
];

const insertMerger = db.prepare(`INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
for (const m of mergers) insertMerger.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover);
console.log(`Inserted ${mergers.length} mergers`);

const dc = (db.prepare("SELECT COUNT(*) as n FROM decisions").get() as { n: number }).n;
const mc = (db.prepare("SELECT COUNT(*) as n FROM mergers").get() as { n: number }).n;
const sc = (db.prepare("SELECT COUNT(*) as n FROM sectors").get() as { n: number }).n;
console.log(`\nDatabase summary:\n  Decisions: ${dc}\n  Mergers: ${mc}\n  Sectors: ${sc}\n\nSeed complete.`);
