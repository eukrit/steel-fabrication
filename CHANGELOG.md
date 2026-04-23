# Changelog

All notable changes to this project will be documented in this file.

## [0.4.0] - 2026-04-23

### Added
- `scripts/load_sanko.py` — ingests the entire Sanko Fastem Thailand archive
  at `C:/Users/Eukrit/My Drive/Products GO/Fastener Products/Sanko Fastem Thailand`
  into Firestore (database `steel-sections`). Walks every file, classifies each
  (quotation / PO / invoice / pricelist / product catalog / admin), extracts text
  from PDFs, and parses line items from Sanko's quotation/PO format with a
  regex that handles the concatenated-amount quirk.
- New Sanko collections:
  - `sanko_documents` (113 docs) — metadata for every file in the archive
  - `sanko_products` (53) — derived catalog from line items + hand-coded
    product families (BA/C/CT/GA/GT/HAS/SC Type anchors, ML SDS-Plus drill
    bits, ER/VR chemical anchors, DBD/DBR benders, AH hammer, HandyPuncher, etc.)
  - `sanko_prices` (25) — pricelist rows from the 2021-03-12 Drop-in and
    Drill-bit pricelists (list + net + discount)
  - `sanko_quotations` (11) — parsed QTP*/quotation PDFs with line items
  - `sanko_purchase_orders` (3) — PO-* / PI-* documents
  - `sanko_sync_runs` — audit log
- `catalog.html` now has a **Sanko** tab showing: KPI summary, searchable
  products table with category filter + priced-only toggle, pricelist rows,
  quotation/PO picker with line-item breakdown, and a searchable document
  index sorted by date (Thai filenames intact).
- SA key path resolution is now dynamic — `ai-agents-go-*.json` glob picks
  up rotated keys automatically (old `4c81b70995db` → current `9b4219be8c01`).

### Result
- Sanko load run `FBDSHKDLdVe6Xku4ZpMx` — status `success`.

## [0.3.1] - 2026-04-21

### Fixed
- `catalog.html` rendered blank because the embedded JSON payload was HTML-escaped
  (`&quot;` etc.) inside a `<script type="application/json">` block — HTML5 does not
  decode entities inside `<script>`, so `JSON.parse` on `textContent` saw literal
  `&quot;` and failed silently. Replaced `html.escape()` with targeted escaping of
  `</`, `<!--`, `-->` only (the actual characters that can terminate a script block).
- Added `.claude/launch.json` so the catalog can be previewed via
  `python -m http.server`.

## [0.3.0] - 2026-04-21

### Added
- `scripts/generate_fasteners_catalog.py` — reads every `fastener*` collection
  from Firestore and writes a self-contained `catalog.html` (≈860 KB) at the
  project root. No network calls at runtime; all data is embedded.
- `catalog.html` — interactive catalog with 8 tabs:
  1. Catalog — 232 models, live search + type/thread/material filters, click
     any row to see the orders history for that model.
  2. Vendor Pricelists — TPC Bolt M 304 pivot (304 price points, length × size)
     and Abpon flat pricelist.
  3. Assembly Config — clamp / base / anchor bolt hardware sets.
  4. Fittings — keder / cable / fork.
  5. Misc Pricelist — ANC SUS / Hilti HY 200R / stud lines.
  6. Total Orders — required / stock / actual rollup.
  7. Purchase Orders — SO21/22/23 with per-PO line items.
  8. Reference — types, threads, Order N column metadata.

## [0.2.0] - 2026-04-21

### Added
- `scripts/load_fasteners.py` — one-shot loader that ingests the `Fasteners Schedule Ver. 2024` Google Sheet (spreadsheet id `1VjEG_KSlDcfK6DO8pm_UIrZkUkawixPwzSVvGwKv_Lk`) into the `steel-sections` Firestore database.
- New collections populated:
  - `fasteners` (232 docs) — master catalog, doc per model, with `orders` subcollection (936 line items)
  - `fastener_types` (34) — from the `Type` tab (Thai descriptions + material)
  - `fastener_threads` (10) — thread size → pitch
  - `fastener_config` (29) — clamp / base / anchor bolt hardware assembly config
  - `fastener_fittings` (4) — keder / cable / fork fittings
  - `fastener_pricelist` (8) — ANC SUS / Hilti HY 200R / stud price lines
  - `fastener_vendor_pricelists` (2) — TPC Bolt M 304 pivot (304 price points) + Abpon (34 rows)
  - `fastener_orders` (169) — Order N column metadata (project + date)
  - `fastener_total_orders` (123) — flat rollup of the `Total Order` tab
  - `fastener_purchase_orders` (9) — SO21 / SO22 / SO23 PO documents
  - `fastener_sync_runs` — audit log entry per loader run
- `SheetCache` wrapper with 429-backoff retry + per-read pacing to stay under the Sheets API 60/min quota.

### Result
- Loader run `wvl0Xqqcws2JPH79fFC2` — status `success`, wrote all collections listed above in a single pass.

## [0.1.0] - 2026-03-24

### Added
- Initial project template with CI/CD pipeline
- CLAUDE.md session protocol
- cloudbuild.yaml for GCP Cloud Build
- verify.sh post-build verification script
- Dockerfile for Cloud Run deployment
- setup.sh bootstrap script
- manifest.example.json credentials template
- PR template with AI eval score fields
- Deployment Plan and Build Plan templates
