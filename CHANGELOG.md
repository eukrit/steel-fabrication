# Changelog

All notable changes to this project will be documented in this file.

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
