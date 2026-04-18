# steel-fabrication

## Project Identity
- Project: steel-fabrication
- Owner: Eukrit / GO Corporation Co., Ltd.
- Notion Dashboard: https://www.notion.so/gocorp/Coding-Project-Dashboard-Claude-32c82cea8bb080f1bbd7f26770ae9e80
- GitHub Repo: https://github.com/eukrit/steel-fabrication
- GCP Project ID: ai-agents-go
- GCP Project Number: 538978391890
- Cloud Run Service: steel-fabrication
- Region: asia-southeast1
- Service Account: claude@ai-agents-go.iam.gserviceaccount.com
- Artifact Registry: asia-southeast1-docker.pkg.dev/ai-agents-go/steel-fabrication
- Language: python

## Related Repos
- **accounting-automation** (master) — Peak API, Xero, MCP server → `eukrit/accounting-automation`
- **business-automation** (main) — ERP gateway, shared libs, dashboard → `eukrit/business-automation`
- Credential files → use `Credentials Claude Code` folder + GCP Secret Manager

## MANDATORY: After every code change
1. `git add` + `git commit` + `git push origin main`
2. Cloud Build auto-deploys to Cloud Run — verify build succeeds
3. Update `eukrit/business-automation` dashboard (`docs/index.html`) if architecture changes
4. Update `eukrit/business-automation/CHANGELOG.md` with version entry

## Credentials & Secrets

### Centralized Credentials Folder
All API credentials are stored in:
```
C:\Users\eukri\OneDrive\Documents\Claude Code\Credentials Claude Code
```
Master instructions: `Credentials Claude Code/Instructions/API Access Master Instructions.txt`

### Credential Loading Rules
1. **Local development**: Load from `.env` file (gitignored) or `credentials/` folder
2. **CI/CD (Cloud Build)**: Load from GCP Secret Manager
3. **MCP connectors**: Auth handled by the MCP platform — no local credentials needed
4. **NEVER hardcode** credentials in source code or committed files
5. **NEVER commit** `.env`, `manifest.json`, `credentials/`, `*.key`, `*.pem`, token files

### GCP Secret Manager (CI/CD)
| Secret Name | Source File | Used By |
|---|---|---|
| GCP SA key (default) | ai-agents-go-4c81b70995db.json | Sheets API, Firestore |

### Credential File References
| File | Location | Purpose |
|---|---|---|
| `ai-agents-go-4c81b70995db.json` | `C:\Users\Eukrit\OneDrive\Documents\Claude Code\Credentials Claude Code\` | GCP service account key |

### Local Development Setup
```bash
# Copy SA key to local credentials folder (gitignored)
mkdir -p credentials
cp "C:\Users\Eukrit\OneDrive\Documents\Claude Code\Credentials Claude Code\ai-agents-go-4c81b70995db.json" credentials/
# Create .env from template
cp .env.example .env
# GOOGLE_APPLICATION_CREDENTIALS=./credentials/ai-agents-go-4c81b70995db.json is already set
```
In Cloud Run: uses Application Default Credentials (ADC) via the service account — no JSON file needed.

## Safety Rules
- NEVER commit credentials, API keys, or tokens
- NEVER auto-merge to main without test pass
- ALWAYS run verify.sh before marking build complete
- ALWAYS load credentials from .env or Secret Manager — never hardcode

## Commit Convention
- feat(scope): description
- fix(scope): description
- docs(scope): description
- chore(scope): description
- test(scope): description

## Branch Strategy
- main → production (auto-deploys to GCP)
- dev/[feature] → development (build only)

## Testing
Run `./verify.sh` for full verification suite.
Minimum pass rate: 100% on critical path, 80% overall.

## Tech Stack
- Runtime: Python 3.11
- Infrastructure: GCP Cloud Run + Cloud Build
- CI/CD: GitHub → GCP Cloud Build trigger
- Automation: n8n (gocorp.app.n8n.cloud)
- Docs: Notion

## Project-Specific Notes

### Data Sources
- **Google Sheet**: `18wczPjTPic2GPh0cG_1hwalGXQK3tMzNvU-dmA3aSxk`
  - 'CHS JIS M' (gid=1757329436) — existing price data
  - 'CHS Table' (gid=1059692193) — OD reference table
  - 'CHS JIS Claude' — output sheet (created by this service)
- **Standards**: TIS 107 (Thai), JIS G3444 (Japanese) — hardcoded from PDFs
- **Pricing**: OneStockHome (onestockhome.com) — scraped daily

### Firestore Database: `steel-sections`
- `sections` — engineering reference data (standards)
- `vendors` — vendor registry (extensible)
- `vendor_prices` — pricing per vendor per section
  - `price_history` subcollection — daily price tracking
- `scrape_runs` — audit log

### Daily Sync
- Cloud Scheduler calls POST /sync at 6 AM Bangkok time
- Scrapes onestockhome → merges with standards → writes sheet + Firestore

---

## Claude Process Standards (MANDATORY)

Full reference: `Credentials Claude Code/Instructions/Claude Process Standards.md`

1. **Always maintain a todo list** — use `TodoWrite` for any task with >1 step or that edits files; mark items done immediately.
2. **Always update a build log** — append a dated, semver entry to `BUILD_LOG.md` (or existing `CHANGELOG.md`) for every build/version: version, date (YYYY-MM-DD), summary, files changed, outcome.
3. **Plan in batches; run them as one chained autonomous pass** — group todos into batches, surface the plan once, then execute every batch back-to-back in a single run. No turn-taking between todos or batches. Run long work with `run_in_background: true`; parallelize independent tool calls. Only stop for true blockers: destructive/unauthorized actions, missing credentials, genuine ambiguity, unrecoverable external errors, or explicit user confirmation request.
4. **Always update `build-summary.html`** at the project root for every build/version (template: `Credentials Claude Code/Instructions/build-summary.template.html`). Include version, date, status badge, and links to log + commit.
5. **Always commit and push — verify repo mapping first** — run `git remote -v` and confirm the remote repo name matches the local folder name (per the Code Sync Rules in the root `CLAUDE.md`). If mismatch (e.g. still pointing at `goco-project-template`), STOP and ask the user. Never push to the wrong repo.
