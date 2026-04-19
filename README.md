# elt-rag-pipeline

An end-to-end data engineering pipeline built as a learning project: scraping
[books.toscrape.com](http://books.toscrape.com), staging raw data in AWS S3,
loading into Snowflake, transforming with dbt, and serving a RAG Q&A API on
Cloud Run — orchestrated by Apache Airflow and deployed via GitHub Actions.

The goal is not just to make it work, but to understand *why* each layer is
designed the way it is. Justifications are documented inline throughout.

---

## Architecture

```
books.toscrape.com
        │
        │  BeautifulSoup scraper (3 parallel Airflow tasks)
        ▼
   AWS S3 — books-pipeline-data/
   ├── raw/books/
   ├── raw/categories/
   └── raw/inventory/
        │
        │  Snowflake COPY INTO (bulk load; PURGE = TRUE removes staged files
        │  after each load — ensures schema changes don't mix old/new format rows)
        ▼
 Snowflake — BOOKS_WAREHOUSE
   RAW schema      raw_books, raw_categories, raw_inventory
   STAGING schema  stg_books, stg_categories, stg_inventory   ← dbt views
   MARTS schema    dim_books                                   ← dbt table
                   BOOK_SEARCH (Cortex Search service)         ← auto-refreshes from dim_books
        │
        │  semantic search + LLM answer (no separate vector DB or LLM service)
        ▼
   FastAPI — Cloud Run /ask endpoint
   retriever.py  → Cortex Search  (vector similarity on search_text)
   llm.py        → Cortex Complete (llama3.1-70b via SQL, no extra SDK)
        │
        ▼
   JSON response: { "answer": "...", "books": [...] }
```

### DAG task graph

```
scrape_categories ──► load_categories ──┐
scrape_books      ──► load_books      ──┼──► dbt_deps ──► dbt_run ──► dbt_test
scrape_inventory  ──► load_inventory  ──┘
```

Scrape tasks run in parallel. Each feeds its own load task independently.
All three loads must succeed before dbt begins.

---

## What dbt produces

The three raw CSVs land in Snowflake exactly as scraped — messy strings, no
types enforced, duplicates possible. dbt transforms them across two layers.

**Staging layer (views)** cleans each raw table independently:
- Price strings like `£12.99` are stripped and cast to `DECIMAL`
- Star ratings stored as English words (`Three`) are mapped to integers (`3`)
- Duplicate rows from multiple scrape runs are collapsed using `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY extracted_at DESC) = 1`

The result is three typed, deduplicated views safe to join downstream.

**Marts layer — `dim_books`** is the only marts table used by the RAG API.
It is a central dimension table: one row per book with all attributes joined.
Key SQL steps in [dbt/models/marts/dim_books.sql](dbt/models/marts/dim_books.sql):

```sql
-- Three-way join: books + categories (for category_name) + inventory (for description, stock)
FROM books b
LEFT JOIN categories c ON b.category_id = c.category_id
LEFT JOIN inventory  i ON b.book_url    = i.book_url

-- Derived stock label — business logic lives in marts, not staging
CASE
    WHEN i.num_in_stock = 0 THEN 'Out of Stock'
    WHEN i.num_in_stock < 5 THEN 'Low Stock'
    ELSE 'Available'
END AS stock_status
```

The `description` field (scraped from each book's detail page via `stg_inventory`)
flows through here into Cortex Search — it is the semantic content that makes
fuzzy matching possible. Books without a description are excluded from the
Cortex Search index (they carry no useful embedding signal).

> The two other marts tables (`mart_category_summary`, `mart_price_bands`) exist
> in the dbt project for analytical use but are not consumed by the API.

### dbt model graph

```
sources (RAW schema)
  raw_books ──────────────┐
  raw_categories ─────────┤
  raw_inventory ──────────┘
          │
          ▼  staging — views, deduplication and type casting
  stg_books ──────────────┐
  stg_categories ─────────┤
  stg_inventory ──────────┘
          │
          ▼  marts — tables, joins and derived columns
  dim_books   (central join: books + categories + inventory)
          │
          ▼  Cortex Search service (auto-indexed, TARGET_LAG = 1 hour)
  BOOK_SEARCH
```

### dbt tests

| Layer | Test type | Purpose |
|---|---|---|
| Staging | `not_null` | Every column that feeds a join must be populated |
| Staging | `unique` | Primary keys enforced before reaching marts |
| Staging | `accepted_values` | `rating_word` can only be One/Two/Three/Four/Five |
| Staging | `expression_is_true` | `price_gbp >= 0`, `num_in_stock >= 0` |
| Marts | `relationships` | Every `category_id` in `dim_books` exists in `stg_categories` |
| Marts | `accepted_values` | `stock_status` is always one of three expected values |

---

## RAG API

The API layer answers natural-language questions about the book catalogue. The
design question was: given that `dim_books` is a deterministic, structured table
(fixed columns, known types), how do you support both precise queries ("books
under £10") and fuzzy ones ("a cosy mystery with a female detective")?

**The approach: two-mode retrieval via a single Cortex Search service.**

Cortex Search manages vector embeddings inside Snowflake — no separate vector
database (Pinecone, Weaviate, etc.) is needed. The service indexes a computed
`search_text` column:

```sql
book_title || ' [' || COALESCE(category_name, '') || ']: ' || COALESCE(description, '')
AS search_text
```

Embedding the full `"Title [Category]: Description"` string rather than
description alone means a query like "Sherlock Holmes detective mystery" matches
on both the category label and the title, not just the blurb text.

The `description` column was not in the original ELT-only project. It comes
from `stg_inventory` (scraped from each book's detail page) and is the key
addition that makes semantic search useful. Without it, the embedding would only
capture title + category — too sparse for reliable fuzzy matching.

Cortex Search `ATTRIBUTES` (price, rating, stock status) are structured columns
available for exact-match filtering in API calls. They are not embedded — they
are returned as metadata alongside results. This is how the "hard SQL-style"
extraction works: Cortex Search handles the semantic ranking, and the caller
can post-filter by price or rating.

**Calling the LLM (`llm.py`):**
`SNOWFLAKE.CORTEX.COMPLETE` is called via raw `session.sql()` rather than
through `snowflake-ml-python`. The ML package is ~500 MB and adds a heavy
dependency for a single function call that is already accessible as SQL.
The prompt is built from the top-5 retrieved books and the original question.
The model (`llama3.1-70b`) is grounded — it is instructed to answer only from
the provided book list and not to invent titles.

**Why Cloud Run:**
The API is stateless (Snowflake session is opened once at startup, held for the
container lifetime). Cloud Run scales to zero between requests and needs no
instance management. The `--memory 2Gi` flag is required because
`snowflake-snowpark-python` allocates ~1.5 GB at startup.

### Example request

```bash
curl -X POST https://books-rag-api-110245679001.australia-southeast1.run.app/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "a mystery novel under 10 pounds"}'
```

```json
{
  "answer": "\"Tastes Like Fear (DI Marnie Rome #3)\" (£10.69)",
  "books": [
    {"book_title": "The Cuckoo's Calling (Cormoran Strike #1)", "category_name": "Mystery", "price_gbp": 19.21, "rating_stars": 1, "description": "..."},
    {"book_title": "The Murder of Roger Ackroyd (Hercule Poirot #4)", "category_name": "Mystery", "price_gbp": 44.1, "rating_stars": 4, "description": "..."},
    {"book_title": "Tastes Like Fear (DI Marnie Rome #3)", "category_name": "Mystery", "price_gbp": 10.69, "rating_stars": 1, "description": "..."}
  ]
}
```

Cortex Search returns the top 5 books (limit set in `retriever.py`). The LLM
reads all five and picks the one that actually satisfies the price constraint —
demonstrating the combination of fuzzy semantic retrieval and structured reasoning.

---

## Design decisions

### ELT over ETL
Raw CSV files are uploaded to S3 before any transformation occurs. This preserves
an immutable audit trail: if transformation logic changes in dbt, historical data
can be re-transformed from the original files without re-scraping the website.

### Idempotency — TRUNCATE → COPY INTO → PURGE
Every load task: truncates the raw table, bulk-loads via `COPY INTO`, then purges
the staged files from S3. If Airflow retries a failed task, the result is identical
to a first run — no duplicate rows accumulate. The `PURGE = TRUE` flag also ensures
that if the CSV schema changes (columns added or renamed), old-format files are
gone before the next load, preventing schema validation errors from mixed formats.

### Deduplication in staging, not in loading
The raw tables are append-only and accumulate every scrape run. Deduplication is
handled in the staging layer with `ROW_NUMBER()` — keeping the most recent scrape
of each record. The raw layer is a complete historical record; staging always
presents a clean current view.

### Snowflake COPY INTO over pandas `df.to_sql()`
`COPY INTO` reads S3 files directly into Snowflake without the data passing
through the Python process. Faster at scale, removes the memory constraint of
loading a full dataset into a DataFrame.

### Separation of orchestration and transformation
Airflow handles scheduling, retries, and task dependencies. dbt handles all SQL
transformation logic. Neither does the other's job. This means dbt models can be
tested, versioned, and reviewed independently of pipeline scheduling.

### Why no pytest suite for the API
The meaningful logic either lives in Snowflake (dbt transformations) or depends
on live Snowflake connections (Cortex Search, Cortex Complete) that cannot be
meaningfully mocked. Writing a pytest suite that mocks Snowflake would be testing
the mock, not the pipeline. CI validates Docker build correctness and import
resolution instead — the failure modes it catches (missing packages, broken import
paths, syntax errors) are the ones that actually occur.

### Cortex Search vs. a standalone vector DB
Using Cortex Search keeps the entire stack inside Snowflake: the data is already
there, the embeddings are managed by Snowflake, and the API calls the same
Snowflake session used for everything else. No Pinecone account, no separate
embedding service, no synchronisation job. The trade-off is Snowflake vendor
lock-in, which is acceptable for a project of this scale.

---

## CI/CD

### Workflow overview

| Files changed | Push to `dev` | PR open/update → `main` | PR merged → `main` |
|---|---|---|---|
| `api/**` | api-ci | api-ci | api-ci + api-cd |
| `dbt/**` | — | dbt-ci | dbt-cd |
| `.github/workflows/api-ci.yml` | api-ci | api-ci | api-ci only |
| Other workflow files, `README.md`, `.gitignore` | — | — | — |

### What each job does

**api-ci** — Docker build validation (no credentials required):
```bash
docker build -t books-rag-api:ci ./api
docker run --rm books-rag-api:ci python -c "import main; import retriever; import llm; print('All imports OK')"
```
Catches: missing packages in `requirements.txt`, broken import paths, syntax errors.
The trial image is discarded after CI; it is never pushed to Artifact Registry.

**api-cd** — triggered only on merge to `main` (push event):
`gcloud auth` → `docker build` tagged with commit SHA → `docker push` to Artifact
Registry → `gcloud run deploy`. Tags with SHA so every production revision is
traceable. Cloud Run performs a rolling update — no downtime.

**dbt-ci** — triggered on PR open/update (not on merge). Connects to Snowflake
and builds only the models changed in the PR (slim CI via `state:modified+` and
a downloaded production `manifest.json`). Writes to isolated `_CI` schemas, never
touches production. Gated on PR rather than every push to avoid burning Snowflake
credits on intermediate commits.

**dbt-cd** — triggered on merge to `main`. Full `dbt build --target prod` against
the production schemas. Uploads `manifest.json` as a GitHub Actions artifact so
the next dbt-ci run can compare against it for slim CI.

### Development flow

```
Make changes on dev branch
  └─► push to dev     → api-ci fires if api/** changed (import validation)

Open PR dev → main
  └─► PR created/updated → api-ci fires if api/** changed
                         → dbt-ci fires if dbt/** changed (Snowflake build in CI schema)

Merge PR → main
  └─► push to main    → api-ci + api-cd fire if api/** changed
                      → dbt-cd fires if dbt/** changed (production deploy)
```

> `README.md`, `.gitignore`, and all workflow files except `api-ci.yml` do not
> trigger any CI/CD job.

---

## Project structure

```
elt-rag-pipeline/
├── .github/
│   └── workflows/
│       ├── api-ci.yml          # Docker build + import validation
│       ├── api-cd.yml          # Build, push to AR, deploy to Cloud Run
│       ├── dbt-ci.yml          # Slim CI build on PR (Snowflake CI schema)
│       └── dbt-cd.yml          # Full production dbt build on merge
├── airflow/
│   ├── dags/
│   │   └── books_pipeline.py   # DAG: scrape → S3 → COPY INTO → dbt
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── requirements.txt
├── api/
│   ├── main.py                 # FastAPI app, Snowflake session lifespan
│   ├── retriever.py            # Cortex Search via snowflake-core Root()
│   ├── llm.py                  # Cortex Complete via session.sql()
│   ├── Dockerfile
│   └── requirements.txt
├── dbt/
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_books.sql + stg_books.yml
│   │   │   ├── stg_categories.sql + stg_categories.yml
│   │   │   └── stg_inventory.sql + stg_inventory.yml
│   │   └── marts/
│   │       ├── dim_books.sql + dim_books.yml
│   │       ├── mart_category_summary.sql
│   │       └── mart_price_bands.sql
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── profiles.yml
├── snowflake/
│   └── cortex_setup.sql        # One-time: create BOOK_SEARCH service + grants
├── snowflake_setup.sql         # One-time: warehouse, DB, schemas, roles, stage
├── .gitignore
└── README.md
```

---

## Local setup

### Prerequisites
- Docker Desktop
- AWS account — S3 bucket `books-pipeline-data`
- Snowflake account

### 1. Clone and configure

```bash
git clone https://github.com/DaleMYChen/elt-rag-pipeline.git
cd elt-rag-pipeline
cp .env.example airflow/.env
# Fill in all credential values in airflow/.env
```

### 2. Bootstrap Snowflake

Open `snowflake_setup.sql` in a Snowflake worksheet as ACCOUNTADMIN. Run each
block in order to create the warehouse, database, schemas, roles, and S3 stage.

### 3. Start Airflow and run the pipeline

```bash
cd airflow
mkdir -p logs plugins
docker compose up airflow-init
docker compose up -d
# Airflow UI: http://localhost:8081  (user: airflow / password: airflow)
```

Enable and manually trigger the `books_pipeline` DAG in the Airflow UI. This
scrapes the site, loads to S3, bulk-loads into Snowflake RAW, then runs dbt
staging and marts — populating `dim_books`.

### 4. Set up Cortex Search (one-time, run after `dim_books` is populated)

Open `snowflake/cortex_setup.sql` in the Snowflake console and run steps 1–3:

- **Step 1** — creates the `BOOK_SEARCH` Cortex Search service on `dim_books`,
  embedding the `search_text` column (`title + [category] + description`).
  `TARGET_LAG = '1 hour'` means the index auto-refreshes within one hour of any
  dbt update to `dim_books` — no manual re-indexing step.
- **Step 2** — grants `SNOWFLAKE.CORTEX_USER` (enables all Cortex functions for
  programmatic clients) and `USAGE ON CORTEX SEARCH SERVICE` (object-level
  access). Both are required; the console bypasses the first for admin users,
  which is why console queries work without it but the Python SDK doesn't.
- **Step 3** — `SHOW CORTEX SEARCH SERVICES` to verify the service was created.

> If you run `CREATE OR REPLACE CORTEX SEARCH SERVICE`, the object-level grant
> (Step 2, second grant) must be re-run because the object is replaced.

### 5. Build and run the API locally

```bash
# From the project root
docker build -t books-rag-api ./api

# Stop any existing container on port 8080 if needed
docker stop $(docker ps -q --filter "publish=8080") 2>/dev/null

# Run with Snowflake credentials from airflow/.env
docker run --rm -p 8080:8080 --env-file airflow/.env books-rag-api
```

Test in a new terminal:

```bash
curl -X POST http://localhost:8080/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "a mystery novel under 10 pounds"}'
```

### 6. Verify results in Snowflake

```sql
SELECT * FROM BOOKS_WAREHOUSE.MARTS.DIM_BOOKS LIMIT 10;
```

---

## GitHub setup

The repository was composed entirely locally, then pushed to GitHub using the
GitHub CLI.

### Create the remote repository

```bash
brew install gh
gh auth login   # follow prompts to authenticate
gh repo create elt-rag-pipeline \
  --public \
  --description "ELT pipeline (Airflow + dbt + Snowflake) with RAG API (Cortex Search + LLM)"
```

### Initialise git and push

```bash
git init
git remote add origin git@github.com:DaleMYChen/elt-rag-pipeline.git

# Review .gitignore before staging — credentials must not be committed
git add .
git commit -m "Initial commit: ELT + RAG pipeline with Cortex Search"

# Switch to HTTPS (required for gh auth setup-git credential helper)
git remote set-url origin https://github.com/DaleMYChen/elt-rag-pipeline.git
gh auth setup-git   # configures git to use gh CLI as the credential helper

git push -u origin dev
```

### Branch setup

After the initial push to `dev`, create a `main` branch on GitHub
(Settings → Branches → Add branch rule) and set it as the default branch.
All development happens on `dev`; `main` is the production-protected branch.

---

## GCP / Cloud Run setup

One-time infrastructure setup for the API deployment target.

### Enable APIs and create Artifact Registry

```bash
gcloud config set project bookselt-rag

gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  iam.googleapis.com

gcloud artifacts repositories create elt-rag-pipeline \
  --repository-format=docker \
  --location=australia-southeast1 \
  --description="Books RAG API images"
```

### Create a service account for GitHub Actions

The SA needs only three roles — no broad editor access:

```bash
gcloud iam service-accounts create github-actions-deployer \
  --display-name="GitHub Actions deployer"

# Deploy and manage Cloud Run services
gcloud projects add-iam-policy-binding bookselt-rag \
  --member="serviceAccount:github-actions-deployer@bookselt-rag.iam.gserviceaccount.com" \
  --role="roles/run.admin"

# Push images to Artifact Registry
gcloud projects add-iam-policy-binding bookselt-rag \
  --member="serviceAccount:github-actions-deployer@bookselt-rag.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

# Required for Cloud Run to act as a service account during deploy
gcloud projects add-iam-policy-binding bookselt-rag \
  --member="serviceAccount:github-actions-deployer@bookselt-rag.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
```

### Download the SA key and add to GitHub secrets

```bash
gcloud iam service-accounts keys create ~/gha-key.json \
  --iam-account=github-actions-deployer@bookselt-rag.iam.gserviceaccount.com

cat ~/gha-key.json   # copy the full JSON into GitHub → Settings → Secrets → GCP_SA_KEY
```

Also add to GitHub Secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`.

Add to GitHub Variables (Settings → Variables, not Secrets — project IDs are not
sensitive): `GCP_PROJECT_ID`.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Snowflake MFA error on load tasks | Trial account enforces MFA for all users | Run Block 4 of `snowflake_setup.sql` |
| Stage not authorised | SYSADMIN lacks explicit stage privileges | Run stage GRANT lines in `snowflake_setup.sql` Block 5 |
| Cortex Search returns 390404 | Missing `USAGE ON CORTEX SEARCH SERVICE` grant | Re-run Step 2 of `cortex_setup.sql` |
| API import error at startup | Missing package in `requirements.txt` | Caught by api-ci; fix and rebuild |
| Unique test failures after retries | COPY INTO loads all CSVs in prefix | Handled via `ROW_NUMBER()` deduplication in staging |
| `dbt_utils` macro not found | `dbt_packages/` not populated | Run `dbt deps --profiles-dir .` |
| Volume mount error on Airflow startup | `logs/` or `plugins/` missing | `mkdir -p airflow/logs airflow/plugins` |
