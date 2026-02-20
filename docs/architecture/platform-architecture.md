# Platform Architecture (Step 1)

## 1) Logical Architecture

1. **Ingestion Layer**
   - Pulls/pushes source data from APIs (FastAPI simulation first, extensible to SaaS/files/queues).
   - Applies schema contract checks and ingestion metadata.
   - Loads immutable records into PostgreSQL `raw` zone.

2. **Raw Layer (PostgreSQL)**
   - Stores source-faithful, append-only data.
   - Maintains ingestion lineage fields (`ingested_at`, `source_system`, `batch_id`).
   - Serves as operational landing store and replay source.

3. **Staging Layer (DuckDB models)**
   - Standardizes datatypes, null handling, deduping, and business keys.
   - Produces reusable conformed tables for downstream analytics.

4. **Analytics Layer (DuckDB marts)**
   - Curated business-ready datasets and aggregates.
   - Performance-oriented schemas for analyst/BI consumption.

5. **Data Quality Layer (Great Expectations)**
   - Validates freshness, uniqueness, null, range, and referential quality.
   - Emits validation artifacts and blocks promotion on critical failures.

6. **Orchestration Layer (Prefect)**
   - Coordinates end-to-end flows:
     `extract -> load_raw -> stage -> analytics -> validate -> publish status`.
   - Handles retries, alert hooks, and run metadata.

7. **Platform & Ops**
   - Config management (`.env` + typed settings), logging, error taxonomy.
   - Dockerized local runtime, AWS-ready object storage abstractions.
   - Terraform-ready `infra/` layout for later IaC rollout.

## 2) Module Responsibilities

- `src/drp/config`: environment loading, typed runtime settings, secrets interface.
- `src/drp/core`: shared logging, exceptions, constants, and cross-cutting utilities.
- `src/drp/ingestion`: source connectors, extraction services, ingestion contracts.
- `src/drp/storage`: persistence adapters (`postgres`, `duckdb`, object storage abstraction).
- `src/drp/transform`: staging and analytics transformation logic boundaries.
- `src/drp/quality`: Great Expectations suites, checkpoints, quality gates.
- `src/drp/orchestration`: Prefect flows, tasks, deployment definitions.
- `src/drp/interfaces`: API/CLI boundaries and external integration clients.
- `tests`: unit/integration/contract/data-quality tests grouped by intent.
- `infra`: Docker and Terraform skeleton for runtime + cloud provisioning.
- `docs`: architecture decisions and operational runbooks.
- `data`: non-production sample/fixture data for local development and tests.
- `scripts`: bootstrap and operational helper scripts.
- `ops`: monitoring, alerting, and platform operations assets.

## 3) Initial Repository Tree

```text
data-reliability-platform/
├── README.md
├── .env.example
├── docker-compose.yml
├── pyproject.toml
├── docs/
│   └── architecture/
│       └── platform-architecture.md
├── src/
│   └── drp/
│       ├── __init__.py
│       ├── config/
│       ├── core/
│       ├── ingestion/
│       │   ├── connectors/
│       │   └── services/
│       ├── storage/
│       │   ├── postgres/
│       │   ├── duckdb/
│       │   └── object_store/
│       ├── transform/
│       │   ├── staging/
│       │   └── analytics/
│       ├── quality/
│       │   └── great_expectations/
│       ├── orchestration/
│       │   └── prefect/
│       ├── interfaces/
│       │   ├── api/
│       │   └── cli/
│       └── utils/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── contract/
│   └── data_quality/
├── infra/
│   ├── docker/
│   └── terraform/
│       ├── envs/
│       │   └── dev/
│       └── modules/
├── data/
│   ├── sample/
│   └── fixtures/
├── scripts/
└── ops/
    └── monitoring/
```
