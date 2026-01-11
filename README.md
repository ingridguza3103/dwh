# Data Engineering — Assignment 05 (WS 2025)
H&M 2019 DWH: Star Schema + ETL Pipeline + OLAP & Analytics Queries

GitHub repository (for the professor): [<https://github.com/ingridguza3103/dwh>](https://github.com/ingridguza3103/dwh)

---

## 0) Before you start (dataset placement)
This github repo does not include the dataset file.

After downloading and unzipping this project folder or pulling this repository from github, place `dwh.zip` (from Moodle) into the root of the project directory.

Expected structure:

- `<project-root>/dwh.zip`
- `<project-root>/code/etl_load.py`
- `<project-root>/sql/`
- `<project-root>/docs/`
- `<project-root>/requirements.txt`

If `dwh.zip` is not located in the project root, the ETL must be run with a correct `--zip` path.

---

## 1) Initial setup

### 1.1 Python environment
1) Create a virtual environment in the project root (recommended).
- Windows / macOS / Linux: run `python -m venv .venv`

2) Activate the environment.
- Windows (PowerShell): run `.venv\Scripts\Activate.ps1`
- Windows (Command Prompt): run `.venv\Scripts\activate.bat`
- macOS / Linux (bash/zsh): run `source .venv/bin/activate`

3) Install dependencies.
- Run `pip install -r requirements.txt`

### 1.2 PostgreSQL setup (Docker or local)
This project works with either Docker PostgreSQL or a locally installed PostgreSQL (pgAdmin).


### Creating the PostgreSQL database `dwh` (two options)

You can run PostgreSQL either with Docker (recommended) or using a local installation with pgAdmin.  
In both cases, you must end up with a database named `dwh`.

---

#### Option A — Create `dwh` using Docker (recommended)

1) Install Docker Desktop and start it.
2) Start a PostgreSQL container that already creates the database `dwh` automatically:
   - Run `docker run --name dwh -e POSTGRES_PASSWORD=12345678 -e POSTGRES_DB=dwh -p 54946:5432 -d postgres:latest`

3) Confirm it is running and check the mapped port:
   - Run `docker ps`
   - In the `PORTS` column you should see something like `0.0.0.0:54946->5432/tcp`
   - This means you must use:
     - Host: `localhost`
     - Port: `54946`
     - Database: `dwh`
     - User: `postgres`
     - Password: `12345678`

4) If you already have a container called `dwh`, you can stop/remove it and recreate it:
   - Stop: `docker stop dwh`
   - Remove: `docker rm dwh`
   - Then run the start command again.

---

#### Option B — Create `dwh` using pgAdmin (local PostgreSQL)

1) Open pgAdmin.
2) In the left menu, expand: `Servers` → your PostgreSQL server connection.
3) Right-click `Databases` → select `Create` → `Database...`
4) Set:
   - Database name: `dwh`
   - Owner: `postgres` (or your local DB user)
5) Click `Save`.

Now you have a database named `dwh` locally.

---

Important: If you have both local Postgres and Docker Postgres, make sure you consistently use the same instance for:
- running the ETL, and
- executing the SQL queries.

If you use Docker, use the Docker “host mapped port” (the number on the left side of the mapping shown by `docker ps`, e.g., `0.0.0.0:54946->5432` means your port is `54946` , ran into some issues with this, worth pointing out).

---

## 2) Exercise 1 — Define a DWH Schema
### Deliverables
- Schema SQL: `sql/exercise1_schema.sql`
- Modeling decisions + assumptions: `docs/exercise1_schema.md`
- Schema screenshot: `docs/01_DWH_Schema_visualized.png`

### Steps
1. Create a PostgreSQL database (recommended name: `dwh`).
2. Execute the schema file `sql/exercise1_schema.sql` on that database.

This creates schema `hm_dwh` and the star schema tables:
- `dim_region`, `dim_date`, `dim_customer`, `dim_article`, `fact_sales`

For the reasoning and assumptions, see `docs/exercise1_schema.md`.

---

## 3) Exercise 2 — Create a DWH (ETL Pipeline)
### Deliverables
- ETL script: `code/etl_load.py`
- ETL documentation: `docs/exercise2_etl.md`
- ETL run screenshot: `docs/02_ETL_pipeline_sample.png`

### Steps
1. Ensure PostgreSQL is running and you can connect to the database (e.g., `dwh`).
2. Run the ETL pipeline script `code/etl_load.py`.

See `docs/exercise2_etl.md` for the steps to run the pipeline and details.

At the end of a successful run, the pipeline prints validation row counts for:
- `dim_article`, `dim_customer`, `dim_date`, `fact_sales`

---

## 4) Exercise 3 — OLAP Queries & Pivot
### Deliverables
- SQL queries: `sql/exercise3_queries.sql`
- Documentation: `docs/exercise3_dwh_querying.md`
- Result screenshots: `docs/exercise 3/` (A.png – F.png)

### Steps
1. Ensure the ETL ran successfully and the tables contain data.
2. Execute the file `sql/exercise3_queries.sql`.

This file includes:
- a pivot-style aggregation (region x product type x season)
- required queries (a)–(f)

For more details and screenshots of results, see `docs/exercise3_dwh_querying.md`.

---

## 5) Exercise 4 — Customer Segmentation & Analytics
### Deliverables
- SQL queries: `sql/exercise4_customer_segm.sql`
- Documentation: `docs/exercise4_customer_segmentation.md`
- Result screenshots: `docs/exercise 4/` (A.png – C.png)

### Steps
1. Execute `sql/exercise4_customer_segm.sql`.
2. Screenshots of the outputs are provided in `docs/exercise 4/`.

For more details and screenshots of results, see `docs/exercise4_customer_segmentation.md`.

---

## 6) Exercise 5 — Article Portfolio Analytics
### Deliverables
- SQL queries: `sql/exercise5_portfolio_analytics.sql`
- Documentation: `docs/exercise5_portfolio_analytics.md`
- Result screenshots: `docs/exercise 5/` (A.png – C.png)

### Steps
1. Execute `sql/exercise5_portfolio_analytics.sql`.
2. Screenshots of the outputs are provided in `docs/exercise 5/`.

For more details and screenshots of results, see `docs/exercise5_portfolio_analytics.md`.
