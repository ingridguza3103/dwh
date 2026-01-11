import argparse
import csv
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PGConnection


# ----------------------------
# Utilities
# ----------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL pipeline for H&M 2019 DWH (Exercise 2)")
    p.add_argument("--zip", required=True, help="Path to dwh.zip")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--db", required=True, help="Database name (e.g., dwh)")
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--schema", default="hm_dwh")
    p.add_argument("--reset", action="store_true", help="Drop and recreate target tables in schema (dangerous)")
    return p.parse_args()


def connect(args: argparse.Namespace) -> PGConnection:
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.db,
        user=args.user,
        password=args.password,
    )


def exec_sql(conn: PGConnection, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _open_with_fallback(csv_path: Path):
    """
    Some CSVs can contain odd characters. Try common encodings.
    """
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            f = open(csv_path, "r", encoding=enc, newline="")
            # quick read test
            f.read(1024)
            f.seek(0)
            return f
        except UnicodeDecodeError:
            continue
    # if all fail, re-raise by trying utf-8 again
    return open(csv_path, "r", encoding="utf-8", newline="")


def copy_csv_to_table(
    conn: PGConnection,
    table_fq: str,
    csv_path: Path,
    columns: list[str],
    has_header: bool = True,
) -> None:
    """
    Uses PostgreSQL COPY for fast loading.

    Important details:
    - NULL '' makes empty fields become SQL NULL (helps when we write temp CSV with blanks for None).
    """
    cols = ", ".join(columns)
    header_opt = "true" if has_header else "false"
    sql = (
        f"COPY {table_fq} ({cols}) FROM STDIN WITH ("
        f"FORMAT csv, HEADER {header_opt}, NULL '', QUOTE '\"', ESCAPE '\"');"
    )

    with conn.cursor() as cur:
        with _open_with_fallback(csv_path) as f:
            cur.copy_expert(sql, f)
    conn.commit()


# ----------------------------
# Transformations required by Exercise 1/2
# ----------------------------
def compute_region_id_from_hex(postal_code: str | None) -> int | None:
    """
    region_id = int(postal_code, 16) % 10
    Data quality handling:
      - if postal_code is null/empty/invalid -> return None
    """
    if postal_code is None:
        return None
    s = str(postal_code).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return int(s, 16) % 10
    except Exception:
        return None


def safe_int(x):
    """
    Converts values like 1.0, '1.0', 1 to int, and returns None for NaN/None/invalid.
    """
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        return int(float(x))
    except Exception:
        return None


def compute_age_range(age: float | int | None) -> str:
    """
    Assumption (documented):
      0–17, 18–24, 25–34, 35–44, 45–54, 55–64, 65+, UNKNOWN
    """
    if age is None:
        return "UNKNOWN"
    try:
        if pd.isna(age):
            return "UNKNOWN"
    except Exception:
        pass

    try:
        a = int(age)
    except Exception:
        return "UNKNOWN"

    if a < 0 or a > 120:
        return "UNKNOWN"
    if a <= 17:
        return "0-17"
    if a <= 24:
        return "18-24"
    if a <= 34:
        return "25-34"
    if a <= 44:
        return "35-44"
    if a <= 54:
        return "45-54"
    if a <= 64:
        return "55-64"
    return "65+"


# ----------------------------
# DDL: staging + target SQL
# ----------------------------
def ddl_create_schema_and_staging(schema: str) -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    -- Staging tables: mirror CSV structure (keep close to raw)
    CREATE TABLE IF NOT EXISTS {schema}.stg_articles (
      article_id BIGINT,
      product_code INTEGER,
      prod_name TEXT,
      product_type_no INTEGER,
      product_type_name TEXT,
      product_group_name TEXT,
      graphical_appearance_no INTEGER,
      graphical_appearance_name TEXT,
      colour_group_code INTEGER,
      colour_group_name TEXT,
      perceived_colour_value_id INTEGER,
      perceived_colour_value_name TEXT,
      perceived_colour_master_id INTEGER,
      perceived_colour_master_name TEXT,
      department_no INTEGER,
      department_name TEXT,
      index_code TEXT,
      index_name TEXT,
      index_group_no INTEGER,
      index_group_name TEXT,
      section_no INTEGER,
      section_name TEXT,
      garment_group_no INTEGER,
      garment_group_name TEXT,
      detail_desc TEXT
    );

    -- IMPORTANT: fn/active/age can be "1.0" style in CSV.
    -- Use DOUBLE PRECISION in staging so COPY never fails; cast/clean later.
    CREATE TABLE IF NOT EXISTS {schema}.stg_customers (
      customer_id TEXT,
      fn DOUBLE PRECISION,
      active DOUBLE PRECISION,
      club_member_status TEXT,
      fashion_news_frequency TEXT,
      age DOUBLE PRECISION,
      postal_code TEXT
    );

    CREATE TABLE IF NOT EXISTS {schema}.stg_transactions (
      t_dat DATE,
      customer_id TEXT,
      article_id BIGINT,
      price NUMERIC(10,2),
      sales_channel_id SMALLINT
    );

    CREATE TABLE IF NOT EXISTS {schema}.stg_weather (
      day DATE,
      weather_code INTEGER
    );
    """


def ddl_reset_target_tables(schema: str) -> str:
    return f"""
    DROP TABLE IF EXISTS {schema}.fact_sales CASCADE;
    DROP TABLE IF EXISTS {schema}.dim_article CASCADE;
    DROP TABLE IF EXISTS {schema}.dim_customer CASCADE;
    DROP TABLE IF EXISTS {schema}.dim_date CASCADE;
    DROP TABLE IF EXISTS {schema}.dim_region CASCADE;

    DROP TABLE IF EXISTS {schema}.stg_articles CASCADE;
    DROP TABLE IF EXISTS {schema}.stg_customers CASCADE;
    DROP TABLE IF EXISTS {schema}.stg_transactions CASCADE;
    DROP TABLE IF EXISTS {schema}.stg_weather CASCADE;
    """


def ddl_create_target_tables(schema: str) -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.dim_region (
      region_id SMALLINT PRIMARY KEY CHECK (region_id BETWEEN 0 AND 9),
      region_name TEXT NOT NULL
    );

    INSERT INTO {schema}.dim_region(region_id, region_name) VALUES
     (1, 'Stockholm'),
     (2, 'Södermanland / Östergötland'),
     (3, 'Jönköping'),
     (4, 'Skåne'),
     (5, 'Kronoberg / Kalmar'),
     (6, 'Värmland / Dalarna'),
     (7, 'Gävleborg / Västernorrland'),
     (8, 'Västerbotten / Norrbotten'),
     (9, 'Blekinge'),
     (0, 'Gotland')
    ON CONFLICT (region_id) DO NOTHING;

    CREATE TABLE IF NOT EXISTS {schema}.dim_date (
      date DATE PRIMARY KEY,
      weekday SMALLINT NOT NULL CHECK (weekday BETWEEN 1 AND 7),
      week_of_year SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
      month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
      season TEXT NOT NULL CHECK (season IN ('Winter','Spring','Summer','Autumn')),
      weather_code INTEGER NULL
    );

    CREATE TABLE IF NOT EXISTS {schema}.dim_customer (
      customer_id TEXT PRIMARY KEY,
      fn SMALLINT NULL,
      active SMALLINT NULL,
      club_member_status TEXT NULL,
      fashion_news_frequency TEXT NULL,
      age SMALLINT NULL CHECK (age IS NULL OR age BETWEEN 0 AND 120),
      age_range TEXT NOT NULL,
      postal_code TEXT NULL,
      region_id SMALLINT NULL REFERENCES {schema}.dim_region(region_id)
    );

    CREATE TABLE IF NOT EXISTS {schema}.dim_article (
      article_id BIGINT PRIMARY KEY,
      product_code INTEGER NULL,
      prod_name TEXT NULL,
      product_type_no INTEGER NULL,
      product_type_name TEXT NULL,
      product_group_name TEXT NULL,
      graphical_appearance_no INTEGER NULL,
      graphical_appearance_name TEXT NULL,
      colour_group_code INTEGER NULL,
      colour_group_name TEXT NULL,
      detail_desc TEXT NULL
    );

    CREATE TABLE IF NOT EXISTS {schema}.fact_sales (
      fact_id BIGSERIAL PRIMARY KEY,
      date DATE NOT NULL REFERENCES {schema}.dim_date(date),
      customer_id TEXT NOT NULL REFERENCES {schema}.dim_customer(customer_id),
      article_id BIGINT NOT NULL REFERENCES {schema}.dim_article(article_id),
      sales_channel_id SMALLINT NOT NULL,
      price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
      quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON {schema}.fact_sales(date);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON {schema}.fact_sales(customer_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_article ON {schema}.fact_sales(article_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_channel ON {schema}.fact_sales(sales_channel_id);
    """


def truncate_staging_and_targets(schema: str) -> str:
    """
    IMPORTANT: Because fact_sales has FKs to dim_*,
    Postgres requires TRUNCATE of fact and referenced dimensions
    to happen in the SAME TRUNCATE statement (or CASCADE).
    """
    return f"""
    -- Clear staging
    TRUNCATE TABLE
      {schema}.stg_articles,
      {schema}.stg_customers,
      {schema}.stg_transactions,
      {schema}.stg_weather;

    -- Clear fact + referenced dimensions in ONE statement (FK-safe)
    TRUNCATE TABLE
      {schema}.fact_sales,
      {schema}.dim_article,
      {schema}.dim_customer,
      {schema}.dim_date
    RESTART IDENTITY;
    """


# ----------------------------
# Load steps (ETL)
# ----------------------------
def extract_zip(zip_path: Path, out_dir: Path) -> dict[str, Path]:
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)

    files = {
        "transactions": out_dir / "transactions.csv",
        "customers": out_dir / "customers.csv",
        "articles": out_dir / "articles.csv",
        "weather": out_dir / "open-meteo-2019.csv",
    }
    if not files["weather"].exists():
        alt = out_dir / "open-meteo.csv"
        if alt.exists():
            files["weather"] = alt

    for k, p in files.items():
        if not p.exists():
            raise FileNotFoundError(f"Missing expected file for {k}: {p}")
    return files


def load_staging(conn: PGConnection, schema: str, paths: dict[str, Path]) -> None:
    log("Loading staging tables with COPY...")

    copy_csv_to_table(
        conn,
        f"{schema}.stg_articles",
        paths["articles"],
        columns=[
            "article_id","product_code","prod_name","product_type_no","product_type_name",
            "product_group_name","graphical_appearance_no","graphical_appearance_name",
            "colour_group_code","colour_group_name","perceived_colour_value_id",
            "perceived_colour_value_name","perceived_colour_master_id","perceived_colour_master_name",
            "department_no","department_name","index_code","index_name","index_group_no","index_group_name",
            "section_no","section_name","garment_group_no","garment_group_name","detail_desc"
        ],
        has_header=True,
    )

    copy_csv_to_table(
        conn,
        f"{schema}.stg_customers",
        paths["customers"],
        columns=["customer_id","fn","active","club_member_status","fashion_news_frequency","age","postal_code"],
        has_header=True,
    )

    copy_csv_to_table(
        conn,
        f"{schema}.stg_weather",
        paths["weather"],
        columns=["day","weather_code"],
        has_header=True,
    )

    log("Loading transactions (large file) with COPY... this can take a while.")
    copy_csv_to_table(
        conn,
        f"{schema}.stg_transactions",
        paths["transactions"],
        columns=["t_dat","customer_id","article_id","price","sales_channel_id"],
        has_header=True,
    )


def populate_dim_article(conn: PGConnection, schema: str) -> None:
    log("Populating dim_article from stg_articles...")
    sql = f"""
    INSERT INTO {schema}.dim_article (
      article_id, product_code, prod_name,
      product_type_no, product_type_name,
      product_group_name,
      graphical_appearance_no, graphical_appearance_name,
      colour_group_code, colour_group_name,
      detail_desc
    )
    SELECT
      a.article_id, a.product_code, a.prod_name,
      a.product_type_no, a.product_type_name,
      a.product_group_name,
      a.graphical_appearance_no, a.graphical_appearance_name,
      a.colour_group_code, a.colour_group_name,
      a.detail_desc
    FROM {schema}.stg_articles a
    ON CONFLICT (article_id) DO NOTHING;
    """
    exec_sql(conn, sql)


def populate_dim_customer(conn: PGConnection, schema: str) -> None:
    """
    Stream stg_customers via server-side cursor (fast), transform,
    write temp CSV, then COPY into dim_customer.
    """
    log("Populating dim_customer with transformations (region_id, age_range)...")

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.writer(tmp)
        writer.writerow([
            "customer_id","fn","active","club_member_status","fashion_news_frequency",
            "age","age_range","postal_code","region_id"
        ])

        # Server-side cursor to avoid huge memory usage
        with conn.cursor(name="cust_stream") as cur:
            cur.itersize = 200_000
            cur.execute(f"""
                SELECT customer_id, fn, active, club_member_status, fashion_news_frequency, age, postal_code
                FROM {schema}.stg_customers
            """)

            processed = 0
            for (customer_id, fn, active, club_member_status, fashion_news_frequency, age, postal_code) in cur:
                fn_i = safe_int(fn)
                active_i = safe_int(active)
                age_i = safe_int(age)

                age_range = compute_age_range(age_i)
                region_id = compute_region_id_from_hex(postal_code)

                writer.writerow([
                    customer_id, fn_i, active_i, club_member_status, fashion_news_frequency,
                    age_i, age_range, postal_code, region_id
                ])

                processed += 1
                if processed % 200_000 == 0:
                    log(f"  processed customers rows: {processed:,}")

    copy_csv_to_table(
        conn,
        f"{schema}.dim_customer",
        tmp_path,
        columns=[
            "customer_id","fn","active","club_member_status","fashion_news_frequency",
            "age","age_range","postal_code","region_id"
        ],
        has_header=True,
    )

    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass


def populate_dim_date(conn: PGConnection, schema: str) -> None:
    log("Populating dim_date (generate 2019 dates + join weather)...")

    sql = f"""
    INSERT INTO {schema}.dim_date (date, weekday, week_of_year, month, season, weather_code)
    SELECT
      d::date AS date,
      EXTRACT(ISODOW FROM d)::smallint AS weekday,
      EXTRACT(WEEK FROM d)::smallint AS week_of_year,
      EXTRACT(MONTH FROM d)::smallint AS month,
      CASE
        WHEN EXTRACT(MONTH FROM d) IN (12,1,2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM d) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6,7,8) THEN 'Summer'
        ELSE 'Autumn'
      END AS season,
      w.weather_code
    FROM generate_series('2019-01-01'::date, '2019-12-31'::date, interval '1 day') AS d
    LEFT JOIN {schema}.stg_weather w
      ON w.day = d::date;
    """
    exec_sql(conn, sql)


def populate_fact_sales(conn: PGConnection, schema: str) -> None:
    log("Populating fact_sales from stg_transactions... (this can take time)")

    # Use joins to dimensions to avoid FK insert errors if any ids are missing
    sql = f"""
    INSERT INTO {schema}.fact_sales (date, customer_id, article_id, sales_channel_id, price, quantity)
    SELECT
      t.t_dat,
      t.customer_id,
      t.article_id,
      t.sales_channel_id,
      t.price,
      1
    FROM {schema}.stg_transactions t
    JOIN {schema}.dim_date d
      ON d.date = t.t_dat
    JOIN {schema}.dim_customer c
      ON c.customer_id = t.customer_id
    JOIN {schema}.dim_article a
      ON a.article_id = t.article_id;
    """
    exec_sql(conn, sql)


def validate_counts(conn: PGConnection, schema: str) -> None:
    log("Validation checks...")

    checks = [
        (f"SELECT COUNT(*) FROM {schema}.dim_article;", "dim_article rows"),
        (f"SELECT COUNT(*) FROM {schema}.dim_customer;", "dim_customer rows"),
        (f"SELECT COUNT(*) FROM {schema}.dim_date;", "dim_date rows"),
        (f"SELECT COUNT(*) FROM {schema}.fact_sales;", "fact_sales rows"),
        (f"SELECT COUNT(*) FROM {schema}.dim_customer WHERE age_range='UNKNOWN';", "customers with UNKNOWN age_range"),
        (f"SELECT COUNT(*) FROM {schema}.dim_customer WHERE region_id IS NULL;", "customers with NULL region_id"),
    ]

    with conn.cursor() as cur:
        for q, label in checks:
            cur.execute(q)
            val = cur.fetchone()[0]
            log(f"  {label}: {val:,}")


def main() -> int:
    args = parse_args()
    zip_path = Path(args.zip).resolve()
    if not zip_path.exists():
        log(f"ERROR: zip not found: {zip_path}")
        return 1

    log("Connecting to Postgres...")
    conn = connect(args)
    schema = args.schema

    try:
        if args.reset:
            log("Reset enabled: dropping existing tables (targets + staging)...")
            exec_sql(conn, ddl_reset_target_tables(schema))

        log("Ensuring target tables exist...")
        exec_sql(conn, ddl_create_target_tables(schema))

        log("Ensuring staging tables exist...")
        exec_sql(conn, ddl_create_schema_and_staging(schema))

        log("Truncating staging + target tables for clean load...")
        exec_sql(conn, truncate_staging_and_targets(schema))

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            log(f"Extracting zip to: {tmpdir_path}")
            paths = extract_zip(zip_path, tmpdir_path)
            load_staging(conn, schema, paths)

        populate_dim_article(conn, schema)
        populate_dim_customer(conn, schema)
        populate_dim_date(conn, schema)
        populate_fact_sales(conn, schema)

        validate_counts(conn, schema)

        log("ETL completed successfully.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
