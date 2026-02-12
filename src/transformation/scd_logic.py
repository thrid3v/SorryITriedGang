"""
RetailNexus — SCD Type 2 for User Dimension
Tracks changes to user attributes (city) over time.
Compares Silver users against existing Gold dim_users and manages
effective_date / end_date / is_current flags.
All joins via duckdb.sql().
"""
import os
from datetime import date

import duckdb

_BASE = os.path.join(os.path.dirname(__file__), "..", "..")
SILVER_DIR = os.path.join(_BASE, "data", "silver")
GOLD_DIR = os.path.join(_BASE, "data", "gold")

SILVER_USERS = os.path.join(SILVER_DIR, "users.parquet").replace("\\", "/")
GOLD_DIM_USERS = os.path.join(GOLD_DIR, "dim_users.parquet").replace("\\", "/")


def _ensure_gold():
    os.makedirs(GOLD_DIR, exist_ok=True)


def apply_scd_type_2():
    """
    SCD Type 2 merge for user dimension.
    - Unchanged rows → carry forward
    - Changed city   → close old record, insert new version
    - Brand-new user → insert with is_current=True
    """
    _ensure_gold()
    today = date.today().isoformat()

    # ── First run: no history exists yet ──
    if not os.path.exists(GOLD_DIM_USERS):
        duckdb.sql(f"""
            COPY (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY user_id)::INTEGER AS surrogate_key,
                    user_id,
                    name,
                    email,
                    city,
                    signup_date     AS effective_date,
                    NULL::DATE      AS end_date,
                    TRUE            AS is_current
                FROM '{SILVER_USERS}'
            ) TO '{GOLD_DIM_USERS}' (FORMAT PARQUET)
        """)
        cnt = duckdb.sql(f"SELECT COUNT(*) FROM '{GOLD_DIM_USERS}'").fetchone()[0]
        print(f"[SCD2] Initial dim_users load: {cnt} rows")
        return

    # ── Subsequent runs: detect changes ──
    max_sk = duckdb.sql(f"""
        SELECT COALESCE(MAX(surrogate_key), 0) FROM '{GOLD_DIM_USERS}'
    """).fetchone()[0]

    duckdb.sql(f"""
        CREATE OR REPLACE TEMP TABLE existing AS
            SELECT * FROM '{GOLD_DIM_USERS}';

        CREATE OR REPLACE TEMP TABLE incoming AS
            SELECT * FROM '{SILVER_USERS}';

        -- Rows that haven't changed: carry forward as-is
        CREATE OR REPLACE TEMP TABLE unchanged AS
            SELECT e.*
            FROM existing e
            JOIN incoming i ON e.user_id = i.user_id
            WHERE e.is_current = TRUE
              AND e.city = i.city;

        -- Old current rows whose city changed → close them
        CREATE OR REPLACE TEMP TABLE closed AS
            SELECT
                e.surrogate_key,
                e.user_id,
                e.name,
                e.email,
                e.city,
                e.effective_date,
                DATE '{today}'     AS end_date,
                FALSE              AS is_current
            FROM existing e
            JOIN incoming i ON e.user_id = i.user_id
            WHERE e.is_current = TRUE
              AND e.city != i.city;

        -- New versions for changed users
        CREATE OR REPLACE TEMP TABLE new_versions AS
            SELECT
                ({max_sk} + ROW_NUMBER() OVER (ORDER BY i.user_id))::INTEGER AS surrogate_key,
                i.user_id,
                i.name,
                i.email,
                i.city,
                DATE '{today}'     AS effective_date,
                NULL::DATE         AS end_date,
                TRUE               AS is_current
            FROM incoming i
            JOIN existing e ON e.user_id = i.user_id
            WHERE e.is_current = TRUE
              AND e.city != i.city;

        -- Brand-new users (no match in existing)
        CREATE OR REPLACE TEMP TABLE brand_new AS
            SELECT
                ({max_sk} + (SELECT COUNT(*) FROM new_versions)
                    + ROW_NUMBER() OVER (ORDER BY i.user_id))::INTEGER AS surrogate_key,
                i.user_id,
                i.name,
                i.email,
                i.city,
                i.signup_date      AS effective_date,
                NULL::DATE         AS end_date,
                TRUE               AS is_current
            FROM incoming i
            WHERE i.user_id NOT IN (SELECT user_id FROM existing);

        -- Historical rows that are already closed (not current) → keep
        CREATE OR REPLACE TEMP TABLE already_closed AS
            SELECT * FROM existing WHERE is_current = FALSE;
    """)

    # ── Merge all pieces and write ──
    duckdb.sql(f"""
        COPY (
            SELECT * FROM unchanged
            UNION ALL
            SELECT * FROM closed
            UNION ALL
            SELECT * FROM new_versions
            UNION ALL
            SELECT * FROM brand_new
            UNION ALL
            SELECT * FROM already_closed
            ORDER BY user_id, effective_date
        ) TO '{GOLD_DIM_USERS}' (FORMAT PARQUET)
    """)

    changed = duckdb.sql("SELECT COUNT(*) FROM closed").fetchone()[0]
    new = duckdb.sql("SELECT COUNT(*) FROM brand_new").fetchone()[0]
    total = duckdb.sql(f"SELECT COUNT(*) FROM '{GOLD_DIM_USERS}'").fetchone()[0]
    print(f"[SCD2] dim_users updated — {changed} closed, {new} new, {total} total rows")

    # cleanup temp tables
    for t in ["existing", "incoming", "unchanged", "closed", "new_versions", "brand_new", "already_closed"]:
        duckdb.sql(f"DROP TABLE IF EXISTS {t}")


if __name__ == "__main__":
    apply_scd_type_2()
