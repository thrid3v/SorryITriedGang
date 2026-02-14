# RetailNexus — Schema-Agnostic Architecture & Hybrid Ingestion

> How we turned a single-purpose retail pipeline into a **universal, schema-driven platform** that adapts to _any_ retail vertical and ingests data from _any_ source — Kaggle batch CSVs, live Faker streams, or both at once.

---

## Table of Contents

1. [The Problem](#the-problem)
2. [High-Level Architecture](#high-level-architecture)
3. [Data Format Journey (CSV → Parquet)](#data-format-journey)
4. [Schema-Agnostic Design — How It Works](#schema-agnostic-design)
   - [Business Context Configuration](#1-business-context-configuration)
   - [Field Generator Engine](#2-field-generator-engine)
   - [Schema-Driven Data Generator](#3-schema-driven-data-generator)
5. [Hybrid Ingestion — Kaggle + Faker](#hybrid-ingestion)
   - [Batch Path: Kaggle / Any CSV](#batch-path-kaggle--any-csv)
   - [Stream Path: Faker Generator](#stream-path-faker-generator)
   - [Unified Pipeline: Both at Once](#unified-pipeline-both-at-once)
6. [Pipeline Stages in Detail](#pipeline-stages)
   - [Bronze Layer (Raw)](#bronze-layer-raw)
   - [Silver Layer (Clean)](#silver-layer-clean)
   - [Gold Layer (Star Schema)](#gold-layer-star-schema)
7. [Dynamic Schema Adaptation — The Key Trick](#dynamic-schema-adaptation)
8. [Adding a New Business Vertical](#adding-a-new-business-vertical)
9. [File Reference](#file-reference)

---

## The Problem

The original pipeline was tightly coupled to a single retail schema:

```python
# OLD generator.py — hardcoded everywhere
PRODUCTS = ["Wireless Earbuds", "Running Shoes", "Organic Milk"]
CATEGORIES = ["Electronics", "Clothing", "Grocery"]
```

This meant:
- **Bakery?** Rewrite the generator, the cleaner, the queries…
- **Clothing store?** Same story — hours of code changes.
- **Real Kaggle data?** Doesn't match our schema at all.

We needed one codebase that works for **any** retail business and **any** data source.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│                                                                 │
│   ┌──────────────┐        ┌──────────────┐                      │
│   │ Kaggle / CSV │        │    Faker     │                      │
│   │  (Batch)     │        │  (Stream)    │                      │
│   └──────┬───────┘        └──────┬───────┘                      │
│          │                       │                              │
│          ▼                       ▼                              │
│  ┌───────────────┐   ┌────────────────────┐                     │
│  │ Schema        │   │ business_contexts  │                     │
│  │ Detector      │   │ .json (schema def) │                     │
│  └───────┬───────┘   └────────┬───────────┘                     │
│          │                    │                                  │
│          ▼                    ▼                                  │
│  ┌───────────────┐   ┌────────────────────┐                     │
│  │ Column Mapper │   │ Field Generator    │                     │
│  │ (rule-based / │   │ (type → Faker)     │                     │
│  │  semantic)    │   │                    │                     │
│  └───────┬───────┘   └────────┬───────────┘                     │
│          │                    │                                  │
│          └────────┬───────────┘                                  │
│                   ▼                                              │
│          ┌────────────────┐                                      │
│          │  data/raw/     │  ← Bronze Layer (CSV)               │
│          │  *.csv files   │                                      │
│          └────────┬───────┘                                      │
│                   ▼                                              │
│          ┌────────────────┐                                      │
│          │  cleaner.py    │  ← Dedup, nulls, type casting       │
│          │  (DuckDB SQL)  │    union_by_name = true             │
│          └────────┬───────┘                                      │
│                   ▼                                              │
│          ┌────────────────┐                                      │
│          │  data/silver/  │  ← Silver Layer (Parquet)           │
│          │  *.parquet     │                                      │
│          └────────┬───────┘                                      │
│                   ▼                                              │
│          ┌────────────────┐                                      │
│          │ star_schema.py │  ← Dims + Facts                    │
│          │  scd_logic.py  │    SCD Type 2 for users             │
│          └────────┬───────┘                                      │
│                   ▼                                              │
│          ┌────────────────┐                                      │
│          │  data/gold/    │  ← Gold Layer (Parquet)             │
│          │  dim_*, fact_* │                                      │
│          └────────┬───────┘                                      │
│                   ▼                                              │
│          ┌────────────────┐                                      │
│          │  DuckDB + API  │  ← Analytics + Dashboard            │
│          │  + AI Analyst  │                                      │
│          └────────────────┘                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Format Journey

Every piece of data goes through the same format progression regardless of source:

| Layer | Format | Purpose | Directory |
|-------|--------|---------|-----------|
| **Bronze** | CSV | Raw ingestion — easy to inspect, debug, append | `data/raw/` |
| **Silver** | Parquet | Cleaned, deduped, type-cast — optimized for queries | `data/silver/` |
| **Gold** | Parquet | Star schema — dims & facts — ready for analytics | `data/gold/` |

**Why CSV in Bronze?**
- Kaggle distributes datasets as CSV.
- Faker generator outputs CSV for human readability and debugging.
- CSV is format-agnostic — any tool can produce it.

**Why Parquet everywhere else?**
- Columnar compression (10-100x smaller than CSV).
- DuckDB queries Parquet natively with zero overhead.
- Type-safe (no more "is this column a string or number?" ambiguity).

The **cleaner.py** module handles the CSV → Parquet conversion automatically.

---

## Schema-Agnostic Design

Three components work together to make the pipeline business-agnostic.

### 1. Business Context Configuration

**File:** `config/business_contexts.json`

This is the single source of truth. Each business vertical defines its own schema:

```json
{
  "active_context": "retail_general",
  "contexts": {
    "retail_general": {
      "name": "General Retail",
      "schema": {
        "products": {
          "pool_size": 30,
          "fields": {
            "product_id": { "type": "id", "prefix": "PRD", "width": 4 },
            "product_name": {
              "type": "choice",
              "options": ["Wireless Earbuds", "Running Shoes", "Organic Milk"]
            },
            "category": {
              "type": "choice",
              "options": ["Electronics", "Clothing", "Grocery"]
            },
            "price": { "type": "float", "min": 5.0, "max": 500.0, "null_rate": 0.02 }
          }
        }
      }
    },
    "bakery": {
      "name": "Bakery Operations",
      "schema": {
        "products": {
          "fields": {
            "product_id": { "type": "id", "prefix": "PRD", "width": 4 },
            "pastry_name": {
              "type": "choice",
              "options": ["Croissant", "Baguette", "Sourdough Loaf"]
            },
            "bake_time_minutes": { "type": "int", "min": 15, "max": 120 },
            "shelf_life_hours": { "type": "int", "min": 6, "max": 72 }
          }
        }
      }
    },
    "clothing": {
      "name": "Clothing Retail",
      "schema": {
        "products": {
          "fields": {
            "product_id": { "type": "id", "prefix": "PRD", "width": 4 },
            "item_name": {
              "type": "choice",
              "options": ["Cotton T-Shirt", "Denim Jeans", "Summer Dress"]
            },
            "size": { "type": "choice", "options": ["XS", "S", "M", "L", "XL"] },
            "color": { "type": "choice", "options": ["Black", "White", "Navy"] },
            "season": { "type": "choice", "options": ["Spring", "Summer", "Fall", "Winter"] }
          }
        }
      }
    }
  }
}
```

**Each context has 5 tables:** `products`, `users`, `transactions`, `inventory`, `shipments`.

**The key insight:** The product schema is completely different per vertical, but the pipeline code stays the same.

---

### 2. Field Generator Engine

**File:** `src/ingestion/field_generator.py`

The `FieldGenerator` class is a type-dispatch engine. It reads a field config dict and produces the right kind of fake data:

```python
class FieldGenerator:

    def generate(self, field_name, field_config, row_index=0):
        field_type = field_config["type"]

        if field_type == "id":       return f"{prefix}_{row_index:04d}"
        if field_type == "choice":   return random.choice(config["options"])
        if field_type == "float":    return round(random.uniform(min, max), 2)
        if field_type == "int":      return random.randint(min, max)
        if field_type == "datetime": return faker.date_time_between(...)
        if field_type == "date":     return faker.date_between(...)
        if field_type == "string":   return getattr(faker, method)()
        if field_type == "email":    return faker.email()
        if field_type == "name":     return faker.name()
        if field_type == "fk":       return random.choice(pool)
```

**Supported types:**

| Type | Config Keys | Example Output |
|------|-------------|----------------|
| `id` | `prefix`, `width` | `PRD_0001` |
| `choice` | `options` (list) | `"Croissant"` |
| `float` | `min`, `max` | `24.99` |
| `int` | `min`, `max` | `45` |
| `datetime` | `start`, `end` (relative: `-30d`, `today`) | `2026-02-10T14:30:00` |
| `date` | `start`, `end` | `2026-02-10` |
| `string` | `faker_method` | `"voluptate"` |
| `email` | — | `jane@example.com` |
| `name` | — | `"Kevin Pacheco"` |
| `fk` | `references` (e.g. `users.user_id`) | `USR_0023` |

**Null injection:** Any field can have `"null_rate": 0.05` — 5% of values will be `None`.

---

### 3. Schema-Driven Data Generator

**File:** `src/ingestion/generator.py`

The old generator had hundreds of lines of hardcoded product lists and categories. The new one is ~280 lines and works for **any** schema:

```python
def main(num_transactions=100, context_name=None):
    # 1. Load business context from JSON
    context = load_business_context()
    schema = context["schema"]

    # 2. Initialize field generator
    field_gen = FieldGenerator(fake)
    fk_pools = {}

    # 3. Generate tables in dependency order
    for table_name in ["users", "products", "transactions", "inventory", "shipments"]:
        table_config = schema[table_name]
        df = generate_table(table_name, table_config, field_gen, fk_pools)

        # 4. Save CSV to raw layer
        df.to_csv(RAW_DIR / f"{table_name}_{timestamp}.csv", index=False)

        # 5. Collect IDs for foreign key references
        for id_field in id_fields:
            fk_pools[f"{table_name}.{id_field}"] = df[id_field].unique().tolist()
```

**What changed from the old generator:**

| Aspect | Before (Hardcoded) | After (Schema-Driven) |
|--------|--------------------|-----------------------|
| Product names | `PRODUCTS = [...]` in Python | `"options": [...]` in JSON |
| Categories | `CATEGORIES = [...]` in Python | `"options": [...]` in JSON |
| Fields per table | Fixed columns | Dynamic from `"fields": {...}` |
| Adding a new vertical | Edit 5+ Python files | Edit 1 JSON file |
| Price ranges | `random.uniform(5, 500)` | `"min": 5.0, "max": 500.0` |

**Same codebase, different output:**

```bash
# Retail: product_name, category, price
python src/ingestion/generator.py --context retail_general
# → PRD_0001, Wireless Earbuds, Electronics, 470.78

# Bakery: pastry_name, category, bake_time_minutes, shelf_life_hours
python src/ingestion/generator.py --context bakery
# → PRD_0001, Pain au Chocolat, Bread, 23, 6

# Clothing: item_name, category, size, color, season, price
python src/ingestion/generator.py --context clothing
# → PRD_0001, Button-Down Shirt, Outerwear, S, Beige, Summer, 244.7
```

---

## Hybrid Ingestion

The pipeline accepts data from two paths simultaneously.

### Batch Path: Kaggle / Any CSV

**Files:**
- `src/ingestion/schema_detector.py` — auto-detects column types from CSV
- `src/ingestion/kaggle_ingestion.py` — downloads and normalizes Kaggle datasets

**Step 1: Schema Auto-Detection**

The `SchemaDetector` reads CSV headers and infers types using pattern matching:

```python
class SchemaDetector:
    type_patterns = {
        'id':       [r'.*_?id$', r'.*_?no$', r'.*_?number$'],
        'price':    [r'.*price.*', r'.*amount.*', r'.*cost.*'],
        'quantity': [r'.*quantity.*', r'.*qty.*', r'.*count.*'],
        'date':     [r'.*date.*', r'.*time.*', r'.*timestamp.*'],
        'category': [r'.*category.*', r'.*type.*', r'.*class.*'],
        'name':     [r'.*name.*', r'.*description.*', r'.*title.*'],
    }
```

Given a Kaggle e-commerce CSV:
```
InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
```

The detector outputs:
```
InvoiceNo    → id           → maps to: transaction_id
StockCode    → string       → (unmapped)
Description  → name         → maps to: product_name
Quantity     → quantity      → maps to: quantity
InvoiceDate  → date         → maps to: timestamp
UnitPrice    → price        → maps to: amount
CustomerID   → id           → maps to: user_id
Country      → string       → maps to: store_id
```

**Step 2: Column Mapping**

Two strategies (used in order):

1. **Rule-based** — Pattern matching on column names (`"InvoiceNo"` contains "No" → ID type, contains "Invoice" → transaction → `transaction_id`).
2. **Semantic** (optional) — OpenAI embeddings match column names to concepts from `metadata_map.json`. E.g., `"order_total"` matches `"revenue"` with 0.92 cosine similarity.

**Step 3: Normalization**

```python
df = pd.read_csv(kaggle_csv)
df = df.rename(columns=column_mapping)   # InvoiceNo → transaction_id
df['data_source'] = 'kaggle'             # Track origin
df.to_csv('data/raw/kaggle_ecommerce_20260214.csv')
```

The normalized CSV lands in `data/raw/` — the exact same place Faker puts its files.

---

### Stream Path: Faker Generator

```bash
python src/ingestion/generator.py --num-transactions 100
```

Reads schema from `business_contexts.json`, uses `FieldGenerator` to produce fake data, saves CSVs to `data/raw/`.

Output example:
```
users_20260214_121522.csv        ← 50 rows
products_20260214_121522.csv     ← 30 rows
transactions_20260214_121522.csv ← 309 rows
inventory_20260214_121522.csv    ← 300 rows
shipments_20260214_121522.csv    ← 50 rows
```

---

### Unified Pipeline: Both at Once

Here's the proof of hybrid ingestion. Both Kaggle and Faker data coexist in `data/raw/`:

```
data/raw/
├── transactions_20260214_121522.csv        ← Faker (batch 1)
├── transactions_20260214_122644.csv        ← Faker (batch 2)
├── kaggle_ecommerce_20260214_122738.csv    ← Kaggle
├── users_20260214_121522.csv               ← Faker
├── products_20260214_121522.csv            ← Faker
├── inventory_20260214_121522.csv           ← Faker
└── shipments_20260214_121522.csv           ← Faker
```

When the cleaner runs, DuckDB globs **all** matching CSVs:

```python
glob_path = "data/raw/transactions_*.csv"  # Matches ALL transaction files
duckdb.sql(f"""
    SELECT * FROM read_csv('{glob_path}', union_by_name=true, auto_detect=true)
""")
```

`union_by_name=true` is the magic — it merges CSVs with **different columns** by matching on column name, filling missing columns with `NULL`. This is how Kaggle data (which may have extra or missing columns) blends with Faker data seamlessly.

---

## Pipeline Stages

### Bronze Layer (Raw)

**Directory:** `data/raw/`
**Format:** CSV
**Source:** Faker generator + Kaggle ingestion

Every run appends timestamped CSVs. Nothing is overwritten.

```
transactions_20260214_121522.csv   ← Run 1
transactions_20260214_122644.csv   ← Run 2
kaggle_ecommerce_20260214.csv      ← Kaggle import
```

### Silver Layer (Clean)

**Directory:** `data/silver/`
**Format:** Parquet
**Module:** `src/transformation/cleaner.py`

The cleaner performs:
1. **Glob all raw CSVs** per table (catches both Faker and Kaggle files)
2. **Deduplicate** on primary keys (`DISTINCT ON`)
3. **Handle nulls** (`COALESCE(amount, 0)`, `COALESCE(city, 'Unknown')`)
4. **Cast types** (`transaction_id::VARCHAR`, `amount::DOUBLE`, `timestamp::TIMESTAMP`)
5. **Validate** (reject negative amounts, null PKs)
6. **Write Parquet** to `data/silver/`

```sql
-- Example: clean_transactions()
COPY (
    SELECT DISTINCT ON (transaction_id, product_id)
        transaction_id::VARCHAR,
        user_id::VARCHAR,
        product_id::VARCHAR,
        timestamp::TIMESTAMP,
        COALESCE(amount, 0)::DOUBLE AS amount,
        store_id::VARCHAR
    FROM read_csv('data/raw/transactions_*.csv', union_by_name=true, auto_detect=true)
    WHERE transaction_id IS NOT NULL
      AND COALESCE(amount, 0) > 0
    ORDER BY transaction_id, product_id, timestamp
) TO 'data/silver/transactions.parquet' (FORMAT PARQUET)
```

**Key design decision:** `union_by_name=true` in `read_csv()`. This means:
- Faker CSVs with columns `[transaction_id, user_id, product_id, timestamp, amount, store_id]`
- Kaggle CSVs with columns `[transaction_id, user_id, product_name, quantity, timestamp, amount, store_id, data_source]`
- DuckDB merges them by column name, filling missing columns with `NULL`.

### Gold Layer (Star Schema)

**Directory:** `data/gold/`
**Format:** Parquet
**Module:** `src/transformation/star_schema.py`

Builds a proper star schema:

```
                    ┌──────────────┐
                    │  dim_dates   │
                    │  date_key    │
                    │  full_date   │
                    │  year        │
                    │  quarter     │
                    │  month       │
                    │  day_of_week │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌───────┴──────────┐   ┌──────────────┐
│ dim_products │───│ fact_transactions│───│  dim_stores  │
│ product_key  │   │ transaction_id   │   │ store_key    │
│ product_id   │   │ product_key (FK) │   │ store_id     │
│ product_name │   │ store_key (FK)   │   │ store_name   │
│ category     │   │ date_key (FK)    │   └──────────────┘
│ price        │   │ amount           │
└──────────────┘   └──────────────────┘

                   ┌──────────────────┐
                   │ fact_inventory   │
                   │ product_key (FK) │
                   │ store_key (FK)   │
                   │ stock_level      │
                   │ reorder_point    │
                   └──────────────────┘

                   ┌──────────────────┐
                   │ fact_shipments   │
                   │ shipment_id      │
                   │ origin_key (FK)  │
                   │ dest_key (FK)    │
                   │ shipping_cost    │
                   └──────────────────┘

┌──────────────┐
│  dim_users   │  (SCD Type 2)
│ user_key     │
│ user_id      │
│ name, email  │
│ valid_from   │
│ valid_to     │
│ is_current   │
└──────────────┘
```

`dim_users` uses **SCD Type 2** (Slowly Changing Dimensions) — when a user's data changes, the old row is closed (`is_current=false`, `valid_to=today`) and a new row is inserted (`is_current=true`). This preserves full user history.

---

## Dynamic Schema Adaptation

The pipeline adapts to different schemas **without any code changes**. Here's exactly how:

### What Happens When You Switch from Retail → Bakery

**1. JSON Config Changes (the ONLY change you make):**
```diff
- "active_context": "retail_general"
+ "active_context": "bakery"
```

**2. Generator reads the bakery schema:**
- Products now have `pastry_name` instead of `product_name`
- Products now have `bake_time_minutes` and `shelf_life_hours`
- Categories change from `["Electronics", "Grocery"]` to `["Bread", "Pastries", "Cakes"]`

**3. Cleaner handles it automatically:**
- `union_by_name=true` merges old retail CSVs with new bakery CSVs
- Columns like `bake_time_minutes` that don't exist in old data → filled with `NULL`
- Columns like `product_name` that don't exist in bakery data → filled with `NULL`

**4. Star Schema builds from whatever Silver data exists.**

**5. Analytics/AI Analyst** uses `schema_inspector.py` to discover the actual columns at runtime — no hardcoded column lists.

**The code never changes. Only the JSON config.**

---

## Adding a New Business Vertical

Say you're deploying RetailNexus for a **coffee shop**. Here's exactly what you do:

### Step 1: Add to `config/business_contexts.json`

```json
"coffee_shop": {
  "name": "Coffee Shop",
  "gold_layer_path": "data/gold",
  "schema": {
    "products": {
      "pool_size": 25,
      "fields": {
        "product_id": { "type": "id", "prefix": "PRD", "width": 4 },
        "drink_name": {
          "type": "choice",
          "options": ["Espresso", "Latte", "Cappuccino", "Cold Brew", "Matcha Latte"]
        },
        "category": {
          "type": "choice",
          "options": ["Hot Drinks", "Cold Drinks", "Pastries", "Snacks"]
        },
        "size": {
          "type": "choice",
          "options": ["Small", "Medium", "Large"]
        },
        "price": { "type": "float", "min": 3.0, "max": 8.0 }
      }
    },
    "users": { ... },
    "transactions": { ... },
    "inventory": { ... },
    "shipments": { ... }
  }
}
```

### Step 2: Set Active Context

```json
"active_context": "coffee_shop"
```

### Step 3: Generate Data and Run Pipeline

```bash
python src/ingestion/generator.py --num-transactions 200
python src/transformation/cleaner.py
python src/transformation/star_schema.py
```

**That's it.** The dashboard, AI Analyst, and all API endpoints automatically work with the coffee shop data.

---

## File Reference

### Ingestion Layer
| File | Purpose |
|------|---------|
| `src/ingestion/generator.py` | Schema-driven Faker data generator. Reads `business_contexts.json`, uses `FieldGenerator`, outputs CSVs to `data/raw/`. |
| `src/ingestion/field_generator.py` | Type-dispatch engine. Maps field configs (`{type, options, min, max, ...}`) to Faker calls. |
| `src/ingestion/kaggle_ingestion.py` | Downloads Kaggle datasets, auto-detects schema, maps columns, normalizes to `data/raw/`. |
| `src/ingestion/schema_detector.py` | Reads CSV headers + sample rows, infers column types via regex patterns and data analysis. |

### Configuration
| File | Purpose |
|------|---------|
| `config/business_contexts.json` | Defines schema per business vertical (retail, bakery, clothing). Single source of truth. |
| `config/metadata_map.json` | Maps universal retail concepts (revenue, product, customer) to possible column names. |

### Transformation Layer
| File | Purpose |
|------|---------|
| `src/transformation/cleaner.py` | Bronze → Silver. Globs all raw CSVs, deduplicates, handles nulls, casts types, writes Parquet. Uses `union_by_name=true`. |
| `src/transformation/star_schema.py` | Silver → Gold. Builds dim_products, dim_stores, dim_dates, fact_transactions, fact_inventory, fact_shipments. |
| `src/transformation/scd_logic.py` | Builds dim_users with SCD Type 2. Tracks user changes over time with `valid_from`/`valid_to`. |

### Analytics Layer
| File | Purpose |
|------|---------|
| `src/analytics/schema_inspector.py` | Runtime introspection — discovers actual tables/columns in Gold layer via DuckDB. No hardcoding. |
| `src/analytics/nl_query.py` | AI Analyst — sends schema + sample rows to GPT, generates SQL, executes on DuckDB. |
| `src/analytics/semantic_matcher.py` | Embedding-based column matching. "earnings" → "amount" with 0.95 confidence. |
| `src/analytics/kpi_queries.py` | Pre-built KPI queries for the dashboard (revenue, CLV, top products, etc.). |

### API Layer
| File | Purpose |
|------|---------|
| `api/main.py` | FastAPI backend. All analytics endpoints. Context switching endpoints. |
| `api/context_manager.py` | Loads/saves business context configurations. |
| `api/auth.py` | JWT authentication for API endpoints. |

---

## Quick Commands

```bash
# Generate data for active context
python src/ingestion/generator.py --num-transactions 100

# Generate data for specific context
python src/ingestion/generator.py --num-transactions 100 --context bakery

# Ingest a local CSV file
python src/ingestion/kaggle_ingestion.py --csv-file path/to/dataset.csv

# Run the full pipeline
python src/transformation/cleaner.py
python src/transformation/scd_logic.py
python src/transformation/star_schema.py

# Start the app
python -m uvicorn api.main:app --reload --port 8000
cd frontend && npm run dev
```
