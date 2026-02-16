# External Dataset Capabilities

## ✅ The Pipeline Can Handle Both Faker AND External Datasets!

The RetailNexus pipeline is **fully capable** of handling external datasets from multiple sources. It's designed to be **schema-agnostic** and can process data from:

1. **Faker-generated synthetic data** (via stream generator)
2. **Uploaded CSV/Excel files** (via web UI)
3. **Kaggle datasets** (via Kaggle API or local files)
4. **Any CSV/TSV/Parquet/JSON file** (via upload endpoints)

---

## 📊 Supported File Formats

### Via Web UI Upload:
- ✅ **CSV** (`.csv`) - with multiple encoding support (UTF-8, Latin-1, CP1252, ISO-8859-1)
- ✅ **Excel** (`.xlsx`, `.xls`)
- ✅ **TSV** (`.tsv`)
- ✅ **Parquet** (`.parquet`)
- ✅ **JSON** (`.json`)

### Via Command Line:
- ✅ **CSV files** (local or from Kaggle)
- ✅ **Kaggle datasets** (via Kaggle API)

---

## 🔄 How External Data Flows Through Pipeline

```
External Dataset (CSV/Excel/etc)
         ↓
[Upload Endpoint] → Staging Directory
         ↓
[Schema Detection] → Auto-detect column types
         ↓
[Column Mapping] → Map to standard schema
         ↓
[Data Normalization] → Fill missing columns, validate
         ↓
data/raw/ (Bronze Layer) → CSV files
         ↓
[Cleaner] → Deduplicate, type-cast, handle nulls
         ↓
data/silver/ (Silver Layer) → Parquet files
         ↓
[Star Schema Builder] → Build dimensions & facts
         ↓
data/gold/ (Gold Layer) → Parquet files
         ↓
[KPIs] → All analytics update automatically!
```

---

## 🎯 Upload Methods

### Method 1: Web UI Upload (Two-Step Process)

**Step 1: Scan File**
```http
POST /api/upload/scan
Content-Type: multipart/form-data

file: <your-file.csv>
```

**Response:**
```json
{
  "status": "success",
  "filename": "data.csv",
  "headers": ["InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice", ...],
  "recommended_mapping": {
    "transaction_id": "InvoiceNo",
    "product_id": "StockCode",
    "amount": "UnitPrice",
    "timestamp": "InvoiceDate"
  },
  "detected_type": "transactions",
  "row_count": 5,
  "sample_data": [...]
}
```

**Step 2: Process with Mapping**
```http
POST /api/upload/process
Content-Type: application/json

{
  "filename": "data.csv",
  "file_type": "transactions",
  "mapping": {
    "transaction_id": "InvoiceNo",
    "product_id": "StockCode",
    "amount": "UnitPrice",
    "timestamp": "InvoiceDate",
    "user_id": "CustomerID",
    "store_id": "Country"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "File processed successfully as transactions",
  "rows": 541909,
  "columns": ["transaction_id", "product_id", "amount", ...],
  "output_file": "transactions_20260216_143022.csv",
  "pipeline": {
    "status": "success",
    "message": "Pipeline completed"
  }
}
```

### Method 2: Simple Upload (Auto-Detection)

```http
POST /api/upload
Content-Type: multipart/form-data

file: <your-file.csv>
```

The system will:
1. Auto-detect file type (transactions, users, products, etc.)
2. Auto-map columns using pattern matching
3. Run the pipeline automatically

---

## 🔍 Schema Detection & Mapping

### Automatic Schema Detection

The pipeline uses **intelligent schema detection** to understand your data:

1. **Column Type Detection:**
   - IDs: `*_id`, `*_no`, `*_number`, `*_key`
   - Prices: `*price*`, `*amount*`, `*cost*`, `*total*`
   - Quantities: `*quantity*`, `*qty*`, `*count*`
   - Dates: `*date*`, `*time*`, `*timestamp*`
   - Categories: `*category*`, `*type*`, `*class*`
   - Names: `*name*`, `*description*`, `*title*`

2. **Pattern-Based Mapping:**
   - `InvoiceNo` → `transaction_id`
   - `StockCode` → `product_id`
   - `CustomerID` → `user_id`
   - `UnitPrice` → `amount`
   - `InvoiceDate` → `timestamp`

3. **Semantic Matching (Optional):**
   - Uses embeddings to match column names to concepts
   - Example: "earnings" → "amount" (0.92 confidence)

### Example: E-commerce Dataset

**Input CSV:**
```csv
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2010-12-01 08:26:00,2.55,17850,United Kingdom
```

**Auto-Detected Mapping:**
```json
{
  "InvoiceNo": "transaction_id",
  "StockCode": "product_id",
  "Description": "product_name",
  "Quantity": "quantity",
  "InvoiceDate": "timestamp",
  "UnitPrice": "amount",
  "CustomerID": "user_id",
  "Country": "store_id"
}
```

---

## 📥 Kaggle Dataset Support

### Via Command Line:

**Search for datasets:**
```bash
python src/ingestion/kaggle_ingestion.py --search "retail sales"
```

**Download and ingest:**
```bash
python src/ingestion/kaggle_ingestion.py --ingest "username/dataset-name"
```

**Ingest local CSV:**
```bash
python src/ingestion/kaggle_ingestion.py --csv-file path/to/dataset.csv
```

### Features:
- ✅ Auto-detects schema
- ✅ Maps columns to standard schema
- ✅ Handles multiple CSV files in dataset
- ✅ Normalizes data format
- ✅ Adds source tracking (`data_source: 'kaggle'`)

---

## 🔄 Data Merging & Schema Evolution

### Key Feature: `union_by_name=true`

The pipeline uses DuckDB's `union_by_name=true` feature, which means:

✅ **Different schemas can coexist:**
- Faker data: `[transaction_id, user_id, product_id, amount, timestamp]`
- Kaggle data: `[transaction_id, user_id, product_name, quantity, timestamp, amount, country]`
- **Result:** Merged seamlessly! Missing columns filled with NULL

✅ **Schema evolution:**
- Old data has columns: `A, B, C`
- New data has columns: `A, B, D`
- **Result:** Both work together, missing columns are NULL

### Example:

**Faker CSV:**
```csv
transaction_id,user_id,product_id,amount,timestamp,store_id
TXN_001,USR_001,PRD_001,25.99,2026-02-16 10:00:00,STORE_001
```

**Kaggle CSV:**
```csv
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536365,85123A,WHITE HANGING HEART,6,2010-12-01 08:26:00,2.55,17850,UK
```

**After Merging (Silver Layer):**
- Both files processed together
- Columns matched by name
- Missing columns (like `Description` from Faker) filled with NULL
- All data available for KPIs!

---

## 🎨 Data Type Detection

The pipeline automatically detects what type of data you're uploading:

### Detection Logic:

**Transactions:**
- Has: `transaction`, `order`, `invoice`, `sale`, `amount`, `price`
- Has: `user`, `customer` columns
- Has: `product`, `item` columns

**Users:**
- Has: `user`, `customer`, `name`, `email`
- Missing: `product`, `transaction` columns

**Products:**
- Has: `product`, `item`, `sku`, `category`
- Missing: `transaction` columns

**Inventory:**
- Has: `stock`, `inventory`, `warehouse`, `reorder`, `quantity_on_hand`

**Shipments:**
- Has: `shipment`, `shipping`, `delivery`, `carrier`, `tracking`

---

## ✅ What Happens After Upload?

1. **File saved to staging** (`data/staging/`)
2. **Schema detected** (column types, patterns)
3. **Columns mapped** (to standard schema)
4. **Data normalized** (missing columns filled, types cast)
5. **Saved to Bronze** (`data/raw/` as CSV)
6. **Pipeline runs automatically:**
   - Cleaner: Bronze → Silver (Parquet)
   - SCD Logic: User history tracking
   - Star Schema: Silver → Gold (dimensions & facts)
7. **KPI cache cleared** (new data immediately available)
8. **All KPIs update** (dashboard shows new data)

---

## 🚀 Real-World Example

### Uploading an E-commerce Dataset:

1. **Upload file:** `online_retail.csv` (541,909 rows)
2. **System detects:** Transaction data
3. **Auto-maps columns:**
   - `InvoiceNo` → `transaction_id`
   - `StockCode` → `product_id`
   - `Description` → `product_name`
   - `UnitPrice` → `amount`
   - `InvoiceDate` → `timestamp`
   - `CustomerID` → `user_id`
   - `Country` → `store_id`
4. **Pipeline processes:**
   - Cleans data (removes duplicates, handles nulls)
   - Builds star schema
   - Creates dimensions (products, users, stores, dates)
   - Creates facts (transactions)
5. **KPIs update:**
   - Summary KPIs: Revenue, Orders, Users
   - Top Products: Best sellers from dataset
   - City Sales: Revenue by country
   - Market Basket: Products bought together
   - All other KPIs!

---

## 🔧 Advanced Features

### Column Mapping Customization

You can customize column mappings in the upload process:

```json
{
  "mapping": {
    "transaction_id": "InvoiceNo",
    "product_id": "StockCode",
    "product_name": "Description",
    "amount": "UnitPrice",
    "quantity": "Quantity",
    "timestamp": "InvoiceDate",
    "user_id": "CustomerID",
    "store_id": "Country"
  }
}
```

### Missing Column Handling

If required columns are missing, the system fills them:

- `user_id` missing → Default: `"U001"`
- `store_id` missing → Default: `"S001"`
- `timestamp` missing → Default: Current timestamp
- `amount` missing → Calculated from `price * quantity` if available

### Data Validation

- ✅ Checks for empty files
- ✅ Validates file types
- ✅ Handles encoding issues (tries multiple encodings)
- ✅ Validates column mappings
- ✅ Checks data types

---

## 📋 Summary

### ✅ The Pipeline CAN Handle:

1. **Faker-generated data** (synthetic, real-time stream)
2. **Uploaded CSV/Excel files** (via web UI)
3. **Kaggle datasets** (via API or local files)
4. **Any external dataset** (CSV, TSV, Parquet, JSON)
5. **Mixed data sources** (Faker + external data together)
6. **Schema evolution** (different schemas merged seamlessly)
7. **Multiple file formats** (CSV, Excel, TSV, Parquet, JSON)
8. **Auto-detection** (schema, column types, data types)
9. **Column mapping** (automatic or manual)
10. **Data normalization** (fills missing columns, validates types)

### 🎯 Key Advantages:

- **Schema-agnostic:** Works with any retail dataset structure
- **Auto-detection:** Intelligent schema and column detection
- **Flexible mapping:** Automatic or manual column mapping
- **Data merging:** Combines Faker and external data seamlessly
- **Pipeline integration:** External data flows through same pipeline as Faker data
- **KPI updates:** All analytics update automatically after upload

---

## 🧪 Testing External Data Upload

### Quick Test:

1. **Get a sample dataset:**
   ```bash
   # Download sample e-commerce data
   wget https://example.com/retail_data.csv
   ```

2. **Upload via API:**
   ```bash
   curl -X POST http://localhost:8000/api/upload \
     -H "X-User-Role: admin" \
     -F "file=@retail_data.csv"
   ```

3. **Check KPIs:**
   ```bash
   curl http://localhost:8000/api/kpis
   ```

4. **Verify data:**
   ```bash
   curl http://localhost:8000/api/health
   ```

---

**Conclusion:** The pipeline is **fully capable** of handling external datasets! It's designed to be flexible and work with data from any source, not just Faker. 🎉
