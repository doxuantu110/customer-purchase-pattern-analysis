# Star Schema Data Modeling Layer (Power BI)

This layer transforms the cleaned retail purchase transactions, RFM/K-means customer segmentation output, and Hadoop MapReduce summaries into a pure **Star Schema** (1 Fact table + 4 Dimension tables) under `powerbi/`, without modifying any existing preprocessing, Hadoop, streaming, or dashboard files.

## Running the Modeling Script

From the repository root:

```powershell
python analysis/10_star_schema_modeling.py
```

Optional CLI flags allow overriding input/output paths:

```powershell
python analysis/10_star_schema_modeling.py `
  --transactions data/clean/transactions.csv `
  --segments output/dashboard/customer_segments.csv `
  --customerstats data/local_out/yarn_customer_r2 `
  --productstats data/local_out/full_product `
  --out powerbi
```

## Generated Outputs (`powerbi/`)

| File | Role | Grain | Key Columns |
|---|---|---|---|
| `FactPurchase.csv` | Fact | 1 row per purchase product-event | `PurchaseKey` (PK), `CustomerKey`, `ProductKey`, `DateKey`, `TimeKey`, `amount`, `session_id` |
| `DimCustomer.csv` | Dimension | 1 row per customer | `CustomerKey` (PK), `customer_id`, `recency_days`, `frequency`, `monetary`, `avg_amount`, `segment_id`, `segment` |
| `DimProduct.csv` | Dimension | 1 row per product | `ProductKey` (PK), `product_id`, `category_code`, `brand` |
| `DimDate.csv` | Dimension | 1 row per UTC calendar date | `DateKey` (PK, `YYYYMMDD`), `full_date`, `day`, `month`, `year`, `day_of_week` (`1=Mon..7=Sun`), `day_name` (`Monday`–`Sunday`) |
| `DimTime.csv` | Dimension | 1 row per UTC hour (24 rows) | `TimeKey` (PK, `0..23`), `hour`, `time_period` (`00:00-00:59`..`23:00-23:59`) |
| `modeling_validation.json` | Validation | Audit report | Row counts, uniqueness, FK integrity, revenue/customer/product reconciliation, conflict log, `PASS`/`WARNING`/`FAIL` |

## Modeling & Data Quality Policies

- **Grain preservation**: Each row in `FactPurchase.csv` corresponds 1:1 to a purchase product-event row in `data/clean/transactions.csv`. No order-level aggregation or synthetic `Quantity`/`InvoiceID` fields are introduced.
- **Timezone**: All timestamps are parsed strictly in **UTC** (matching `TimePatternMapper.java`).
- **Deterministic surrogate keys**: `CustomerKey` and `ProductKey` are assigned sequentially after sorting by `customer_id` and `product_id` ascending, so reruns on unchanged data produce identical keys.
- **Product attribute conflicts**: If a `product_id` appears with multiple `category_code` or `brand` values in `transactions.csv`, the script prefers non-`"unknown"` values when present, then selects the most frequent (mode) value, breaking ties by latest UTC timestamp and alphabetical order. All conflicts are recorded in `modeling_validation.json` and raise the validation status to at least `WARNING`.
- **Encoding**: CSV outputs use `utf-8-sig` so Power BI Desktop reads Vietnamese `DimCustomer.segment` labels properly.

## Power BI Import & Relationship Setup

1. Open **Power BI Desktop** → **Get Data → Text/CSV** and load all five CSVs from `powerbi/`:
   - `FactPurchase.csv`
   - `DimCustomer.csv`
   - `DimProduct.csv`
   - `DimDate.csv`
   - `DimTime.csv`
2. In **Power Query Editor**, verify:
   - File Origin is **65001: Unicode (UTF-8)**.
   - Decimal columns (`FactPurchase[amount]`, `DimCustomer[monetary]`, `DimCustomer[avg_amount]`) are typed with **Using Locale → Decimal Number (English - United States)** so `.` is parsed as the decimal separator regardless of OS locale.
   - `customer_id`, `product_id`, and `session_id` are kept as **Text**.
   - Key columns (`PurchaseKey`, `CustomerKey`, `ProductKey`, `DateKey`, `TimeKey`, `recency_days`, `frequency`, `segment_id`, `day`, `month`, `year`, `day_of_week`, `hour`) are **Whole Number**.
   - `DimDate[full_date]` is **Date** (mark `DimDate` as the Date Table via **Table tools → Mark as date table → `full_date`**).
3. In **Model view**, configure exactly four **One-to-Many (`1:*`)**, **Single-direction** relationships (Dimension on the `1` side, Fact on the `*` side):
   - `DimCustomer[CustomerKey]` `1:*` `FactPurchase[CustomerKey]`
   - `DimProduct[ProductKey]` `1:*` `FactPurchase[ProductKey]`
   - `DimDate[DateKey]` `1:*` `FactPurchase[DateKey]`
   - `DimTime[TimeKey]` `1:*` `FactPurchase[TimeKey]`
4. Do **not** create any relationships directly between Dimension tables.
