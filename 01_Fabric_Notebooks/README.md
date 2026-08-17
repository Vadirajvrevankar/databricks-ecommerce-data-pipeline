# Fabric E-Commerce Data Engineering Project

## Overview

A hands-on Microsoft Fabric Data Engineering project using Fabric
Notebooks, PySpark, Lakehouse, Delta tables, reusable functions, and
Parent-Child notebook orchestration.

The pipeline takes raw e-commerce order data, validates and cleans it,
creates Silver and Gold Delta tables, archives processed input, and
demonstrates production-oriented notebook practices.

## Architecture

``` text
Raw CSV
   |
   v
01_main_pipeline (Parent)
   |
   | notebook.run()
   v
02_transform_orders (Child)
   |
   +--> Read raw_orders.csv
   +--> Validate and clean
   +--> Transform
   +--> Silver Delta
   +--> Gold Delta
   +--> Archive input
   |
   | notebook.exit("SUCCESS")
   v
Parent receives status

03_common_functions
   +--> Logging
   +--> Column validation
   +--> NULL checks
   +--> Duplicate checks
   +--> Reusable business rules
```

## Notebooks

### 01_main_pipeline

Parent/orchestration notebook.

Covers: - Parameters - Logging - Parent-to-child execution - Parameter
passing - Error handling - Retry and timeout - Overall orchestration

Example:

``` python
result = notebookutils.notebook.run(
    "02_transform_orders",
    120,
    {
        "input_path": "Files/raw_orders.csv",
        "environment": "dev"
    }
)
```

### 02_transform_orders

Child processing notebook.

Covers: - Receiving parameters - Reading raw CSV - Data-quality
validation - Cleaning and rejecting bad records - Duplicate removal -
Business-rule validation - `order_amount` calculation - Silver and Gold
creation - File archiving - Returning `SUCCESS`

### 03_common_functions

Reusable utility notebook loaded with:

``` python
%run "./03_common_functions"
```

Functions include: - `log_message()` - `validate_required_columns()` -
`validate_no_nulls()` - `show_null_records()` - `get_null_records()` -
`get_duplicates()` - `validate_duplicates()` - `validOrders()` -
`checkValidPrice()` - Positive-value validation

## Data Quality

The project covers:

-   Required-column validation
-   NULL detection and handling
-   Duplicate detection and removal
-   Quantity validation (`quantity > 0`)
-   Unit-price validation (`unit_price > 0`)
-   Date validation
-   Whitespace cleaning
-   Business-rule validation

Typical flow:

``` text
Raw
 |
 v
Detect bad records
 |
 v
Reject / Clean
 |
 v
Validate cleaned data
 |
 v
Continue
```

## Transformation

The project calculates:

``` text
order_amount = quantity * unit_price
```

Example:

``` python
from pyspark.sql.functions import col

df_clean_orders = df_clean_orders.withColumn(
    "order_amount",
    col("quantity") * col("unit_price")
)
```

## Silver Layer

Cleaned order-level data is stored as:

``` text
silver_orders
```

Typical columns:

``` text
order_id
customer_id
product_id
quantity
unit_price
order_date
order_amount
```

Write pattern:

``` python
df_silver.write     .format("delta")     .mode("overwrite")     .saveAsTable("silver_orders")
```

## Gold Layer

Customer-level business aggregation:

``` text
customer_id
total_revenue
total_orders
```

Example:

``` python
df_gold = (
    df_silver_read
    .groupBy("customer_id")
    .agg(
        sum("order_amount").alias("total_revenue"),
        count("order_id").alias("total_orders")
    )
)
```

Stored as:

``` text
gold_customer_sales
```

## File Archiving

After successful processing:

``` text
Files/raw_orders.csv
        |
        v
Files/archive/raw_orders.csv
```

This helps prevent the same processed input from being picked up again.

## Error Handling

The project uses `try/except` and propagates failures:

``` python
try:
    # processing
except Exception as e:
    print("Processing failed")
    print("Error:", str(e))
    raise
```

## Logging

Reusable logging:

``` python
from datetime import datetime

def log_message(message):
    print(f"[{datetime.now()}] {message}")
```

Example:

``` python
log_message("Order processing started")
log_message("Validation completed")
log_message("Silver table created")
log_message("Gold table created")
log_message("Order processing completed successfully")
```

## Parent-Child Orchestration

``` text
Parent
  |
  | notebook.run()
  v
Child
  |
  | notebook.exit("SUCCESS")
  v
Parent
```

The parent can pass parameters such as:

``` python
{
    "input_path": "Files/raw_orders.csv",
    "environment": "dev"
}
```

## Timeout and Retry

Child execution uses a timeout:

``` python
notebookutils.notebook.run(
    "02_transform_orders",
    120,
    {...}
)
```

A limited retry pattern was also practiced for temporary failures.

## Idempotency / Safe Reruns

The project considers safe reruns through: - Delta overwrite during
development - Duplicate handling - Unique order IDs - Archiving
successfully processed input

Goal:

``` text
Run 1 -> correct result
Run 2 -> same correct result
Run 3 -> same correct result
```

rather than continuously creating duplicates.

## Debugging Skills

Practical issues debugged included:

### PATH_NOT_FOUND

``` text
Files/raw_orders.csv not found
```

Investigated with:

``` python
files = notebookutils.fs.ls("Files/")

for file in files:
    print(file.name)
```

### NameError

``` text
name 'input_path' is not defined
```

Investigated through parameter cells and Parent-to-Child parameter
passing.

### UNRESOLVED_COLUMN

Example:

``` text
order_amount cannot be resolved
```

Debugged by checking:

``` python
print(df.columns)
```

and ensuring the transformation creating `order_amount` occurs before
selecting it.

## Notebook Concepts Covered

-   Notebook parameters
-   `%run`
-   Reusable functions
-   Parent / Child notebooks
-   `notebook.run()`
-   `notebook.exit()`
-   Parameter passing
-   Error handling
-   Logging
-   Timeout
-   Retry
-   Session / runtime concepts
-   Python package concepts
-   `%pip` concept
-   Fabric Environment concept
-   Secrets / credential concepts
-   Debugging
-   Conditional execution
-   Monitoring / run-history concepts
-   Notebook dependencies
-   Production notebook structure

## Production-Oriented Checklist

### Data Quality

-   [x] Input file validation
-   [x] Required columns
-   [x] NULL handling
-   [x] Duplicate handling
-   [x] Business-rule validation
-   [x] Data cleansing

### Processing

-   [x] Silver layer
-   [x] Gold layer
-   [x] Delta format
-   [x] File archiving

### Reliability

-   [x] Error handling
-   [x] Logging
-   [x] Retry
-   [x] Timeout
-   [x] Safe-rerun considerations

### Architecture

-   [x] Parent notebook
-   [x] Child notebook
-   [x] Reusable functions
-   [x] Parameters
-   [x] Notebook dependencies

### Operations

-   [x] Debugging
-   [x] Monitoring concepts
-   [x] Run-history concepts

### Security

-   [x] No hardcoded credentials
-   [x] Secret-management concept

## Next Phases

### Spark Configuration

Planned topics: - Spark configuration - SparkSession - Shuffle
partitions - Adaptive Query Execution - Driver/executor concepts -
Memory concepts - Partition configuration - Performance tuning -
Practical scenarios

### Structured Streaming

Planned topics: - `readStream` - `writeStream` - Triggers - Output
modes - Checkpointing - Watermarking - Late-arriving data - Streaming
joins - Streaming aggregations - Failure recovery - Production streaming
design

## Learning Goal

The project demonstrates the complete data-engineering thought process:

``` text
Raw Data
   |
   v
Validate
   |
   v
Detect bad records
   |
   v
Reject / Clean
   |
   v
Transform
   |
   v
Silver
   |
   v
Gold
   |
   v
Archive
   |
   v
Monitor
   |
   v
Handle failures
   |
   v
Safe rerun
   |
   v
Production-oriented pipeline
```

