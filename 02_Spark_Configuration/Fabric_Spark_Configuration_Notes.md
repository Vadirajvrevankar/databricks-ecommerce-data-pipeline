# Fabric + PySpark Learning Notes

## Spark Fundamentals

**Driver** → Main process that controls and coordinates the Spark application.

**Executor** → Process that performs actual data processing and runs tasks.

**Partition** → A chunk of data that Spark processes independently.

**Task** → Actual work Spark performs on one partition.

**Job** → Complete computation triggered by a Spark action.
Example: `df.count()`

**Stage** → Group of tasks between shuffle boundaries.

**Shuffle** → Redistributing data between partitions.
Common examples: `groupBy()`, `join()`, `distinct()`, `orderBy()` can cause shuffle.

**Cluster** → Group of computing resources used to run Spark applications.

---

## Partitions

### Check partitions
```python
df = spark.range(100)
print(df.rdd.getNumPartitions())
```

### repartition()
**Definition:** Changes partitions by redistributing data.
**Important:** Causes shuffle.

```python
df4 = df.repartition(4)
```

### coalesce()
**Definition:** Reduces partitions without a full shuffle.

```python
df2 = df4.coalesce(2)
```

**Remember:**
- repartition → redistribution + shuffle
- coalesce → reduce partitions + no full shuffle

---

## Shuffle Configuration

### spark.sql.shuffle.partitions
**Definition:** Starting number of partitions used for many shuffle operations.

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Our environment showed: `200`.

**Important:** 200 does not mean every DataFrame has 200 partitions.

---

## AQE

**AQE (Adaptive Query Execution)** → Allows Spark to adjust execution using runtime information.

```python
spark.conf.get("spark.sql.adaptive.enabled")
```

Our value: `true`.

### AQE coalescing
**Definition:** AQE can combine small shuffle partitions.

```python
spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")
```

Our value: `true`.

---

## Broadcast Join

**Definition:** Sends a small table to executors so Spark can avoid a large shuffle.

```python
joined_df = orders.join(
    products,
    orders.product_id == products.product_id
)

joined_df.explain()
```

Look for `BroadcastHashJoin`.

### Broadcast threshold

```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
```

Our value: `26214400` bytes ≈ `25 MB`.

---

## Data Skew

**Definition:** Uneven distribution of data, causing some partitions/tasks to have much more work.

Example:
```text
C1 → 5 records
C2 → 1 record
C3 → 1 record
C4 → 1 record
```

C1 has much more data → skew.

---

## Skewed Join

**Definition:** A join where some keys contain much more data than others, causing uneven work.

### AQE Skew Join

**Definition:** AQE can optimize heavily skewed join partitions.

```python
spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
```

Our value: `true`.

---

## Spark UI

**Definition:** Spark UI is used to monitor and debug Spark execution.

Important areas:
- Jobs → Spark jobs
- Stages → stages and tasks
- SQL → DataFrame/SQL execution
- Executors → executor activity

### Execution relationship
```text
Action
 ↓
Job
 ↓
Stage
 ↓
Tasks
 ↓
Partitions
```

### Shuffle Read
**Definition:** Data read after a shuffle.

### Shuffle Write
**Definition:** Data written during a shuffle.

### Slow Task
**Definition:** A task that takes much longer than other tasks.

---

## Small Files

**Definition:** Too many small files create overhead and can slow processing.

Example:
```text
1 file → 1 GB       Good
10,000 tiny files   Problem
```

Common relationship:
```text
Too many partitions
 ↓
Too many output files
 ↓
Small Files Problem
```

---

## Partition Pruning

**Definition:** Spark skips unnecessary partitions and reads only required partitions.

Example:
```text
year=2024
year=2025
year=2026
```

Filter:
```python
df.filter("year = 2026")
```

Spark can skip 2024 and 2025.

**Remember:** Partition pruning = skip unnecessary partitions.

---

## Predicate Pushdown

**Definition:** Pushes filter conditions closer to the data source so less data is read.

Example:
```python
df.filter("customer_id = 'C101'")
```

**Difference:**
- Partition pruning → skips partitions
- Predicate pushdown → filters closer to the data source

---

## Cache / Persist

### Cache
**Definition:** Keeps a DataFrame available for reuse to avoid recomputation.

```python
df.cache()
```

### Persist
**Definition:** Stores a DataFrame using a selected storage level.

```python
df.persist()
```

Use when the same DataFrame is reused multiple times. Do not cache everything.

---

# Fabric Spark Compute

**Spark Compute** → Resources used to execute Spark workloads.

**Spark Pool** → Pool of compute resources for Spark.

**Starter/Default Pool** → Managed/default compute; good for learning, development, testing and normal workloads.

**Custom Pool** → User-configured Spark compute for workloads needing more control.

**Node** → Compute machine/resource providing CPU and memory.

**Minimum Nodes** → Minimum nodes in the scaling range.

**Maximum Nodes** → Maximum nodes the pool can scale to.

**Autoscale** → Automatically adjusts nodes according to workload demand.

**Node Size** → CPU and memory capacity available per node.

### Node vs Partition
```text
Node      → Compute resource
Partition → Chunk of data
```



---

# Quick Memory 

```text
Driver      → Controls
Executor    → Processes
Partition   → Data chunk
Task        → Work on partition
Job         → Complete computation
Stage       → Group of tasks
Shuffle     → Moves/redistributes data
repartition→ Changes partitions + shuffle
coalesce   → Reduces partitions without full shuffle
AQE         → Runtime optimization
Broadcast   → Sends small table to executors
Data Skew   → Uneven data distribution
Spark UI    → Monitor/debug
Pruning     → Skip partitions
Pushdown    → Filter early
Cache       → Reuse data
Persist     → Reuse data with storage level
Custom Pool → Configured compute
Node        → Compute resource
Max Nodes   → Maximum scaling
Autoscale   → Adjusts nodes
```


