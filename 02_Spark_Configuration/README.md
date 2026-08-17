# 02 - Spark Configuration

## Overview

This module covers Apache Spark configuration, execution fundamentals,
performance optimization, and debugging using PySpark in Microsoft Fabric.

## Concepts Covered

### Spark Fundamentals
- Driver
- Executor
- Partition
- Task
- Job
- Stage
- Shuffle
- Cluster

### Partition & Performance
- `repartition()`
- `coalesce()`
- `spark.sql.shuffle.partitions`
- Adaptive Query Execution (AQE)
- Broadcast Join
- Data Skew
- Skewed Join
- AQE Skew Join

### Performance Optimization
- Small Files Problem
- Partition Pruning
- Predicate Pushdown
- Cache / Persist

### Spark UI & Debugging
- Jobs
- Stages
- Tasks
- Shuffle Read / Shuffle Write
- Slow Task identification

### Microsoft Fabric Spark Compute
- Spark Pool
- Starter / Default Pool
- Custom Pool
- Nodes
- Minimum / Maximum Nodes
- Autoscale
- Node Size

## Practical Work

During this module, I worked:

- Checking DataFrame partitions
- Using `repartition()` and `coalesce()`
- Checking Spark configuration values
- Observing AQE behavior
- Testing Broadcast Join execution plans
- Creating and understanding Data Skew
- Inspecting Spark Jobs and Stages using Spark UI
- Understanding Shuffle Read and Shuffle Write
- Understanding Fabric Spark compute configuration

## Key Learning

```text
Driver
   ↓
Executors
   ↓
Tasks
   ↓
Partitions

Action
   ↓
Job
   ↓
Stage
   ↓
Tasks