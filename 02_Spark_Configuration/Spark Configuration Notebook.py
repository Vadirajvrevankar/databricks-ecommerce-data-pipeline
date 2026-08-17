#!/usr/bin/env python
# coding: utf-8

# ## Spark Configuration Notebook
# 
# New notebook

# # spark configuration 

# In[2]:


# Create a DataFrame containing numbers from 0 to 99

df = spark.range(100)

print(df.rdd.getNumPartitions())


# In[3]:


# Check the current number of partitions
df.rdd.glom().collect()


# In[18]:


#REPARTITION CONCEPT

# Create a new DataFrame with 12 partitions.
# Spark redistributes the data across the 12 partitions.

df12 = df.repartition(12)



print("Before:", df.rdd.getNumPartitions())
print("After :", df12.rdd.getNumPartitions())


# repartition()
# → Changes partitions
# → Redistributes data
# → Causes shuffle

# In[10]:


# Repartition the data into 4 partitions
# based on customer_id.

df_customer = df.repartition(
    4,
    "customer_id"
)


# coalesce()
# → Combines existing partitions
# → No full shuffle 🚫

# In[19]:


#COLEASCE CONCEPT
# used to reduce partitions

df.coalesce(2)  



# repartition() → Redistributes data to change the number of partitions.
# 
# coalesce() → Reduces partitions by combining existing partitions without a full shuffle.

# In[11]:


# Check Spark's current shuffle partition setting

shuffle_partitions = spark.conf.get(
    "spark.sql.shuffle.partitions"
)


print("Shuffle partitions:", shuffle_partitions)

#When a shuffle happens, Spark's SQL engine uses 200 partitions for the shuffle stage.


# In[12]:


# Create sample order data
orders = spark.range(1000)

# Check current partitions
print(
    "Before shuffle:",
    orders.rdd.getNumPartitions()
)

# groupBy triggers a shuffle
result = orders.groupBy("id").count()

# Check the resulting partition count
print(
    "After shuffle:",
    result.rdd.getNumPartitions()
)


# spark.sql.shuffle.partitions = 200  - (AQE)
#         ↓
# Starting shuffle partition setting
# 
# AQE
#         ↓
# Can reduce unnecessary partitions
# 
# Your result = 8
#         ↓
# Spark optimized the small workload

# AQE
# 
# [spark.sql.adaptive.enabled.]
# [spark.sql.adaptive.coalescePartitions.enabled].
# 
# Spark automatically adjusts the execution plan based on actual runtime data

# In[20]:


#ADE CONCEPT

# Check whether Adaptive Query Execution is enabled
print(
    "AQE enabled:",
    spark.conf.get("spark.sql.adaptive.enabled")
)

# Check whether AQE can reduce/coalesce shuffle partitions
print(
    "AQE coalescing:",
    spark.conf.get(
        "spark.sql.adaptive.coalescePartitions.enabled"
    )
)


# Normal join
# → Shuffle data 🔄
# → Expensive
# 
# Broadcast join
# → Send small table to executors 📡
# → Avoid large shuffle
# → Often faster

# In[14]:


# BROADCAST JOIN CONCEPT

# Check Spark's automatic broadcast join threshold
broadcast_threshold = spark.conf.get(
    "spark.sql.autoBroadcastJoinThreshold"
)

print(
    "Broadcast threshold:",
    broadcast_threshold
)

#Spark can automatically consider broadcasting a table when its estimated size is ≤ 25 MB.


# In[15]:


# Create a large orders DataFrame
orders = spark.range(1000000).withColumnRenamed(
    "id",
    "order_id"
)

# Create a small products DataFrame
products = spark.range(100).withColumnRenamed(
    "id",
    "product_id"
)

print("Orders:", orders.count())
print("Products:", products.count())


# In[16]:


# Join the large orders table
# with the small products table.
joined_df = orders.join(
    products,
    orders.order_id == products.product_id
)

# Display the physical execution plan
joined_df.explain()


# In[21]:


#DATA SKEW CONCEPT
#Data skew happens when data is distributed unevenly across partitions, causing some tasks to process much more data than other


# In[27]:


# C1 has many orders → skewed key
orders = spark.createDataFrame([
    ("C1", 100),
    ("C1", 200),
    ("C1", 300),
    ("C1", 400),
    ("C1", 500),
    ("C2", 100),
    ("C3", 200)
], ["customer_id", "amount"])


# In[28]:


# Small customer lookup table
customers = spark.createDataFrame([
    ("C1", "India"),
    ("C2", "USA"),
    ("C3", "UK")
], ["customer_id", "country"])


# In[29]:


# Join orders with customer information
joined = orders.join(
    customers,
    "customer_id"
)

display(joined)


# In[31]:


# Check whether AQE skew join optimization is enabled
print(
    "Skew Join Enabled:",
    spark.conf.get(
        "spark.sql.adaptive.skewJoin.enabled"
    )
)

#AQE Skew Join → Helps Spark handle heavily skewed join partitions.


# In[33]:


# Create sample data
df = spark.range(1000000)

# Perform an action to trigger Spark execution
count = df.count()

print("Count:", count)

# job - stage - task automaticly created 


# In[34]:


# Create sample data
df = spark.range(100000)

# groupBy can cause a shuffle
result = df.groupBy("id").count()

# Trigger execution
result.count()


# In[1]:


# Create 100,000 records
df = spark.range(100000)

# Create a highly skewed key
# 90% of records will have customer_id = "C1"
skewed_df = df.selectExpr(
    "CASE WHEN id < 90000 THEN 'C1' ELSE CAST(id AS STRING) END AS customer_id"
)

# Trigger a shuffle
result = skewed_df.groupBy("customer_id").count()

# Execute the job
result.show()

