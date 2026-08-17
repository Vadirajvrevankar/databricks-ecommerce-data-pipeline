#!/usr/bin/env python
# coding: utf-8

# ## 02_transform_orders
# 
# null

# In[7]:


print("child notebook started")


# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run "./03_common_functions"


# In[11]:


input_path = "raw_order.csv"
environment = "dev"


# In[12]:


print("input_path =", input_path)
print("environment =", environment)


# In[13]:


# Check input file



files = notebookutils.fs.ls("Files/")
file_names = [file.name for file in files]
print("File names:", file_names)

if input_path not in file_names:
    

    print("Input file found:", input_path)


# In[ ]:


try:
    print("Input path:", input_path)
    print("Environment:", environment)

    file_path = f"Files/{input_path}"

    print("Reading:", file_path)        

    df_orders_child = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    print("File read successfully")

except Exception as e:
    print("Failed to read input file")
    print("Error:", str(e))
    raise


# In[ ]:


# Step 3: Inspect orders data

print("Columns:")
print(df_orders_child.columns)

print("\nSchema:")
df_orders_child.printSchema()

print("\nSample data:")
display(df_orders_child.limit(10))


# In[ ]:


# Step 4: Validate required columns

log_message("Validating required columns")

required_columns = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_date",
]

validate_required_columns(
    df_orders_child,
    required_columns
)

print("Required column validation passed.")


# In[ ]:


# Step 5: Validate required values

log_message("checking null values")

required_non_null = [
    "order_id",
    "customer_id",
    "product_id"
]

def validate_no_nulls(df_orders_child, required_non_null):

    print("NULL validation passed.")


# In[ ]:


# Find records with NULL order_id

display(
    df_orders_child.filter(df_orders_child["customer_id"].isNull())
)


# In[ ]:


df_valid_orders = df_orders_child.filter(
    df_orders_child["customer_id"].isNotNull()
)

log_message("checked valid orders")


# In[ ]:


df_rejected_orders = df_orders_child.filter(
    df_orders_child["customer_id"].isNull()
)

log_message("checked rejected orders")


# In[ ]:


print("Valid:", df_orders_child.count())
print("Rejected:", df_orders_child.count())


# In[ ]:


df_duplicates = get_duplicates(
    df_orders_child,
    "order_id"
)

display(df_duplicates)

df_clean_orders = df_orders_child.dropDuplicates(
    ["order_id"]
)

display(df_duplicates)

df_clean_orders = df_orders_child.dropDuplicates(
    ["order_id"]
)


# In[ ]:


validate_duplicates(
    df_clean_orders,
    "order_id"
)


# In[ ]:


# Task 9: Remove duplicate order IDs


print("Original records:", df_orders_child.count())
print("After removing duplicates:", df_clean_orders.count())

display(df_clean_orders)

log_message("clean records")


# In[ ]:


df_invalid_quantity = get_invalid_positive_values(
    df_orders_child,
    "quantity"
)

display(df_invalid_quantity)


# In[ ]:


# Separate NULL customer_id records
df_accepted_customer = get_valid_records(
    df_valid_orders,
    "customer_id"
)


log_message("accepting customers")

print("Accepted:", df_accepted_customer.count())


# In[ ]:


display(df_valid_orders)


# In[ ]:


df_valid_price = checkValidPrice(
    df_accepted_customer,
    "unit_price"
)

display(df_valid_price)

log_message("checked invalid price")


# In[ ]:


df_valid_date =df_valid_price.filter(df_valid_price['order_date'].isNotNull())
df_invalid_date =df_valid_price.filter(df_valid_price['order_date'].isNull())

print("Valid date:", df_valid_date.count())
print("Invalid date:", df_invalid_date.count())


# In[ ]:


from pyspark.sql.functions import trim, col

df_whitespace = df_valid_date.filter(
    col("customer_id") != trim(col("customer_id"))
)

log_message("trimmed spaces")
display(df_whitespace)


# In[ ]:


from pyspark.sql.functions import trim, col

df_clean_orders = df_valid_date.withColumn(
    "customer_id",
    trim(col("customer_id"))
)

display(df_clean_orders)


# In[ ]:


from pyspark.sql.functions import col

df_clean_orders = df_clean_orders.withColumn(
    "order_amount",
    col("quantity") * col("unit_price")
)


# In[ ]:


df_silver = df_clean_orders.select(
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_date",
    "order_amount"
)

display(df_silver)


# In[ ]:


display(spark.table("silver_orders"))


# In[ ]:


df_silver_read = spark.table("silver_orders")

display(df_silver_read)

log_message("silver table craeted")


# In[ ]:


# gold layer

from pyspark.sql.functions import sum, count

df_gold = (
    df_silver_read
    .groupBy("customer_id")
    .agg(
        sum("order_amount").alias("total_revenue"),
        count("order_id").alias("total_orders")
    )
)

display(df_gold)


# In[ ]:


display(spark.table("gold_customer_sales"))


# In[ ]:


notebookutils.fs.mv(
    input_path,
    "Files/archive/new"
)

processing_status = "SUCCESS"
print("Order processing status:", processing_status)


# In[ ]:


notebookutils.notebook.exit("SUCCESS")

