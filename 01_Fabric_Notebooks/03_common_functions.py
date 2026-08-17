#!/usr/bin/env python
# coding: utf-8

# ## 03_common_functions
# 
# null

# In[1]:


from datetime import datetime

def log_message(message):
    print(f"[{datetime.now()}] {message}")


# In[ ]:


def validate_required_columns(df, required_columns):

    missing_columns = []

    for column in required_columns:
        if column not in df.columns:
            missing_columns.append(column)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    return True


# In[ ]:


def show_null_records(df, columns):

    for column in columns:

        null_records = df.filter(
            df[column].isNull()
        )

        if null_records.count() > 0:
            print(f"NULL values found in: {column}")
            display(null_records)


# In[ ]:


def validate_duplicates(df, column):

    duplicate_records = (
        df.groupBy(column)
          .count()
          .filter("count > 1")
    )

    if duplicate_records.count() > 0:
        display(duplicate_records)
        raise ValueError(
            f"Duplicate values found in {column}"
        )

    return True


# In[ ]:


def get_invalid_positive_values(df, column):
    return df.filter(
        df[column] <= 0
    )


# In[ ]:


def validOrders(df, col):
    return df.filter(
        df[col] > 0
    )


# In[ ]:


def showRejectCustomer(df, col):
    return df.filter(
        df[col].isNull()
    )


 


# In[ ]:


def get_valid_records(df, col):
    return df.filter(
        df[col].isNotNull()
    )


# In[ ]:


def checkValidPrice(df, column):

    df_valid_price = df.filter(
        df[column] > 0
    )

    return df_valid_price


# In[ ]:


def get_duplicates(df, col):
    return (
        df.groupBy(col)
          .count()
          .filter("count > 1")
    )

