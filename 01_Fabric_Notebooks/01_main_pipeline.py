#!/usr/bin/env python
# coding: utf-8

# ## 01_main_pipeline
# 
# null

# # E-Commerce Order Processing Pipeline
# 
# ## Purpose
# Main orchestration notebook for processing E-Commerce order data.
# 
# ## Responsibilities
# - Receive parameters
# - Check input files
# - Validate input availability
# - Execute transformation notebook
# - Handle success/failure
# - Log pipeline progress

# In[1]:


# Fabric Environment

import requests

print("requests version:", requests.__version__)


# In[15]:


import time

max_retries = 1

for attempt in range(1, max_retries + 1):

    try:
        print(f"Attempt {attempt}")

        result = notebookutils.notebook.run(
            "02_transform_orders",
            120,
            {
                "input_path": "raw_order.csv",
                "environment": "dev"
            }
        )

        print("Child result:", result)
        break

    except Exception as e:

        print(f"Attempt {attempt} failed: {str(e)}")

        if attempt == max_retries:
            raise

        time.sleep(5)



# 
