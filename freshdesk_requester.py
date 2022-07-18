#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import awswrangler as wr
import requests
import json
import pandas as pd
from tqdm import tqdm
import boto3
import os
import re
import io


my_session = boto3.Session(aws_access_key_id="AKIAT3PM4W6RC3TZ7EY",aws_secret_access_key="rbebflz+HWx/HVrrclFixdwxM5ETLNzqhf94RoAR",region_name="eu-west-1")

i=1
j=0
request_kaufmann = pd.DataFrame([])
while j == 0:
    url = "https://kaufmannayuda.freshdesk.com/api/v2/contacts?page="+str(i)+"&per_page=100"
    payload={}
    #headers = {'Authorization': 'Bearer Q2ZEcFhWZXpWVnI3VEZ4TGlmMWc=','Cookie': '_x_w=38_3'}
    headers = {'Authorization': 'Bearer VJPS3BuWHFEYTFZemtKams6WA==','Cookie': '_x_w=38_3'}
    response = requests.request("GET", url, headers=headers, data=payload)
    a=json.loads(response.text)

    req_kaufmann = pd.json_normalize(a)
    i=i+1
    print(i)
    if len(req_kaufmann) == 0:
        j=1
    else:
        request_kaufmann = request_kaufmann.append(req_kaufmann)

wr.s3.to_parquet(df=(request_kaufmann),
                 path="s3://kaufmann-data-science/freshdesk_kaufmann_request/freshdesk_kaufmann_request.parquet",
                 dataset=True,
                 mode = 'overwrite',
                 boto3_session=my_session)


# In[32]:



i=1
j=0
request_kaufparts = pd.DataFrame([])
while j == 0:
    url = "https://kaufparts.freshdesk.com/api/v2/contacts?page="+str(i)+"&per_page=100"
    payload={}
    headers = {'Authorization': 'Bearer Q2ZEcFhWZXpWVnI3VEZ4TGlmMWc=','Cookie': '_x_w=38_3'}
    #headers = {'Authorization': 'Bearer YVJPS3BuWHFEYTFZemtKams6WA==','Cookie': '_x_w=38_3'}
    response = requests.request("GET", url, headers=headers, data=payload)
    a=json.loads(response.text)

    req_kaufmann = pd.json_normalize(a)
    i=i+1
    print(i)
    if len(req_kaufmann) == 0:
        j=1
    else:
        request_kaufparts = request_kaufparts.append(req_kaufmann)

        
wr.s3.to_parquet(df=(request_kaufparts),
                 path="s3://kaufmann-data-science/freshdesk_kaufparts_request/freshdesk_kaufparts_request.parquet",
                 dataset=True,
                 mode = 'overwrite',
                 boto3_session=my_session)


# In[ ]:




