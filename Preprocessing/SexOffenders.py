#!/usr/bin/env python
# coding: utf-8

# In[15]:


import csv
import json
from geopy import geocoders
from geopy.geocoders import GoogleV3
import time


# In[16]:


ofile  = open('Sex_Offenders_modified.csv', "w")
writer = csv.writer(ofile)


# In[20]:


YOUR_API_KEY =  "AIzaSyD2v_0UftOba5BQyt5WY19cjAFdiu9S7FQ"
g = GoogleV3(api_key=YOUR_API_KEY)

with open('Sex_Offenders.csv', mode='r') as infile:
    reader = csv.reader(infile)
    rowval = next(reader)
    rowval.append("latitude")
    rowval.append("longitude")
    writer.writerow(rowval)
    for row in reader:
        address = " ".join(row[2].split(" ", 2)[2:])
        time.sleep(1)
        location = g.geocode(address)
        row.append(location.latitude)
        row.append(location.longitude)
        writer.writerow(row)


# In[21]:


ofile.close()
infile.close()


# In[ ]:




