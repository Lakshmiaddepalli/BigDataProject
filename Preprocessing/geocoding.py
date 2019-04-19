#!/usr/bin/env python
# coding: utf-8

# In[16]:


#!pip install geopy


# In[2]:


import geocoder
import time


# In[14]:


from geopy import geocoders
from geopy.geocoders import GoogleV3


# In[15]:


address = 'Times Square, NY'
YOUR_API_KEY =  "AIzaSyD2v_0UftOba5BQyt5WY19cjAFdiu9S7FQ"
g = GoogleV3(api_key=YOUR_API_KEY)
location = g.geocode(address, timeout=15)
print(location.latitude)
print(location.longitude)

