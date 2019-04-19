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


# In[25]:


address = 'CENTRAL AVE'
YOUR_API_KEY =  "AIzaSyD2v_0UftOba5BQyt5WY19cjAFdiu9S7FQ"
g = GoogleV3(api_key=YOUR_API_KEY)
location = g.geocode(address, timeout=15)
if(location == None):
    location = g.geocode(address + " Chicago")
print(location.latitude)
print(location.longitude)


# In[ ]:




