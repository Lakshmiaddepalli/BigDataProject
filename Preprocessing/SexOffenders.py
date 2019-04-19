#!/usr/bin/env python
# coding: utf-8

# In[7]:


import csv
import json
from geopy import geocoders
from geopy.geocoders import GoogleV3
import time


# In[8]:


def point_inside_polygon(x,y,poly):
    n = len(poly)
    inside =False

    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x,p2y = poly[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xinters = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x,p1y = p2x,p2y

    return inside


# In[9]:


def getVal(key):
    return key, mydict[key]


# In[10]:


def getValString(val):
    for key in mydict.keys():
        if val != None and val in key:
            return key,mydict[key]
    return None,0


# In[11]:


def get_neighborhood_for_point(lat, lng, commareas):
    for commarea, commdata in commareas.items():
        if point_inside_polygon(lng, lat, commdata):
            return commarea
    else:
        return None


# In[12]:


user_data = ""
with open('community_areas.json', 'r') as file:
        user_data = json.load(file)


# In[13]:


ofile  = open('Sex_Offenders_modified.csv', "w")
writer = csv.writer(ofile)


# In[17]:


YOUR_API_KEY =  "AIzaSyD2v_0UftOba5BQyt5WY19cjAFdiu9S7FQ"
g = GoogleV3(api_key=YOUR_API_KEY)
dic_blockgeomapping = {}
dic_block_communityareamapping = {}
i = 0
with open('Sex_Offenders.csv', mode='r') as infile:
    reader = csv.reader(infile)
    rowval = next(reader)
    rowval.append("latitude")
    rowval.append("longitude")
    writer.writerow(rowval)
    for row in reader:
        address = " ".join(row[2].split(" ", 2)[2:])
        i = i+1
        print(i)
        print(address)
        time.sleep(0.2)
        location = g.geocode(address)
        if(location == None):
            location = g.geocode(address + " Chicago")
        row.append(location.latitude)
        row.append(location.longitude)
        res = get_neighborhood_for_point(location.latitude,location.longitude,user_data)
        print(res)
        dic_blockgeomapping[address] = [location.latitude,location.longitude]
        dic_block_communityareamapping[address] = res
        writer.writerow(row)


# In[18]:


ofile.close()
infile.close()


# In[19]:


len(dic_blockgeomapping)


# In[27]:


bc  = open('block_geocoding_mapping.csv', "w")
w = csv.writer(bc)
for key, val in dic_blockgeomapping.items():
    w.writerow([key, val])
bc.close()


# In[28]:


bca = open("block_communityareamapping.csv", "w")
w = csv.writer(bca)
for key, val in dic_block_communityareamapping.items():
    w.writerow([key, val])
bca.close()


# In[ ]:




