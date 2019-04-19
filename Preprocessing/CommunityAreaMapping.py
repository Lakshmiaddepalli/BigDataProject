#!/usr/bin/env python
# coding: utf-8

# In[162]:


import csv
import json


# In[163]:


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


# In[164]:


mydict = {}
with open('Census_Data_-_Selected_socioeconomic_indicators_in_Chicago__2008___2012.csv', mode='r') as infile:
    reader = csv.reader(infile)
    mydict = {rows[1]:rows[0] for rows in reader}


# In[165]:


def getVal(key):
    return key, mydict[key]


# In[174]:


def getValString(val):
    for key in mydict.keys():
        if val != None and val in key:
            return key,mydict[key]
    return None,0


# In[175]:


for key,val in mydict.items():
    print(key, "=>", val)


# In[176]:


def get_neighborhood_for_point(lat, lng, commareas):
    for commarea, commdata in commareas.items():
        if point_inside_polygon(lng, lat, commdata):
            return commarea
    else:
        return None


# In[177]:


user_data = ""
with open('community_areas.json', 'r') as file:
        user_data = json.load(file)


# In[178]:


ofile  = open('Sex_Offenders_final.csv', "w")
writer = csv.writer(ofile)
i = 0


# In[179]:


with open('Sex_Offenders_modified.csv', mode='r') as infile:
    reader = csv.reader(infile)
    next(reader)
    for row in reader:
        i = i+1
        print(i)
        print(row[10])
        print(row[11])
        if(len(row[10]) > 0 and len(row[11]) > 0):
            res = get_neighborhood_for_point(float(row[10]),float(row[11]),user_data)
            try:
                ca,parseval = getVal(res)
            except KeyError:
                ca,parseval = getValString(res)
            print(ca)
            print(parseval)
            row.append(ca)
            row.append(parseval)
            writer.writerow(row)


# In[172]:


ofile.close()


# In[173]:


infile.close()


# In[ ]:




