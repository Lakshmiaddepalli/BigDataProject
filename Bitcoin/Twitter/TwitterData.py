#!/usr/bin/env python
# coding: utf-8

# In[2]:


#!pip install selenium


# In[20]:


import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from datetime import datetime, timedelta
import textblob
from textblob import TextBlob
import pandas as pd 


# In[ ]:


browser = webdriver.Chrome()
base_url = u'https://twitter.com/search?q='
query = u'%23bitcoinprice'
url = base_url + query

browser.get(url)
time.sleep(1)
data = []

body = browser.find_element_by_tag_name('body')

for _ in range(1000000):
    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(0.2)
    
streams = browser.find_elements_by_class_name('stream-item')


for stream in streams:
    tweets  = ''
    dt = ''
    tweet = stream.find_elements_by_class_name('tweet-text')
    for t in tweet:
        tweets = t.text
    timestamp = stream.find_elements_by_class_name('tweet-timestamp')
    for s in timestamp:
        dt = s.text
    data.append({'date':str(dt),
                 'tweet':str(tweets)})

    
df = pd.DataFrame(data)


# In[29]:


df.head(5)


# In[30]:


tweets = df.to_csv("tweetstest.csv", index = None, header=True)


# In[ ]:




