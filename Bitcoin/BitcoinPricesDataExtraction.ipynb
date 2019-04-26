{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import urllib.request\n",
    "from bs4 import BeautifulSoup\n",
    "import pandas as pd\n",
    "from datetime import datetime"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "bitcoin_url = 'https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20180315&end=20190315'\n",
    "bitcoin_pages = urllib.request.urlopen(bitcoin_url)\n",
    "soup = BeautifulSoup(bitcoin_pages, 'html.parser')\n",
    "\n",
    "bitoinpriceDiv = soup.find('div', attrs={'class':'table-responsive'})\n",
    "bcprices = bitoinpriceDiv.find_all('tr')\n",
    "\n",
    "bitcoin_data = []\n",
    "i = 0;\n",
    "for bc in bcprices:\n",
    "    tmpdata = []\n",
    "    bitcointds = bc.findChildren()\n",
    "\n",
    "    for bitcoin in bitcointds:\n",
    "        tmpdata.append(bitcoin.text)\n",
    "\n",
    "    if(i > 0):\n",
    "        tmpdata[0] = tmpdata[0].replace(',','')\n",
    "        tmpdata[5] = tmpdata[5].replace(',','')\n",
    "        tmpdata[6] = tmpdata[6].replace(',','')\n",
    "        bitcoin_data.append({'date':datetime.strptime(tmpdata[0], '%b %d %Y'),\n",
    "                     'open':float(tmpdata[1]),\n",
    "                     'high':float(tmpdata[2]),\n",
    "                     'low':float(tmpdata[3]),\n",
    "                     'close':float(tmpdata[4]),\n",
    "                     'volume':float(tmpdata[5]),\n",
    "                     'mcap':float(tmpdata[6])})\n",
    "\n",
    "    i = i + 1;\n",
    "\n",
    "dfval = pd.DataFrame(bitcoin_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [],
   "source": [
    "bitcoincsv = dfval.to_csv (\"finalbitcoin.csv\", index = None, header=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>close</th>\n",
       "      <th>date</th>\n",
       "      <th>high</th>\n",
       "      <th>low</th>\n",
       "      <th>mcap</th>\n",
       "      <th>open</th>\n",
       "      <th>volume</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>3960.91</td>\n",
       "      <td>2019-03-15</td>\n",
       "      <td>3968.54</td>\n",
       "      <td>3914.02</td>\n",
       "      <td>6.967500e+10</td>\n",
       "      <td>3926.66</td>\n",
       "      <td>9.394211e+09</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>3924.37</td>\n",
       "      <td>2019-03-14</td>\n",
       "      <td>3946.50</td>\n",
       "      <td>3901.30</td>\n",
       "      <td>6.902470e+10</td>\n",
       "      <td>3905.58</td>\n",
       "      <td>1.048079e+10</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>3906.72</td>\n",
       "      <td>2019-03-13</td>\n",
       "      <td>3926.60</td>\n",
       "      <td>3891.90</td>\n",
       "      <td>6.870670e+10</td>\n",
       "      <td>3913.05</td>\n",
       "      <td>9.469185e+09</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>3909.16</td>\n",
       "      <td>2019-03-12</td>\n",
       "      <td>3926.89</td>\n",
       "      <td>3863.56</td>\n",
       "      <td>6.874300e+10</td>\n",
       "      <td>3903.76</td>\n",
       "      <td>9.809887e+09</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>3905.23</td>\n",
       "      <td>2019-03-11</td>\n",
       "      <td>3966.38</td>\n",
       "      <td>3889.24</td>\n",
       "      <td>6.866693e+10</td>\n",
       "      <td>3953.74</td>\n",
       "      <td>1.012590e+10</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "     close       date     high      low          mcap     open        volume\n",
       "0  3960.91 2019-03-15  3968.54  3914.02  6.967500e+10  3926.66  9.394211e+09\n",
       "1  3924.37 2019-03-14  3946.50  3901.30  6.902470e+10  3905.58  1.048079e+10\n",
       "2  3906.72 2019-03-13  3926.60  3891.90  6.870670e+10  3913.05  9.469185e+09\n",
       "3  3909.16 2019-03-12  3926.89  3863.56  6.874300e+10  3903.76  9.809887e+09\n",
       "4  3905.23 2019-03-11  3966.38  3889.24  6.866693e+10  3953.74  1.012590e+10"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "dfval.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "close     366\n",
       "date      366\n",
       "high      366\n",
       "low       366\n",
       "mcap      366\n",
       "open      366\n",
       "volume    366\n",
       "dtype: int64"
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "dfval.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.1"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
