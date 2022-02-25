import os
from pathlib import Path

import re
from time import time

import numpy as np
import pandas as pd

import tqdm
from multiprocessing.pool import ThreadPool
from itertools import repeat

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import HTTPError, ConnectionError, Timeout, ReadTimeout

from io import BytesIO
import zipfile as zp

from bs4 import BeautifulSoup

# import logging

# import http.client
# http.client.HTTPConnection.debuglevel = 1

# # You must initialize logging, otherwise you'll not see debug output.
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

# convert to beautiful soup obkect using lxml parser
def get_soup(URL):
    bsoup = BeautifulSoup(requests.get(URL).text, 'lxml')
    files = bsoup.findAll('a', attrs={'href': re.compile('.zip')})
    return files

# Download zip file from url
def get_zip_content(link_to_zipfile, download_path):
    try:
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5) # retry three times in case of connection error
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        response = session.get(link_to_zipfile, stream=True, timeout=(3,10))
        if response.ok:
            content = BytesIO(response.content)
    except ReadTimeout as e:
        print(e)
    return content

def parseandsave_txt(content, filename, save=True):
    import pandas as pd
    
    patents = re.sub(" +" , " ", content).split('PATN')[1:]
    patent_number_pattern = "WKU\s(.*?)\s"
    patent_numbers = [re.search(patent_number_pattern, i).group(1) for i in patents]
    patent_descriptions = [i.partition('BSUM')[2] if 'BSUM' in i else i.partition('PAL')[2] for i in patents]
    patent_info = dict(zip(patent_numbers, patent_descriptions))
    df = pd.DataFrame({'WKU': patent_numbers,'DESC': patent_descriptions})
    if save:
        df.to_csv(Path.joinpath(download_path, filename.split('.')[0]+'.csv'))
    else:
        return df

def get_description(link_to_zipfile, download_path):
    zip_content = get_zip_content(link_to_zipfile, download_path)
    try:
        with zp.ZipFile(zip_content, 'r') as archive:
            for filename in archive.namelist():
                if filename.endswith('.txt'):
                    content = archive.read(filename).decode('utf-8')
                    parseandsave_txt(content,filename, save=True)
            archive.close()
    except zp.BadZipFile as e:
        print(e)


ROOT = Path(os.getcwd())
DATA = ROOT.joinpath('USPTO_code')
DATA_year = DATA.joinpath('roots')

years = np.arange(2000,2001)

for year in years:
    
    # create year folder
    download_path = DATA.joinpath(str(year))
    download_path.mkdir(parents=True, exist_ok=True)
    print(download_path)
    
    # url to extract zip files from
    URL = f'https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/{year}/'
    soup = get_soup(URL)
    URLs = [URL+i['href'] for i in soup]
    print(f"No. of files for the year {year}: {len(URLs)}")
    
    start = time()
    thread_pool = ThreadPool(10)
    tqdm.tqdm(thread_pool.starmap(get_description, zip(URLs, [download_path]*len(URLs))), total=len(URLs))
    print(f"Time to download: {time() - start}")