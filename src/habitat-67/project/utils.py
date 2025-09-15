import itertools
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from itertools import chain, islice, tee
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Callable
import pandas as pd
import pytz
import requests
import urllib3
# import xmlschema
# import xmltodict
# from backoff import expo, on_exception
# from fake_useragent import UserAgent
from lxml import etree, html
# from ratelimit import RateLimitException, limits
from requests import Session
from requests.exceptions import (
    ConnectTimeout, ProxyError, ReadTimeout, RequestException, SSLError
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# TODO:
# refactor to use Pandas' read_parquet with pyarrow engine, saving the file with gzip compression: http://aseigneurin.github.io/2017/03/14/incrementally-loaded-parquet-files.html
# https://tomaztsql.wordpress.com/2022/05/08/comparing-performances-of-csv-to-rds-parquet-and-feather-data-types/
# https://towardsdatascience.com/stop-using-csvs-for-storage-this-file-format-is-150-times-faster-158bd322074e


class SessionHandler:

    def __init__(self, **kwargs):
        super(SessionHandler, self).__init__(**kwargs)
        self.session = self.create_session()

    def create_session(headers=None, url=None) -> Session:
        '''Return a Requests session with the appropriate headers'''

        headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0',
                # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',         # specifies the request source (user-initiated, in this case) for the server to decide if it should be allowed
                'Sec-Fetch-User': '?1',
                'Pragma': 'no-cache',             # for backwards compatibility with the HTTP/1.0 caches that do not have a Cache-Control HTTP/1.1 header
                'Cache-Control': 'no-cache',      # forces caches to submit the request to the origin server for validation before a cached copy is released
        }
        session = Session()
        session.headers.update(headers)
        if url:
            session.get(url)
        return session

    # def call_initialiser(self, calls=1, period=3, max_tries=15):
    #     '''A closure function which defines rate-limiting parameters for successive GET requests'''

    #     @on_exception(expo, (RateLimitException, RequestException), max_tries=max_tries, logger=__name__)
    #     @limits(calls=calls, period=period)
    #     def get_request(url: str, params: dict(), response_type: str):
    #         response = self.session.get(url, params=params, allow_redirects=True)
    #         # print(response.raw.version)
    #         # print(response.url)
    #         # print(response.status_code)
    #         if response.status_code == 200:
    #             # print(response.request.headers)
    #             if response_type == 'html':
    #                 return html.fromstring(response.content)
    #             elif response_type == 'json':
    #                 try:
    #                     return response.json()
    #                 except JSONDecodeError:
    #                         xml_object = xmltodict.parse(response.content)
    #                         stringified_json = json.dumps(xml_object)
    #                         return json.loads(stringified_json)
    #             elif response_type == 'xml':
    #                 return etree.XML(response.content)
    #             elif response_type == 'xsd':
    #                 return xmlschema.XMLSchema11(response.content)
    #     return get_request


class FileHandler:

    def __init__(self, file_header: list = None, generator: Callable = None, tag: str = '', file_label: str = None, delta: int = 1, **kwargs):
        super(FileHandler, self).__init__(**kwargs)
        self.file_header = file_header
        self.generator = generator
        self.tag = tag
        self.file_label = file_label
        self.delta = delta
        self.file_timestamp = None
        self.df = None

    def update(self, ticker: str = None) -> None:
        '''Update a file if necessary, and load its contents into memory'''

        if ticker:
            self.file_label = ticker
        if self.__file_exists():
            if self.__is_updated():
                pass
            else:
                self.__rename_file()
                self.save_latest_data()
        else:
            self.save_latest_data()
        self.df = pd.read_csv(self.__path_to_file())
        return

    def __file_exists(self) -> str:
        '''Check the availability of a file, storing its "last checked" timestamp if present'''

        for _, _, files in os.walk(self.__path_to_dir()):
            for file in files:
                res = re.match(f'^{self.file_label}-(\d+).{self.ext}', file)
                if res:
                    self.file_timestamp = res.group(1)
                    return True
        return False

    def __path_to_dir(self):
        '''Return the absolute path to the application's data store or one of its sub-directories, creating it if non-existent'''

        root_dir = Path(__file__).parent
        dir_path = os.path.join(root_dir, self.dir, self.tag)
        try:
            os.mkdir(dir_path)
        except FileExistsError:
            pass
        return dir_path

    def __is_updated(self):
        '''Check whether a file needs to be modified to include more recent data'''

        if delta_in_days(self.file_timestamp) <= self.delta:
            return True
        return False

    def __rename_file(self):
        '''Update the UNIX timestamp of a file to the present time'''

        old_filename = self.__path_to_file()
        self.file_timestamp = None
        new_filename = self.__path_to_file()
        os.rename(old_filename, new_filename)
        return

    def __path_to_file(self):
        '''Construct the absolute path of a file to be stored within the application's data store'''

        root_dir = Path(__file__).parent
        if self.file_timestamp is None:
            self.file_timestamp = str(int(datetime.now().timestamp()))
        return os.path.join(root_dir, self.dir, self.tag, f'{self.file_label}-{self.file_timestamp}.{self.ext}')

    # def __save_latest_data(self):
    #     '''Append new data to a file'''

    #     filename = self.__path_to_file()
    #     with open(filename, 'a+') as file:
    #         writer = csv.writer(file, quoting=csv.QUOTE_ALL)
    #         if os.stat(filename).st_size == 0:
    #             writer.writerow(self.file_header)
    #         for args in self.generator():
    #             writer.writerow(args)
    #     return

class Proxies:

    def __init__(self, pool_size=50):
        self.log = get_logger(__name__)
        # self.useragent_faker = UserAgent()
        self.proxy_url_pool = itertools.cycle(['https://sslproxies.org/', 'https://free-proxy-list.net/'])
        self.proxies_url = next(self.proxy_url_pool)
        self.ip_finder_url = 'https://api.ipify.org'
        self.network_ip = requests.get(self.ip_finder_url).text
        self.n = pool_size
        self.proxies = self.generate_proxy_pool(self.n)
        self.proxy_pool = itertools.cycle(self.proxies)
        self.counter = 0

    def generate_proxy_pool(self, n):
        response = requests.get(self.proxies_url)
        parser = html.fromstring(response.text)
        proxies = []
        for i in parser.xpath('//tbody/tr')[:n]:
            if i.xpath('.//td[7][contains(text(),"yes")]'):  # copy only https proxies
                proxy = ':'.join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.append(proxy)
        return proxies

    def update_pool(self):
        self.counter += 1
        if self.counter % 50 == 0:
            self.proxies_url = next(self.proxy_url_pool)
            self.log.info(f'Changing proxy list to: {self.proxies_url}')
            self.proxies = self.generate_proxy_pool(self.n)
            self.proxy_pool = itertools.cycle(self.proxies)

    def get_proxy(self):
        while True:
            test_proxy = next(self.proxy_pool)
            self.log.info(f'Proxy currently in use: {test_proxy}')
            try:
                requests.get(
                    self.ip_finder_url,
                    proxies={'https': f'https://{test_proxy}', 'http': f'http://{test_proxy}'},
                    timeout=5,
                    verify=False,
                    allow_redirects=False).text
                return test_proxy
            except (ProxyError, SSLError, urllib3.exceptions.MaxRetryError):
                self.log.info('Proxy error. Choosing a new proxy.')
                self.update_pool()
                continue
            except (ConnectTimeout, ReadTimeout):
                self.log.info('Timeout error. Choosing a new proxy.')
                self.update_pool()
                continue

    def create_session(self, url, headers):
        while True:
            self.proxy = self.get_proxy()
            self.session = Session()
            self.session.headers.update(headers)
            self.session.headers.update({'User-Agent': self.useragent_faker.random})
            self.session.proxies.update({'https': f'https://{self.proxy}', 'http': f'http://{self.proxy}'})
            try:
                r = self.session.get(
                    url,
                    timeout=10,
                    verify=False,
                    allow_redirects=False
                )
                assert r.status_code == 200
                self.log.info('Session created successfully.')
                return self.session
            except (ProxyError, urllib3.exceptions.MaxRetryError):
                self.log.info('Proxy error. Creating a new session.')
                continue
            except SSLError:
                self.log.info('SSL verification error. Creating a new session.')
                continue
            except (ConnectTimeout, urllib3.exceptions.ConnectTimeoutError, ReadTimeout):
                self.log.info('Timeout error. Creating a new session.')
                continue
            except AssertionError:
                self.log.info('Returned an unsuccessful response. Creating a new session.')
                continue


def feed_template(feed: Callable,):
    for crawler in feed:
        crawler.execute()


def get_logger(name):
    '''Print log info to console while saving to disk'''

    log_format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt='%m-%d %H:%M',
        filename='cctraderbot.log',
        filemode='a'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger(name).addHandler(console)
    return logging.getLogger(name)


def datetime_from_midnight(dt=None, timezone='UTC', day_offset=None):
    '''Return a datetime object for a given date at midnight'''

    if dt is None:
        dt = datetime.now()
    elif not isinstance(dt, date):
        dt = datetime.utcfromtimestamp(int(dt))
    tz = pytz.timezone(timezone)
    dt = tz.localize(dt.replace(minute=0, hour=0, second=0, microsecond=0, tzinfo=None))
    if day_offset:
        return (dt - timedelta(days=day_offset))
    return dt


def delta_in_days(timestamp: str | int | datetime, reference: str | int | datetime = None):
    '''Evaluate how recent a timestamp is relative to a reference date, or to the present time'''

    if not isinstance(timestamp, date):
        timestamp = datetime.utcfromtimestamp(int(timestamp))
    if reference is None:
        reference = datetime.now()
    else:
        if not isinstance(reference, date):
            reference = datetime.utcfromtimestamp(int(reference))
    return abs((timestamp - reference).days)


# def file_search(tag: str, ticker: str):
#     '''Check the availability of certain ticker data'''

#     for _, _, files in os.walk(path_to_file(filename='')):
#         for file in files:
#             res = re.match(f'^{tag}-{ticker}-(\d+).csv', file)
#             if res:
#                 return res.group(1)
#     return None

def path_to_file(filename: str = '', subdir: str = ''):
    '''Return the absolute path for the application's data directory'''

    root_dir = Path(__file__).parent
    return os.path.join(root_dir, 'data', subdir, filename)


# def file_search(tag: str, ticker: str):
#     '''Check the availability of certain ticker data and whether it is up-to-date'''

#     dt = datetime.now()
#     file_timestamp = str(int(dt.timestamp()))
#     is_recent = False
#     for _, _, files in os.walk(path_to_file(filename='')):
#         for file in files:
#             res = re.match(f'^{tag}-{ticker}-(\d+).csv', file)
#             if res:
#                 file_timestamp = res.group(1)
#                 file_dt = datetime.utcfromtimestamp(int(file_timestamp))
#                 is_recent = True if (dt - file_dt).days < 30 else False
#                 break
#         break
#     return file_timestamp, is_recent


def market_status():
    '''Return the current market status'''

    response = requests.get(url='https://finance.yahoo.com/', params=None, timeout=10)
    tree = html.fromstring(response.content)
    message = tree.xpath('//span[@data-id="mk-msg"]/text()')
    if 'open in' in message[0]:
        return 'is_opening'
    elif 'close in' in message[0]:
        return 'open'
    elif (not message) or ('closed' in message[0]):
        return 'closed'
    else:
        return 'open'


def flatten_dict(input_dict: dict) -> str:
    '''Convert a nested dictionary into a string'''

    output_dict = {}
    def flatten(d, parent=[]):
        for k, v in d.items():
            if type(v) is dict:
                parent.append(k)
                flatten(v, parent)
                parent = []
            else:
                if len(parent) > 1:
                    k = parent[0] + ''.join([f'[{i}]' for i in parent[1:]]) + f'[{k}]'
                elif len(parent) == 1:
                    k = f'{parent[0]}[{k}]'
                output_dict[k] = v
    flatten(input_dict)
    return output_dict

def text_to_num(text):
    '''Convert an abbreviated number to the full number'''

    switcher = {
        'K': 1000,
        'M': 1000000,
        'B': 1000000000,
        'T': 1000000000000
    }
    if not re.search(r'\d+(?:\.\d+)?', text):
        return None
    value = re.findall(r'\d+(?:\.\d+)?', text)[0]
    unit = re.findall(r'[a-zA-Z]', text)[0]
    return int(float(value) * switcher.get(unit, 1))


def previous_and_next(iterable):
    '''Access previous and next values in a loop, based on @nosklo's response to StackOverflow #1011938'''

    previous_item, item, next_item = tee(iterable, 3)
    previous_item = chain([None], previous_item)
    next_item = chain(islice(next_item, 1, None), [None])
    return zip(previous_item, item, next_item)


def extract_value(fragment, xpath):
    '''Extract data from an HTML fragment using an XPath expression'''

    container = fragment.xpath(xpath)
    if container:
        return container[0].text
    return None

