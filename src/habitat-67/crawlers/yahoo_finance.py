from datetime import datetime, timedelta
from utils import SessionHandler
import pandas as pd
from lxml import html

class YahooFinance(SessionHandler):
    """
    Constructs and sends a :class:`Request <Request>`.

    :param method: method for the new :class:`Request` object: ``GET``, ``OPTIONS``, ``HEAD``, ``POST``, ``PUT``, ``PATCH``, or ``DELETE``.
    :param url: URL for the new :class:`Request` object.
    :param params: (optional) Dictionary, list of tuples or bytes to send
    in the query string for the :class:`Request`.
    :param data: (optional) Dictionary, list of tuples, bytes, or file-like
    object to send in the body of the :class:`Request`.
    :param json: (optional) A JSON serializable Python object to send in the body of the :class:`Request`.
    :param headers: (optional) Dictionary of HTTP Headers to send with the :class:`Request`.
    :param cookies: (optional) Dict or CookieJar object to send with the :class:`Request`.
    :param files: (optional) Dictionary of ``'name': file-like-objects`` (or ``{'name': file-tuple}``) for multipart encoding upload.
    ``file-tuple`` can be a 2-tuple ``('filename', fileobj)``, 3-tuple ``('filename', fileobj, 'content_type')``
    or a 4-tuple ``('filename', fileobj, 'content_type', custom_headers)``, where ``'content_type'`` is a string
    defining the content type of the given file and ``custom_headers`` a dict-like object containing additional headers
    to add for the file.
    :param auth: (optional) Auth tuple to enable Basic/Digest/Custom HTTP Auth.
    :param timeout: (optional) How many seconds to wait for the server to send data
    before giving up, as a float, or a :ref:`(connect timeout, read
    timeout) <timeouts>` tuple.
    :type timeout: float or tuple
    :param allow_redirects: (optional) Boolean. Enable/disable GET/OPTIONS/POST/PUT/PATCH/DELETE/HEAD redirection. Defaults to ``True``.
    :type allow_redirects: bool
    :param proxies: (optional) Dictionary mapping protocol to the URL of the proxy.
    :param verify: (optional) Either a boolean, in which case it controls whether we verify
        the server's TLS certificate, or a string, in which case it must be a path
        to a CA bundle to use. Defaults to ``True``.
    :param stream: (optional) if ``False``, the response content will be immediately downloaded.
    :param cert: (optional) if String, path to ssl client cert file (.pem). If Tuple, ('cert', 'key') pair.
    :return: :class:`Response <Response>` object
    :rtype: requests.Response

    Usage::

    >>> import requests
    >>> req = requests.request('GET', 'https://httpbin.org/get')
    >>> req
    <Response [200]>
    """

    def __init__(self):
        super(YahooFinance, self).__init__()
        self.call = self.call_initialiser(calls=1, period=3)
        self.__popup_handler()


    def get_earnings_calendar(self, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        '''Retrieve company earnings dates (DEPRECATED)'''

        earnings_dates = list()
        day_offset = 0
        day_counter = abs((end_date - start_date).days)
        while day_counter != day_offset:
            wanted_date = start_date + timedelta(days=day_offset)
            for reporting_company in self.__get_reporting_companies(wanted_date):
                earnings_dates.extend(reporting_company)
            day_offset += 1
        df = pd.DataFrame(earnings_dates)
        return df


    def get_split_calendar(self) -> pd.DataFrame:
        return

    def get_historical_prices(self, ticker: str, start_date: str, end_date: str):
        '''Retrieve split-adjusted, historical price data'''

        def add_event(df, event_type):
            is_event = price_data['events'].get(event_type, None)
            if is_event:
                event_dict = {}
                for _, dict in is_event.items():
                    amount, ex_date = dict.values()
                    event_dict[ex_date] = amount
                df[event_type] = df['date'].map(event_dict)
            else:
                df[event_type] = np.nan

        base_url = 'https://iquery.finance.yahoo.com/v8/finance/chart/'  # query1 uses HTTP/1.0 while query2 uses HTTP/1.1
        url = f'{base_url}{ticker}'
        params = {
            'period1': int(start_date),
            'period2': int(end_date),
            'interval': '1d',
            'includePrePost': False,
            'events': 'div,splits'
        }
        response = self.get_request(url=url, params=params, response_type='json', calls=1 , period=4)
        price_data = response['chart']['result'][0]
        df = pd.DataFrame(price_data['indicators']['quote'][0])
        df['date'] = np.array(price_data['timestamp'])
        for event_type in ['dividends', 'splits']:
            add_event(df, event_type)
        df['date'] = pd.to_datetime(df['date'], unit='s').dt.strftime('%Y-%m-%d')
        df.set_index('date', inplace=True)
        df.to_csv(path_to_file('yahoo_finance.csv'), index_label=False)
        return df


    def __popup_handler(self) -> None:
        '''Bypass the EU consent form to use Yahoo Finance'''

        self.session.headers.update({
            'Cache-Control': 'max-age=0',
            'Origin': 'https://consent.yahoo.com',
            'Sec-GPC': '1',
            'Sec-Fetch-Site': 'same-origin'
        })
        response = self.session.get('https://finance.yahoo.com')
        tree = html.fromstring(response.content)
        payload = {
            'csrfToken': tree.xpath('//input[@name="csrfToken"]/@value')[0],
            'sessionId': tree.xpath('//input[@name="sessionId"]/@value')[0],
            'namespace': 'yahoo',
            'agree': ['agree', 'agree'],
        }
        self.session.post('https://consent.yahoo.com/v2/collectConsent', data=payload)


    def __get_reporting_companies(self, date: datetime, offset=0) -> dict:
        '''Fetch earnings dates from Yahoo Finance which limits results to 100 per page'''

        while True:
            page = self.call(
                url='https://finance.yahoo.com/calendar/earnings/',
                params={
                    'day': date.strftime('%Y-%m-%d'),
                    'offset': offset,
                    'size': 100
                }
            )
            if not page.xpath('//tbody'):
                return
            else:
                for row in page.xpath('//tbody/tr[contains(@class,"simpTblRow")]'):
                    reporting_data = {
                        'date': date.strftime('%Y-%m-%d'),
                        'ticker': row.xpath('./td/a/text()')[0],
                        'release_time': row.xpath('./td[@aria-label="Earnings Call Time"]')[0].text_content(),
                        'watched': row.xpath('./td[@aria-label="EPS Estimate"]')[0].text_content()
                    }
                    reporting_data['release_time'] = self.__get_release_time(reporting_data)
                    reporting_data['watched'] = 0 if reporting_data['watched'] == '-' else 1
                    yield reporting_data
                offset += 100

    def __get_release_time(self, reporting_data: dict) -> str:
        '''Return the release time of stock earnings relative to market trading hours'''

        switcher = {
            'Time Not Supplied': None,
            'TAS': None,
            'Before Market Open': 'PM',
            'After Market Close': 'AH'
        }
        mapped = switcher.get(reporting_data['release_time'], None)
        # if not mapped:
            # self.log.info(f"Error on {reporting_data['date']} for {reporting_data['ticker']}")
        return mapped
