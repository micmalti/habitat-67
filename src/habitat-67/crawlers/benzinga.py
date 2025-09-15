class Benzinga(SessionHandler):

    def __init__(self):
        super(Benzinga, self).__init__()
        self.call = self.call_initialiser(calls=1, period=3)
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*'
            # 'Origin': 'https://www.benzinga.com',
            # 'Referer': 'https://www.benzinga.com/',
            # 'Sec-Fetch-Dest': 'empty',
            # 'Sec-Fetch-Mode': 'cors',
            # 'Sec-Fetch-Site': 'same-site'
        })

    def get_earnings_calendar(self, start_date: datetime = None, end_date: datetime = None):
        '''Retrieve company earnings dates'''

        df = pd.DataFrame()
        nyse_schedule = market_schedule(start_date, end_date, exchange='NYSE')
        correction = timedelta(days=1)
        start_date = f'{(start_date - correction):%Y-%m-%d}'
        end_date = f'{end_date:%Y-%m-%d}'
        for chunk in self.__get_reporting_companies(start_date, end_date, schedule=nyse_schedule):
            df.merge(chunk)
        # df.drop_duplicates(keep='last', inplace=True)
        return df


    def __get_reporting_companies(self, start_date: str, end_date: str, schedule: pd.DataFrame, counter=0):
        '''Fetch earnings dates from the Benzinga API which auto-limits results to 1000 entries'''

        params = {
            'token': config.BENZINGA_API_KEY,
            'parameters': {
                'date_from': start_date,
                'date_to': end_date,
            },
            'pagesize': '1000'
        }
        data = self.call(
            url='https://api.benzinga.com/api/v2.1/calendar/earnings',
            params=flatten_dict(params)
        )
        df = pd.json_normalize(data, record_path =['earnings'])
        df['release_time'] = df[['date', 'release_time']].apply(self.__get_release_time, schedule=schedule, axis=1)
        if df['date'].min() != start_date:
            yield df
            end_date = df['date'].min()
            counter += 1
            self.__get_reporting_companies(start_date, end_date, schedule, counter)
        else:
            df = df[df['date'] != df['date'].min()]
            yield df

    def __get_release_time(self, date, release_time, schedule: pd.DataFrame):
        '''Return the release time of stock earnings relative to market trading hours'''

        if pd.Timestamp(date) in schedule.index:
            tz = pendulum.timezone('America/New_York')
            market_open = schedule.loc[pd.Timestamp(date)]['market_open']
            market_close = schedule.loc[pd.Timestamp(date)]['market_close']
            try:
                earnings_call = tz.convert(datetime.strptime(date + release_time, '%Y-%m-%d%X'))
                if earnings_call < market_open:
                    return 'PM'
                elif earnings_call >= market_close:
                    return 'AH'
                else:
                    return 'MO'
            except ValueError:
                # self.log.info(f"Error on {stock['date']} {stock['release_time']} for {stock['ticker']}")
                return None
        else:
            return 'AH'


class Benzinga(SessionHandler):

    def __init__(self):
        super(Benzinga, self).__init__()
        self.call = self.call_initialiser(calls=1, period=3)
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*'
            # 'Origin': 'https://www.benzinga.com',
            # 'Referer': 'https://www.benzinga.com/',
            # 'Sec-Fetch-Dest': 'empty',
            # 'Sec-Fetch-Mode': 'cors',
            # 'Sec-Fetch-Site': 'same-site'
        })


    def get_recommendations(self, ticker: str = '', wanted_date: datetime = datetime.now()):
        '''Fetch analyst recommendations and price targets'''

        params = {
            'token': '',
            'parameters': {
                'date_from': f'{datetime.now():%Y-%m-%d}',
                'date_to': f'{datetime.now():%Y-%m-%d}',
                'tickers': ticker
            },
            'pagesize': 1000
        }
        data = self.call(
            url='https://api.benzinga.com/api/v2.1/calendar/ratings',
            params=flatten_dict(params),
            response_type='json'
        )
        df = pd.json_normalize(data, record_path =['ratings'])
        # print(df.head())
        return df
