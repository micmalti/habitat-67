from utils import SessionHandler

class Nasdaq(SessionHandler):

    def __init__(self):
        super(Nasdaq, self).__init__()
        self.call = self.call_initialiser(calls=1, period=3)
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.nasdaq.com/',
            'Origin': 'https://www.nasdaq.com',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
        })
        self.base_url = 'https://api.nasdaq.com/api'

    def get_earnings_calendar(self):
        return

    def get_ex_date_calendar(self):
        return
    
    def get_market_classification(self):
        return