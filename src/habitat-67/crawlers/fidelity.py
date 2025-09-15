from utils import SessionHandler

class Fidelity(SessionHandler):

    def __init__(self):
        super(Fidelity, self).__init__()
        self.call = self.call_initialiser(calls=1, period=3)
    
    def get_market_classification(self):
        return

    def get_ex_date_calendar(self):
        return

    def get_split_calendar(self):
        return
