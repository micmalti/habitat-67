"""Another ok ghest"""

from requests import Session
import json
import pandas as pd

class TradingAccount:
  """A wrapper for Moneybase API"""
  def __init__(self, config: dict):
    self.session = Session()
    self.base_url = 'https://preapi.moneybase.com/authentication/v2'   #https://prelive.cctrader.com
    self.config = config.moneybase
    self.login()
    self.get_user_access()

  def login(self):
    headers = {
      'application-id': self.config["APP_ID"],
      'client-public-id': self.config["PUBLIC_ID"],
      'client-secret-key': self.config["SECRET_KEY"],
      'sub-application-name': 'CCTrader',
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }
    payload = json.dumps({
      "grantType": "pin",
      "mobile": self.config["MOBILE"],
      "pin": self.config["PIN"],
      "deviceCode": ""
    })
    response = self.session.request('POST', f'{self.base_url}/token', headers=headers, data=payload)
    print(json.dumps(response, indent=4, sort_keys=True))
    self.access_token = json.loads(response.text)['access_token']  # takes a string and produces an object (to read a json file, use load)
    self.refresh_token = json.loads(response.text)['refresh_token']
    return

  def register_device(self):
    self.session.headers.update({'Authorization': f'Bearer {self.access_token}'})
    response = self.session.request('GET', f'{self.base_url}/device/otp')
    payload = json.dumps({
      "otpValue": json.loads(response.text)["otpValue"],
      "otpPublicId": json.loads(response.text)["otpPublicId"]
    })
    response = self.session.request('POST', f'{self.base_url}/device', data=payload)
    return json.loads(response.text)['deviceCode']

  def get_user_access(self):
    response = self.session.request('GET', f'{self.base_url}/identities/subidentities')
    payload = json.dumps({
      "externalReference": json.loads(response.text)['data']['externalReference'],
      "deviceCode": self.register_device()
    }) # takes an object and produces a string (to write to a file, use dump)
    response = self.session.request('PUT', f'{self.base_url}/identities/subidentities', data=payload)
    self.access_token = json.loads(response.text)['access_token']
    self.refresh_token = json.loads(response.text)['refresh_token']
    return

  def generate_access_token(self):
    payload = json.dumps({
      "grantType": "refreshToken",
      "refresh_token": self.refresh_token
    })
    response = self.session.request('POST', f'{self.base_url}/token', data=payload)
    self.access_token = json.loads(response.text)['access_token']
    return

  def get_cash_balance(self):
    response = self.session.request('GET', f'{self.base_url}/client/balance')
    for currency in json.loads(response.text)['Accounts']:
      if currency['Currency'] == "USD":
        return currency['AmountAvailable']

  def get_portfolio(self):
    params = json.dumps({
      "properties": [
        "TotalValue",
        "InstrumentCode",
        "InstrumentTicker",
        "Quantity",
        "AverageWeightedPurchasePrice",
        "MarketValueInstrumentCurrency",
        "PriceChangePercentage"
      ],
      "valuationCurrencyCode": "USD"
    })
    response = self.session.request('GET', f'{self.base_url}/portfolio', params=params)
    portfolio_data = json.loads(response.text)
    portfolio_value = portfolio_data['TotalValue']
    portfolio = pd.DataFrame(data=portfolio_data['Holdings'])
    return portfolio_value, portfolio

  def get_active_orders(self, active_order_list=[], page=1):
      response = self.session.request('GET', f'{self.base_url}/order/active', params={'page': page})
      active_orders = json.loads(response.text)['Orders']
      active_order_list.extend(active_orders)
      if len(active_orders) < json.loads(response.text)['Total']:
        page += 1
        self.get_active_orders(active_order_list, page)
      else:
        return active_order_list

  def get_stock_id(self, ticker):
    params = json.dumps({
      "name": ticker,
      "includeTickSize": 0,
      "includePercentageChange": 0,
      "types": [2, 4],
      "size": 5
    })
    response = self.session.request('GET', f'{self.base_url}/instrument/search', params=params)
    for stock in json.loads(response.text):
      if stock['Symbol'] == ticker:
        return stock['Id']

  def get_commission(self, stock_id, buy, quantity, price):
    payload = json.dumps({
      "IsBuy": buy,
      "InstrumentId": stock_id,
      "Quantity": quantity,
      "Price": price
    })
    response = self.session.request('POST', f'{self.base_url}/order/estimate', data=payload)
    commission_info = json.loads(response.text)['TradeTotal']['Commission']
    accrued_interest = commission_info['InterestAccrued']
    total_commission = commission_info['Amount']
    exchange_commission = commission_info['ExchangeCommission']
    brokerage_commission = commission_info['BrokerageCommission']
    commission_breakdown = [
      exchange_commission['StampDuty'],
      exchange_commission['PTMLevy'],
      exchange_commission['ExchangeCharge'],
      exchange_commission['ExchangeVolumeFee'],
      brokerage_commission['Amount']
    ]
    return total_commission, commission_breakdown, accrued_interest

  def place_order(self, buy, stock_id, quantity, price):
    payload = json.dumps({
      "IsBuy": buy,
      "InstrumentId": stock_id,
      "HoldingType": 1,
      "Quantity": quantity,
      "ExecutionType": 1,
      "Price": price,
      "ValidityType": 0,
      "DividendPayment": 1,
      "DividendCurrency": "USD",
      "DeductWitholdingTax": False,
      "SettlementCurrencyOrder": ["USD"]
    })
    response = self.session.request('POST', f'{self.base_url}/order/place', data=payload)
    return json.loads(response.text)['OrderId'], json.loads(response.text)['Succeeded']

  def modify_order(self, order_id, quantity, price):
    payload = json.dumps({
      "Id": order_id,
      "Quantity": quantity,
      "ExecutionType": 1,
      "Price": price,
      "ValidityType": 0,
      "DividendPayment": 1,
      "DividendCurrency": "USD",
      "DeductWitholdingTax": False,
      "SettlementCurrencyOrder": ["USD"]
    })
    response = self.session.request('POST', f'{self.base_url}/order/update', data=payload)
    return json.loads(response.text)['Succeeded']

  def cancel_order(self, order_id):
    response = self.session.request('GET', f'{self.base_url}/order/cancel/{order_id}')
    return json.loads(response.text)['Succeeded']

  def logout(self):
    self.session.request('DELETE', f'{self.base_url}/sessions')
    return
