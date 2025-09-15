"""Hey"""

from pathlib import Path
from dataclasses import dataclass
import json
# from libs.moneybase.moneybase.moneybase import TradingAccount
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

@dataclass
class Moneybase:
    APP_ID: str
    PUBLIC_ID: str
    SECRET_KEY: str
    MOBILE: str
    PIN: str

@dataclass(frozen=True)
class Config:
  moneybase: Moneybase

def load_config() -> Config:
    config_file = Path.cwd() / "config.json"
    config_dict = json.loads(config_file.read_text())
    return Config(**config_dict)

if __name__ == '__main__':
    config = load_config()
    # TradingAccount(config=config)
