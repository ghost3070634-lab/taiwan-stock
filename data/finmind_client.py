import os
from datetime import datetime, timedelta

import pandas as pd
from FinMind.data import DataLoader


class FinMindClient:
    def __init__(self):
        token = os.getenv("FINMIND_API_TOKEN", "")
        self.dl = DataLoader()
        if token:
            self.dl.login_by_token(api_token=token)

    def get_index_daily(self, index_id: str, days: int = 120) -> pd.DataFrame:
        end = datetime.today().date()
        start = end - timedelta(days=days * 2)
        df = self.dl.taiwan_stock_index(
            index_id=index_id,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
        return df.tail(days).reset_index(drop=True)

    def get_stock_daily(self, stock_id: str, days: int = 120) -> pd.DataFrame:
        end = datetime.today().date()
        start = end - timedelta(days=days * 2)
        df = self.dl.taiwan_stock_daily(
            stock_id=stock_id,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
        return df.tail(days).reset_index(drop=True)

    def get_stock_month_revenue(self, stock_id: str, months: int = 12) -> pd.DataFrame:
        df = self.dl.taiwan_stock_month_revenue(stock_id=stock_id)
        return df.tail(months).reset_index(drop=True)

    def get_stock_institutional_investors(
        self,
        stock_id: str,
        days: int = 30,
    ) -> pd.DataFrame:
        end = datetime.today().date()
        start = end - timedelta(days=days * 2)
        df = self.dl.taiwan_stock_institutional_investors(
            stock_id=stock_id,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
        # 只留投信
        df = df[df["name"] == "Investment_Trust"]
        return df.tail(days).reset_index(drop=True)

    def get_stock_margin(self, stock_id: str, days: int = 60) -> pd.DataFrame:
        end = datetime.today().date()
        start = end - timedelta(days=days * 2)
        df = self.dl.taiwan_stock_margin_purchase_short_sale(
            stock_id=stock_id,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
        return df.tail(days).reset_index(drop=True)
