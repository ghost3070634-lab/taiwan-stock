from __future__ import annotations

import os
import requests
from datetime import datetime, timedelta, date as date_type
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

from config import INDUSTRY_CONFIG


@dataclass
class IndustryStat:
    industry: str
    total_turnover: float
    avg_return: float
    leading_stock_count_in_top20: int
    score: float


def load_stock_universe() -> pd.DataFrame:
    """
    回傳含有 `stock_id`, `stock_name`, `industry` 欄位的股票清單。

    先以簡單靜態表為主：
    - 若 `data/stock_universe.csv` 存在，優先讀取該檔。
    - 否則回傳程式內建的少量示意資料，方便之後再擴充。
    """
    from pathlib import Path

    base_dir = Path(__file__).resolve().parent
    csv_path = base_dir / "stock_universe.csv"

    if csv_path.exists():
        df = pd.read_csv(csv_path, dtype={"stock_id": str})
        expected_cols = {"stock_id", "stock_name", "industry"}
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"stock_universe.csv 缺少欄位: {missing}")
        # 確保欄位順序與名稱統一
        return df[["stock_id", "stock_name", "industry"]]

    data = [
        {"stock_id": "2330", "stock_name": "台積電", "industry": "半導體"},
        {"stock_id": "2317", "stock_name": "鴻海", "industry": "電子代工"},
        {"stock_id": "2603", "stock_name": "長榮", "industry": "航運"},
        {"stock_id": "2412", "stock_name": "中華電", "industry": "通信網路"},
        {"stock_id": "2882", "stock_name": "國泰金", "industry": "金融"},
    ]
    return pd.DataFrame(data, columns=["stock_id", "stock_name", "industry"])


def _fetch_daily_price_for_universe(target_date: str) -> pd.DataFrame:
    """
    使用政府 OpenAPI 獲取全市場「最新一個交易日」的資料。
    包含上市 (TWSE) 與 上櫃 (TPEx)。
    """
    print(f"[{target_date}] 準備透過 OpenAPI 抓取全市場最新報價...")
    
    # 1. 抓取上市股票 (TWSE)
    try:
        twse_url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp_twse = requests.get(twse_url, timeout=15)
        resp_twse.raise_for_status()
        df_twse = pd.DataFrame(resp_twse.json())
        # 映射上市 OpenAPI 的欄位名稱
        df_twse = df_twse.rename(columns={
            "Code": "stock_id",
            "OpeningPrice": "open",
            "ClosingPrice": "close",
            "TradeValue": "turnover"
        })
    except Exception as e:
        print(f"上市資料抓取失敗: {e}")
        df_twse = pd.DataFrame()

    # 2. 抓取上櫃股票 (TPEx)
    try:
        tpex_url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
        resp_tpex = requests.get(tpex_url, timeout=15)
        resp_tpex.raise_for_status()
        df_tpex = pd.DataFrame(resp_tpex.json())
        # 映射上櫃 OpenAPI 的欄位名稱
        df_tpex = df_tpex.rename(columns={
            "SecuritiesCompanyCode": "stock_id",
            "Open": "open",
            "Close": "close",
            "TradingAmount": "turnover"
        })
    except Exception as e:
        print(f"上櫃資料抓取失敗: {e}")
        df_tpex = pd.DataFrame()

    # 3. 合併上市與上櫃資料
    df_all = pd.concat([df_twse, df_tpex], ignore_index=True)
    
    if df_all.empty:
        raise RuntimeError("無法從政府 OpenAPI 獲取任何報價資料！")

    # 4. 資料清理與型別轉換
    df_all["stock_id"] = df_all["stock_id"].astype(str)
    
    # 轉為數值 (遇到 '--' 或空字串會強制轉為 NaN)
    df_all["open"] = pd.to_numeric(df_all["open"], errors="coerce")
    df_all["close"] = pd.to_numeric(df_all["close"], errors="coerce")
    df_all["turnover"] = pd.to_numeric(df_all["turnover"], errors="coerce").fillna(0.0)

    # 計算單日漲跌幅: (收盤 - 開盤) / 開盤
    df_all["daily_return"] = (df_all["close"] - df_all["open"]) / df_all["open"].replace(0, pd.NA)

    # 標記日期 (OpenAPI 不帶具體日期欄位，以 target_date 作為輸出標記)
    df_all["date"] = target_date

    # 挑出需要的欄位並過濾掉沒有收盤價的無效資料
    out = df_all[["date", "stock_id", "close", "turnover", "daily_return"]].dropna(subset=["stock_id", "close"])

    print(f"✅ 成功獲取 {len(out)} 檔股票的最新資料！")
    return out.reset_index(drop=True)
    
def _taipei_today() -> date_type:
    """以台灣時區取得今天日期。"""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).date()
    except Exception:
        return date_type.today()


def find_nearest_trading_date_with_data(start_date: date_type, max_days_back: int = 20) -> Tuple[date_type, str]:
    """
    因為 OpenAPI 永遠會提供「最新一個交易日」的全市場資料。
    所以不需要再浪費時間往前遞迴測試，直接回傳預期的計算基準日即可。
    """
    ds = start_date.strftime("%Y-%m-%d")
    return start_date, ds


def get_weekly_push_target_date() -> Tuple[date_type, str]:
    """
    週報推波日期：
    - 以台灣時間的「最近一個週五」為基準
    """
    today = _taipei_today()
    # Monday=0 ... Friday=4
    days_since_friday = (today.weekday() - 4) % 7
    base_friday = today if days_since_friday == 0 else today - timedelta(days=days_since_friday)
    
    # 直接使用 OpenAPI 邏輯，回傳基準日
    return find_nearest_trading_date_with_data(base_friday, max_days_back=20)

def find_nearest_trading_date_with_data(start_date: date_type, max_days_back: int = 20) -> Tuple[date_type, str]:
    """
    因為 OpenAPI 永遠會提供「最新一個交易日」的全市場資料。
    所以不需要再浪費時間往前遞迴測試，直接回傳預期的計算基準日即可。
    """
    ds = start_date.strftime("%Y-%m-%d")
    return start_date, ds

def get_weekly_push_target_date() -> Tuple[date_type, str]:
    """
    週報推波日期：
    - 以台灣時間的「最近一個週五」為基準
    - 若週五無資料，就找週四、週三…最多回推 20 天
    """
    today = _taipei_today()
    # Monday=0 ... Friday=4
    days_since_friday = (today.weekday() - 4) % 7
    base_friday = today if days_since_friday == 0 else today - timedelta(days=days_since_friday)
    return find_nearest_trading_date_with_data(base_friday, max_days_back=20)

def compute_industry_stats(target_date: str) -> pd.DataFrame:
    """
    計算每個產業在指定日期的統計資訊：
    - 總成交金額 (total_turnover)
    - 平均漲跌幅 (avg_return)
    - 在「當日漲幅前 N 名」中出現的檔數 (leading_stock_count_in_top20)
    """
    universe = load_stock_universe()
    price_df = _fetch_daily_price_for_universe(target_date)

    merged = price_df.merge(universe, on="stock_id", how="left")
    merged = merged.dropna(subset=["industry"])

    if merged.empty:
        raise RuntimeError("股價資料與產業對應後為空，請確認 stock_universe 內容是否正確。")

    # 計算每個產業的基礎統計
    grouped = merged.groupby("industry", as_index=False)
    stats = grouped.agg(
        total_turnover=("turnover", "sum"),
        avg_return=("daily_return", "mean"),
        stock_count=("stock_id", "nunique"),
    )

    # 計算「當日漲幅前 N 名」中的領漲檔數
    rank_n = INDUSTRY_CONFIG.leading_rank_window
    top_n = (
        merged.sort_values("daily_return", ascending=False)
        .head(rank_n)
        .groupby("industry")["stock_id"]
        .nunique()
        .rename("leading_stock_count_in_top20")
        .reset_index()
    )

    stats = stats.merge(top_n, on="industry", how="left").fillna(
        {"leading_stock_count_in_top20": 0}
    )

    # 根據三個指標計算綜合分數（皆轉成 0~1 的排名分數後加總）
    def _rank_normalize(series: pd.Series, ascending: bool) -> pd.Series:
        if series.nunique() <= 1:
            return pd.Series(0.5, index=series.index)
        ranks = series.rank(method="min", ascending=ascending)
        return (ranks - 1) / (len(series) - 1)

    score_turnover = _rank_normalize(stats["total_turnover"], ascending=True)
    score_return = _rank_normalize(stats["avg_return"], ascending=True)
    score_leading = _rank_normalize(
        stats["leading_stock_count_in_top20"], ascending=True
    )

    stats["score"] = score_turnover + score_return + score_leading

    return stats.sort_values("score", ascending=False).reset_index(drop=True)


def pick_top_industries(
    target_date: str,
    top_n: Optional[int] = None,
) -> Tuple[List[str], pd.DataFrame]:
    """
    綜合產業統計指標後挑出主流產業清單。

    回傳:
    - leading_industries: 依照 score 排序後的前 N 名產業名稱清單
    - stats: 含有所有產業統計與分數的 DataFrame（已依 score 由高到低排序）
    """
    if top_n is None:
        top_n = INDUSTRY_CONFIG.top_industry_count

    stats = compute_industry_stats(target_date)

    # 先依最小領漲檔數門檻過濾，再取前 N 名
    filtered = stats[
        stats["leading_stock_count_in_top20"]
        >= INDUSTRY_CONFIG.min_leading_stock_count_in_top20
    ]
    if filtered.empty:
        # 若嚴格條件下沒有任何產業通過，則退而求其次：直接取分數最高的前 N 名
        filtered = stats

    top = filtered.head(top_n)
    industries = top["industry"].tolist()
    return industries, stats
