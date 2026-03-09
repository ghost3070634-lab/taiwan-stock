from __future__ import annotations

import os
import requests

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
    抓「全市場」(全部台股) 指定日期的日資料（調整後股價）。
    若該日休市/無資料，回傳空 DataFrame 讓上層決定是否回推日期。
    """
    token = os.getenv("FINMIND_API_TOKEN", "")
    params = {
        "dataset": "TaiwanStockPriceAdj",
        "start_date": target_date,
        "end_date": target_date,
    }
    if token:
        params["token"] = token

    resp = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    data = payload.get("data")
    if not data:
        # 沒開盤/休市/無資料
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if df.empty:
        return df

    # 欄位標準化（不同資料集/版本欄位可能大小寫不同）
    df.columns = [c.lower() for c in df.columns]

    # stock_id 欄位可能 叫 stock_id 或 data_id
    if "stock_id" not in df.columns and "data_id" in df.columns:
        df["stock_id"] = df["data_id"]

    required = {"date", "stock_id"}
    if not required.issubset(df.columns):
        # 欄位不足就視為無可用資料
        return pd.DataFrame()

    # 取 open/close/volume
    # volume 常見欄位：trading_volume / volume
    if "trading_volume" in df.columns:
        volume = pd.to_numeric(df["trading_volume"], errors="coerce").fillna(0.0)
    elif "volume" in df.columns:
        volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    else:
        volume = pd.Series(0.0, index=df.index)

    open_ = pd.to_numeric(df.get("open"), errors="coerce")
    close = pd.to_numeric(df.get("close"), errors="coerce")

    # turnover：優先用 trading_money，否則用 close * volume
    if "trading_money" in df.columns:
        turnover = pd.to_numeric(df["trading_money"], errors="coerce").fillna(0.0)
    else:
        turnover = (close.fillna(0.0) * volume).fillna(0.0)

    daily_return = (close - open_) / open_.replace(0, pd.NA)

    out = pd.DataFrame(
        {
            "date": df["date"],
            "stock_id": df["stock_id"].astype(str),
            "close": close,
            "turnover": turnover,
            "daily_return": daily_return,
        }
    ).dropna(subset=["stock_id", "date"])

    # 同一日同一股票若有重複列，保留最後一筆
    return out.sort_values(["stock_id", "date"]).groupby("stock_id").tail(1).reset_index(drop=True)

from datetime import datetime, timedelta, date as date_type

def _taipei_today() -> date_type:
    """以台灣時區取得今天日期。"""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).date()
    except Exception:
        return date_type.today()

def find_nearest_trading_date_with_data(start_date: date_type, max_days_back: int = 20) -> Tuple[date_type, str]:
    """
    從 start_date 開始往前找，直到找到有資料的交易日為止（最多回推 max_days_back 天）。
    判斷方式：_fetch_daily_price_for_universe(date) 回傳非空 DataFrame。
    """
    for i in range(max_days_back):
        d = start_date - timedelta(days=i)
        ds = d.strftime("%Y-%m-%d")
        try:
            df = _fetch_daily_price_for_universe(ds)
            if df is not None and not df.empty:
                return d, ds
        except Exception:
            # 任何錯誤都視為該日不可用，繼續回推
            continue
    raise RuntimeError(f"回推 {max_days_back} 天仍找不到有資料的交易日")

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
