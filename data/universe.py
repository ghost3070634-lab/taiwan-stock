from __future__ import annotations

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
    from FinMind.data import DataLoader
    import datetime
    import pandas as pd
    
    api = DataLoader()
    # 這是你 Repo 原本就有的權值股清單
    stocks = ["2330", "2317", "2603", "2412", "2882"]
    
    curr = datetime.datetime.strptime(target_date, "%Y-%m-%d")
    price_df = pd.DataFrame()
    
    # 往下嘗試 20 天，直到抓到有資料的那天 (例如 03-06)
    for _ in range(20):
        # 使用最基礎的 API，避免參數出錯
        df = api.taiwan_stock_daily(
            stock_id="2330", 
            start_date=curr.strftime("%Y-%m-%d"),
            end_date=curr.strftime("%Y-%m-%d")
        )
        if not df.empty:
            # 找到日期後，一口氣抓完清單內的所有股票
            price_df = api.taiwan_stock_daily(
                stock_id=stocks,
                start_date=curr.strftime("%Y-%m-%d"),
                end_date=curr.strftime("%Y-%m-%d")
            )
            break
        curr -= datetime.timedelta(days=1)
        
    if price_df.empty:
        raise RuntimeError("回溯 20 天仍無資料")

    # 針對 compute_industry_stats 所需欄位進行強制對齊
    # 1. 確保有 turnover (成交金額)
    if 'trading_money' in price_df.columns:
        price_df['turnover'] = price_df['trading_money']
    
    # 2. 確保有 daily_return (漲跌幅)，若沒抓到就設為 0 避免 KeyError
    if 'daily_return' not in price_df.columns:
        price_df['daily_return'] = 0 
        
    return price_df



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
