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
    """
    使用 FinMind 取得特定日期的全市場股價資料。

    為了避免對 API 發出過多請求，這裡採用「逐檔抓取 + 合併」的方式，
    之後若要最佳化，可以改成一次抓全部再 merge。
    """
    try:
        from FinMind.data import DataLoader
    except ImportError as exc:
        raise RuntimeError("需要安裝 FinMind 才能計算產業統計，請先在環境中安裝 FinMind。") from exc

    loader = DataLoader()
    universe = load_stock_universe()

    frames: List[pd.DataFrame] = []
    for stock_id in universe["stock_id"].unique():
        try:
            df = loader.taiwan_stock_daily(
                stock_id=stock_id,
                start_date=target_date,
                end_date=target_date,
            )
        except Exception:
            continue
        if df.empty:
            continue

        # FinMind 欄位通常包含: date, stock_id, open, close, max, min, Trading_Volume ...
        df = df.copy()
        if "date" not in df.columns or "open" not in df.columns or "close" not in df.columns:
            continue

        df = df.assign(
            target_date=df["date"],
            stock_id=df.get("stock_id", stock_id),
            open=df["open"].astype(float),
            close=df["close"].astype(float),
        )
        if "Trading_Volume" in df.columns:
            volume = df["Trading_Volume"].astype(float)
        elif "volume" in df.columns:
            volume = df["volume"].astype(float)
        else:
            volume = pd.Series(0.0, index=df.index)

        turnover = df["close"] * volume
        daily_return = (df["close"] - df["open"]) / df["open"].replace(0, pd.NA)

        frames.append(
            pd.DataFrame(
                {
                    "date": df["date"],
                    "stock_id": df["stock_id"],
                    "close": df["close"],
                    "turnover": turnover,
                    "daily_return": daily_return,
                }
            )
        )

    if not frames:
        raise RuntimeError(f"在 {target_date} 無法取得任何股價資料，請確認 FinMind API 或日期是否正確。")

    all_prices = pd.concat(frames, ignore_index=True)
    # 只保留該日期當天的最後一筆記錄（多數情況下一天只會有一筆）
    return all_prices.sort_values(["stock_id", "date"]).groupby("stock_id").tail(1)


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
