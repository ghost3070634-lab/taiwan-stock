from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd

from data.finmind_client import FinMindClient


@dataclass
class Alert:
    symbol: str
    name: str
    reasons: List[str]


@dataclass
class EntrySignal:
    symbol: str
    name: str
    price: float
    size: float
    reason: str
    breakout_low: Optional[float] = None


@dataclass
class ExitSignal:
    symbol: str
    name: str
    price: float
    reason: str


class BreakoutInstitutionalStrategy:
    def __init__(self, client: Optional[FinMindClient] = None):
        self.client = client or FinMindClient()

    # === 大盤環境判斷 ===
    def is_bullish_env(self) -> bool:
        tse = self.client.get_index_daily("TAIEX", days=60)
        otc = self.client.get_index_daily("OTC", days=60)
        if tse.empty or otc.empty:
            return False

        tse_ma20 = tse["close"].tail(20).mean()
        otc_ma20 = otc["close"].tail(20).mean()
        tse_close = tse["close"].iloc[-1]
        otc_close = otc["close"].iloc[-1]
        return (tse_close > tse_ma20) and (otc_close > otc_ma20)

    # === 不利於持股的警訊 ===
    def detect_negative_alerts(
        self,
        symbol: str,
        name: str,
        breakout_low: Optional[float],
    ) -> Optional[Alert]:
        reasons: List[str] = []

        inst = self.client.get_stock_institutional_investors(symbol, days=15)
        margin = self.client.get_stock_margin(symbol, days=15)
        daily = self.client.get_stock_daily(symbol, days=60)
        if daily.empty:
            return None

        close = daily["close"].values
        ma10 = pd.Series(close).rolling(10).mean().values
        ma20 = pd.Series(close).rolling(20).mean().values
        today_close = close[-1]

        # 1) 投信由買轉賣 / 連續賣超
        if not inst.empty:
            inst_recent = inst.tail(5)
            diff = inst_recent["buy"].values - inst_recent["sell"].values
            if np.all(diff[-3:] < 0):
                reasons.append("投信近 3 日連續賣超")
            elif diff[-1] < 0 and inst_recent["sell"].iloc[-1] > inst_recent["buy"].sum():
                reasons.append("投信單日賣超大於前幾日買超總和")

        # 2) 投信持股比例過高（這裡做示意，實際要用持股比例欄位）
        # 若有持股比例欄位 (holding_shares_ratio)，可改用該欄
        if "Investment_Trust" in inst.columns:
            # 留給你之後補充
            pass

        # 3) 高檔融資暴增
        if not margin.empty:
            m_recent = margin.tail(5)
            base = m_recent["MarginPurchaseBalance"].iloc[0]
            if base > 0:
                change_pct = (
                    m_recent["MarginPurchaseBalance"].iloc[-1] - base
                ) / base * 100
                if change_pct > 20:
                    reasons.append("高檔融資餘額近幾日大幅增加 (>20%)")

        # 4) 高檔爆量收黑 / 長上影線
        vol = daily["Trading_Volume"].values
        vol_ma20 = pd.Series(vol).rolling(20).mean().values
        if len(vol) >= 20:
            if vol[-1] > 5 * vol_ma20[-1]:
                high = daily["max"].iloc[-1]
                open_ = daily["open"].iloc[-1]
                low = daily["min"].iloc[-1]
                # 粗略判斷：收盤遠離最高價 or 收黑
                if today_close < open_ or (high - today_close) > (today_close - low):
                    reasons.append("高檔爆量收黑或長上影線（量 > 5 倍均量）")

        # 5) 跌破起漲紅 K 低點
        if breakout_low is not None and today_close < breakout_low:
            reasons.append(f"股價已跌破起漲紅K低點 {breakout_low:.2f}")

        # 6) 跌破 10 日線且 10 日線下彎
        if len(ma10) >= 12 and not np.isnan(ma10[-1]):
            if today_close < ma10[-1]:
                slope = ma10[-1] - ma10[-3]
                if slope < 0:
                    reasons.append("股價跌破 10 日線且 10 日線下彎")

        # 7) 櫃買指數風險（簡化，改由 daily job 全局判斷一次即可）

        if reasons:
            return Alert(symbol=symbol, name=name, reasons=reasons)
        return None

    # === 進出場訊號（目前先留空，之後補突破/回測加碼） ===
    def detect_entry_signals(self) -> List[EntrySignal]:
        # TODO: 真正實作時需有「標的池」
        return []

    def detect_exit_signals(
        self,
        symbol: str,
        name: str,
        breakout_low: Optional[float],
        entry_avg_price: float,
    ) -> Optional[ExitSignal]:
        daily = self.client.get_stock_daily(symbol, days=60)
        if daily.empty:
            return None

        close = daily["close"].values
        today_close = close[-1]
        ma20 = pd.Series(close).rolling(20).mean().values
        reason = None

        # 停損：虧損 >= 10%
        pnl_pct = (today_close - entry_avg_price) / entry_avg_price * 100
        if pnl_pct <= -10:
            reason = f"虧損達 {pnl_pct:.2f}% >= 10% 停損"

        # 技術停損：跌破突破紅K低點
        if breakout_low is not None and today_close < breakout_low:
            reason = f"跌破突破紅K低點 {breakout_low:.2f}"

        # 20 日線移動停利（簡化：跌破 MA20）
        if len(ma20) >= 20 and today_close < ma20[-1]:
            reason = reason or "股價跌破 20 日線，觸發移動停利"

        if reason:
            return ExitSignal(symbol=symbol, name=name, price=today_close, reason=reason)
        return None
