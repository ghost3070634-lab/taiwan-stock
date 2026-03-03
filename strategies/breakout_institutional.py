from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import pandas as pd


@dataclass
class BreakoutInstitutionalConfig:
    min_score: float = 0.0


@dataclass
class Alert:
    symbol: str
    name: str
    breakout_low: float
    reasons: List[str]


@dataclass
class ExitSignal:
    symbol: str
    name: str
    price: float
    reason: str


class BreakoutInstitutionalStrategy:
    def __init__(
        self,
        leading_industries: Optional[Iterable[str]] = None,
        config: Optional[BreakoutInstitutionalConfig] = None,
    ) -> None:
        self.leading_industries = (
            set(leading_industries) if leading_industries is not None else None
        )
        self.config = config or BreakoutInstitutionalConfig()

    # ========= weekly 選股（你原本的骨架，保留）=========

    def _filter_by_leading_industries(self, universe: pd.DataFrame) -> pd.DataFrame:
        if not self.leading_industries:
            return universe.copy()
        if "industry" not in universe.columns:
            raise ValueError("universe DataFrame 需要包含 'industry' 欄位才能依產業篩選。")
        return universe[universe["industry"].isin(self.leading_industries)].copy()

    def _apply_signal_conditions(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        if "score" not in result.columns:
            result["score"] = 1.0
        return result[result["score"] >= self.config.min_score]

    def pick(self, universe: pd.DataFrame) -> pd.DataFrame:
        filtered = self._filter_by_leading_industries(universe)
        if filtered.empty:
            return filtered

        selected = self._apply_signal_conditions(filtered)
        if selected.empty:
            return selected

        sort_cols: List[str] = []
        if "industry" in selected.columns:
            sort_cols.append("industry")
        if "score" in selected.columns:
            sort_cols.append("score")

        if sort_cols:
            selected = selected.sort_values(
                sort_cols, ascending=[True] + [False] * (len(sort_cols) - 1)
            )

        return selected.reset_index(drop=True)

    # ========= daily_after_close：風險警示/出場條件（你要的）=========

    def _loader(self):
        try:
            from FinMind.data import DataLoader
        except Exception:
            return None
        token = os.getenv("FINMIND_API_TOKEN", "")
        try:
            return DataLoader(token=token) if token else DataLoader()
        except Exception:
            return DataLoader()

    def _to_date_str(self, d: datetime) -> str:
        return d.date().isoformat()

    def _get_stock_daily(self, stock_id: str, end_date: str, days: int = 80) -> pd.DataFrame:
        api = self._loader()
        if api is None:
            return pd.DataFrame()

        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)

        try:
            df = api.taiwan_stock_daily(
                stock_id=stock_id,
                start_date=self._to_date_str(start_dt),
                end_date=end_date,
            )
        except Exception:
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        for c in ["open", "max", "min", "close", "Trading_Volume", "Trading_money"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["open", "close"])

    def _get_investment_trust_net(self, stock_id: str, end_date: str, days: int = 20) -> pd.DataFrame:
        api = self._loader()
        if api is None:
            return pd.DataFrame()

        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)

        try:
            df = api.taiwan_stock_institutional_investors(
                stock_id=stock_id,
                start_date=self._to_date_str(start_dt),
                end_date=end_date,
            )
        except Exception:
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df = df[df["name"] == "Investment_Trust"].copy()
        if df.empty:
            return pd.DataFrame()

        df["net"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0) - pd.to_numeric(
            df["sell"], errors="coerce"
        ).fillna(0)
        return df[["date", "net"]]

    def _get_margin_balance(self, stock_id: str, end_date: str, days: int = 20) -> pd.DataFrame:
        api = self._loader()
        if api is None:
            return pd.DataFrame()

        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)

        try:
            df = api.taiwan_stock_margin_purchase_short_sale(
                stock_id=stock_id,
                start_date=self._to_date_str(start_dt),
                end_date=end_date,
            )
        except Exception:
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        if "MarginPurchaseTodayBalance" not in df.columns:
            return pd.DataFrame()

        df["mp_balance"] = pd.to_numeric(df["MarginPurchaseTodayBalance"], errors="coerce")
        return df[["date", "mp_balance"]].dropna()

    def _get_otc_index(self, end_date: str, days: int = 120) -> pd.DataFrame:
        api = self._loader()
        if api is None:
            return pd.DataFrame()

        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)

        # FinMind 這個介面可拿指數總報酬指數；OTC 的 index_id 在不同資料源可能不同，這裡多試幾個
        for idx in ["OTC", "TPEx", "TPEX", "TWO"]:
            try:
                df = api.taiwan_stock_total_return_index(
                    index_id=idx,
                    start_date=self._to_date_str(start_dt),
                    end_date=end_date,
                )
            except Exception:
                continue
            if df is None or df.empty:
                continue
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
            df = df.dropna(subset=["price"])
            if not df.empty:
                return df[["date", "price"]]
        return pd.DataFrame()

    def _get_month_revenue(self, stock_id: str, end_date: str, months: int = 18) -> pd.DataFrame:
        api = self._loader()
        if api is None:
            return pd.DataFrame()

        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=months * 31)

        try:
            df = api.taiwan_stock_month_revenue(
                stock_id=stock_id,
                start_date=self._to_date_str(start_dt),
                end_date=end_date,
            )
        except Exception:
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
        return df.dropna(subset=["revenue"])

    def detect_negative_alerts(
        self,
        symbol: str,
        name: str,
        breakout_low: float,
    ) -> Optional[Alert]:
        """
        回傳 Alert 代表「持股風險警示」（推播到 position 頻道）。
        規則對應你提供的四大類警訊：籌碼、技術價量、大盤環境、基本面。
        """
        try:
            today = datetime.today().date().isoformat()

            price = self._get_stock_daily(symbol, today, days=120)
            if price.empty:
                return None

            last = price.iloc[-1]
            close = float(last["close"])
            open_ = float(last["open"])
            high = float(last["max"])
            low = float(last["min"])
            vol = float(last.get("Trading_Volume", 0) or 0)

            price["ma10"] = price["close"].rolling(10).mean()
            price["ma20"] = price["close"].rolling(20).mean()
            ma10 = float(price["ma10"].iloc[-1]) if not pd.isna(price["ma10"].iloc[-1]) else None
            ma10_prev = float(price["ma10"].iloc[-2]) if len(price) >= 2 and not pd.isna(price["ma10"].iloc[-2]) else None
            ma20 = float(price["ma20"].iloc[-1]) if not pd.isna(price["ma20"].iloc[-1]) else None

            avg20_vol = float(price["Trading_Volume"].rolling(20).mean().iloc[-1]) if "Trading_Volume" in price.columns and len(price) >= 20 else None

            reasons: List[str] = []

            # --- 技術與價量警訊 ---
            # 1) 高檔爆量收黑 / 長上影（用量能倍數 + 上影線比例近似）
            if avg20_vol and avg20_vol > 0:
                rng = max(high - low, 1e-9)
                upper_wick = high - max(open_, close)
                upper_wick_ratio = upper_wick / rng
                is_bear = close < open_
                is_spike = vol >= 5 * avg20_vol
                is_long_wick = upper_wick_ratio >= 0.6 and vol >= 3 * avg20_vol
                if (is_bear and is_spike) or is_long_wick:
                    reasons.append("技術：高檔爆量收黑/長上影，疑似出貨")

            # 2) 跌破起漲紅K低點（假突破防線）
            if breakout_low is not None and close < float(breakout_low):
                reasons.append("技術：跌破起漲紅K低點（假突破警訊）")

            # 3) 破 10 日線且均線下彎
            if ma10 and ma10_prev and close < ma10 and ma10 < ma10_prev:
                reasons.append("技術：跌破 10 日線且 10 日線下彎，短線動能轉弱")

            # --- 籌碼面警訊（投信、融資）---
            it = self._get_investment_trust_net(symbol, today, days=15)
            if not it.empty:
                last_n = it.tail(5).copy()
                # 連續賣超 2~3 天
                sell_streak = 0
                for v in reversed(last_n["net"].tolist()):
                    if v < 0:
                        sell_streak += 1
                    else:
                        break
                if sell_streak >= 2:
                    reasons.append(f"籌碼：投信連續賣超 {sell_streak} 天（由買轉賣）")

                # 單日賣超張數 > 過去幾天買超總和（用 net 近似）
                today_net = float(last_n["net"].iloc[-1])
                prev_pos_sum = float(last_n["net"].iloc[:-1][last_n["net"].iloc[:-1] > 0].sum())
                if today_net < 0 and prev_pos_sum > 0 and abs(today_net) > prev_pos_sum:
                    reasons.append("籌碼：投信單日賣超大於前幾日買超總和（認錯/結帳）")

            margin = self._get_margin_balance(symbol, today, days=20)
            if not margin.empty and len(margin) >= 4:
                last4 = margin.tail(4)
                inc3 = (
                    last4["mp_balance"].iloc[-1] > last4["mp_balance"].iloc[-2] >
                    last4["mp_balance"].iloc[-3] > last4["mp_balance"].iloc[-4]
                )
                if inc3 and ma20 and close > ma20:
                    reasons.append("籌碼：高檔融資餘額連增，籌碼疑似轉向散戶")

            # --- 大盤環境警訊（OTC）---
            otc = self._get_otc_index(today, days=200)
            if not otc.empty and len(otc) >= 25:
                otc["ma20"] = otc["price"].rolling(20).mean()
                otc_close = float(otc["price"].iloc[-1])
                otc_ma20 = float(otc["ma20"].iloc[-1]) if not pd.isna(otc["ma20"].iloc[-1]) else None
                if otc_ma20 and otc_close < otc_ma20:
                    reasons.append("環境：櫃買指數跌破 20 日線，資金可能撤退（中小型股轉弱）")

            # --- 基本面警訊（營收）---
            rev = self._get_month_revenue(symbol, today, months=24)
            if not rev.empty:
                # 用「同月 YoY」：找出最新月份與去年同月
                latest = rev.iloc[-1]
                latest_year = int(latest.get("revenue_year", pd.to_datetime(latest["date"]).year))
                latest_month = int(latest.get("revenue_month", pd.to_datetime(latest["date"]).month))
                latest_rev = float(latest["revenue"])

                prev = rev[
                    (rev.get("revenue_year", rev["date"].dt.year) == latest_year - 1)
                    & (rev.get("revenue_month", rev["date"].dt.month) == latest_month)
                ]
                if not prev.empty:
                    prev_rev = float(prev.iloc[-1]["revenue"])
                    if prev_rev > 0:
                        yoy = (latest_rev - prev_rev) / prev_rev * 100
                        if yoy < 0:
                            reasons.append(f"基本面：最新月營收 YoY 轉負（{yoy:.1f}%）")

            if not reasons:
                return None

            return Alert(
                symbol=symbol,
                name=name,
                breakout_low=float(breakout_low) if breakout_low is not None else 0.0,
                reasons=reasons,
            )
        except Exception:
            # daily job 不要因為單一標的資料問題中斷
            return None

    def detect_exit_signals(
        self,
        symbol: str,
        name: str,
        breakout_low: float,
        entry_avg_price: float,
    ) -> Optional[ExitSignal]:
        """
        回傳 ExitSignal 代表「出場通知」（推播到 exit 頻道 + 寫入 portfolio 狀態）。

        你給的「最終防線」我落地成：
        - 虧損 <= -7% → 直接清倉
        - 或 跌破 20 日線 → 直接清倉
        - 或 跌破起漲紅K低點（假突破）→ 直接清倉
        - 或（投信連賣 >=3 且 跌破10日線且10MA下彎）→ 出場
        """
        try:
            today = datetime.today().date().isoformat()

            price = self._get_stock_daily(symbol, today, days=120)
            if price.empty:
                return None

            last = price.iloc[-1]
            close = float(last["close"])

            # 報酬率（%）
            pnl_pct = (close - float(entry_avg_price)) / float(entry_avg_price) * 100

            price["ma10"] = price["close"].rolling(10).mean()
            price["ma20"] = price["close"].rolling(20).mean()
            ma10 = float(price["ma10"].iloc[-1]) if not pd.isna(price["ma10"].iloc[-1]) else None
            ma10_prev = float(price["ma10"].iloc[-2]) if len(price) >= 2 and not pd.isna(price["ma10"].iloc[-2]) else None
            ma20 = float(price["ma20"].iloc[-1]) if not pd.isna(price["ma20"].iloc[-1]) else None

            # 最終防線 1：-7%
            if pnl_pct <= -7:
                return ExitSignal(
                    symbol=symbol,
                    name=name,
                    price=close,
                    reason=f"虧損達 -7% 防線（目前 {pnl_pct:.2f}%）",
                )

            # 最終防線 2：跌破 20 日線
            if ma20 and close < ma20:
                return ExitSignal(
                    symbol=symbol,
                    name=name,
                    price=close,
                    reason="跌破 20 日線（月線）防線",
                )

            # 最終防線 3：假突破（跌破起漲紅K低點）
            if breakout_low is not None and close < float(breakout_low):
                return ExitSignal(
                    symbol=symbol,
                    name=name,
                    price=close,
                    reason="跌破起漲紅K低點（假突破、真出貨）",
                )

            # 兩警訊組合：投信連賣 + 跌破 10 日線且下彎
            it = self._get_investment_trust_net(symbol, today, days=15)
            sell_streak = 0
            if not it.empty:
                last_n = it.tail(5)
                for v in reversed(last_n["net"].tolist()):
                    if v < 0:
                        sell_streak += 1
                    else:
                        break

            if (
                sell_streak >= 3
                and ma10
                and ma10_prev
                and close < ma10
                and ma10 < ma10_prev
            ):
                return ExitSignal(
                    symbol=symbol,
                    name=name,
                    price=close,
                    reason="投信連賣 + 跌破10日線且均線下彎（趨勢轉弱）",
                )

            return None
        except Exception:
            return None
