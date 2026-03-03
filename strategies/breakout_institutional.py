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

    # ========= FinMind 共同工具 =========

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

    def _get_stock_daily(
        self, stock_id: str, end_date: str, days: int = 120
    ) -> pd.DataFrame:
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
        for c in [
            "open",
            "max",
            "min",
            "close",
            "Trading_Volume",
            "Trading_money",
        ]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["open", "close"])

    def _get_investment_trust_net(
        self, stock_id: str, end_date: str, days: int = 20
    ) -> pd.DataFrame:
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

    def _get_it_holding_pct(
        self, stock_id: str, end_date: str, days: int = 120
    ) -> Optional[float]:
        """
        近似投信持股比例：用 HoldingSharesPer 中有投信常駐區間的總股數 / 發行股數。
        簡化處理：抓最新一筆 percent 總和當成「法人 / 大戶」比例，作為 proxy。
        """
        api = self._loader()
        if api is None:
            return None

        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)

        try:
            df = api.taiwan_stock_holding_shares_per(
                stock_id=stock_id,
                start_date=self._to_date_str(start_dt),
                end_date=end_date,
            )
        except Exception:
            return None

        if df is None or df.empty:
            return None

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        latest = df.iloc[-1]
        # 全部持股分級 percent 加總 = 100%，這裡先用「中大戶區間」當作法人/主力近似
        big = df[
            df["HoldingSharesLevel"].isin(
                ["10001-15000", "15001-20000", "20001-40000", "40001-80000", "80001-160000", "160001以上"]
            )
        ]
        if big.empty:
            return None
        pct = float(big[big["date"] == latest["date"]]["percent"].sum())
        return pct

    def _get_month_revenue(
        self, stock_id: str, end_date: str, months: int = 18
    ) -> pd.DataFrame:
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

    def _compute_last_3m_yoy(self, rev: pd.DataFrame) -> Optional[float]:
        """
        回傳近三個月的平均 YoY（%），且三個月都必須 > 0 才回傳，否則回傳 None。
        """
        if rev.empty:
            return None

        # 補齊欄位 revenue_year / revenue_month，若不存在就從 date 推
        if "revenue_year" in rev.columns:
            rev["y"] = pd.to_numeric(rev["revenue_year"], errors="coerce")
        else:
            rev["y"] = rev["date"].dt.year
        if "revenue_month" in rev.columns:
            rev["m"] = pd.to_numeric(rev["revenue_month"], errors="coerce")
        else:
            rev["m"] = rev["date"].dt.month

        last3 = rev.tail(3).copy()
        if len(last3) < 3:
            return None

        yoy_list: List[float] = []
        for _, row in last3.iterrows():
            y = int(row["y"])
            m = int(row["m"])
            cur = float(row["revenue"])
            prev = rev[(rev["y"] == y - 1) & (rev["m"] == m)]
            if prev.empty:
                return None
            prev_rev = float(prev.iloc[-1]["revenue"])
            if prev_rev <= 0:
                return None
            yoy = (cur - prev_rev) / prev_rev * 100
            if yoy <= 0:
                return None
            yoy_list.append(yoy)

        if len(yoy_list) != 3:
            return None
        return sum(yoy_list) / 3.0

    # ========= weekly 選股：依你指定的條件 =========

    def _filter_by_leading_industries(self, universe: pd.DataFrame) -> pd.DataFrame:
        if not self.leading_industries:
            return universe.copy()
        if "industry" not in universe.columns:
            raise ValueError("universe DataFrame 需要包含 'industry' 欄位才能依產業篩選。")
        return universe[universe["industry"].isin(self.leading_industries)].copy()

    def pick(self, universe: pd.DataFrame) -> pd.DataFrame:
        """
        每週推薦股邏輯：

        1. 先在 leading_industries 範圍內取 universe。
        2. 再從「當日漲幅前 100 名」裡面，套以下條件：
           - 近 3 個月月營收 YoY 皆 > 0（取平均 YoY）
           - 投信近 3~5 日連買，且投信持股比例落在 1%~3% (approx)
           - 均線多頭排列：5 > 10 > 20 > 60，且四條均線皆向上
        3. 回傳欄位：
           stock_id, stock_name, industry, rev_yoy_3m, it_holding_pct
        """
        today = datetime.today().date().isoformat()
        api = self._loader()
        if api is None or universe.empty:
            return pd.DataFrame()

        # Step 1: 僅保留主流產業
        base = self._filter_by_leading_industries(universe)
        if base.empty:
            return pd.DataFrame()

        # Step 2: 拿全市場當日價格，抓漲幅前 100 名
        try:
            # 只指定 start_date=today，代表當日
            mkt = api.taiwan_stock_daily(
                stock_id=None,
                start_date=today,
                end_date=today,
            )
        except Exception:
            return pd.DataFrame()
        if mkt is None or mkt.empty:
            return pd.DataFrame()

        mkt = mkt.copy()
        mkt["change_pct"] = (
            (pd.to_numeric(mkt["close"], errors="coerce")
             - pd.to_numeric(mkt["open"], errors="coerce"))
            / pd.to_numeric(mkt["open"], errors="coerce").replace(0, pd.NA)
        ) * 100
        mkt = mkt.dropna(subset=["change_pct"])
        top = mkt.sort_values("change_pct", ascending=False).head(100)
        top_ids = set(top["stock_id"].astype(str))

        cand = base[base["stock_id"].astype(str).isin(top_ids)].copy()
        if cand.empty:
            return pd.DataFrame()

        rows = []
        for _, row in cand.iterrows():
            sid = str(row["stock_id"])
            name = str(row["stock_name"])
            industry = str(row["industry"])

            # 價格 + 均線
            price = self._get_stock_daily(sid, today, days=120)
            if price.empty or len(price) < 65:
                continue
            price["ma5"] = price["close"].rolling(5).mean()
            price["ma10"] = price["close"].rolling(10).mean()
            price["ma20"] = price["close"].rolling(20).mean()
            price["ma60"] = price["close"].rolling(60).mean()
            last = price.iloc[-1]
            prev = price.iloc[-2]

            def val(series_name: str, idx=-1):
                v = price[series_name].iloc[idx]
                return float(v) if not pd.isna(v) else None

            ma5, ma10, ma20, ma60 = (
                val("ma5"),
                val("ma10"),
                val("ma20"),
                val("ma60"),
            )
            ma5_prev, ma10_prev, ma20_prev, ma60_prev = (
                val("ma5", -2),
                val("ma10", -2),
                val("ma20", -2),
                val("ma60", -2),
            )
            if None in [ma5, ma10, ma20, ma60, ma5_prev, ma10_prev, ma20_prev, ma60_prev]:
                continue

            # 均線多頭排列 + 皆向上
            if not (ma5 > ma10 > ma20 > ma60):
                continue
            if not (ma5 > ma5_prev and ma10 > ma10_prev and ma20 > ma20_prev and ma60 > ma60_prev):
                continue

            # 營收 YoY 條件
            rev = self._get_month_revenue(sid, today, months=18)
            avg_yoy_3m = self._compute_last_3m_yoy(rev)
            if avg_yoy_3m is None:
                continue

            # 投信近 3~5 日開始連續買超
            it = self._get_investment_trust_net(sid, today, days=10)
            if it.empty:
                continue
            last5 = it.tail(5)["net"].tolist()
            if all(v <= 0 for v in last5[-3:]):
                # 至少後 3 天是正的，才算「連買」
                continue

            # 近 3~5 日中，最近連續 > 0 的天數 >=3
            streak = 0
            for v in reversed(last5):
                if v > 0:
                    streak += 1
                else:
                    break
            if streak < 3:
                continue

            # 投信持股比例 1%~3%（用持股分級 proxy）
            it_holding_pct = self._get_it_holding_pct(sid, today)
            if it_holding_pct is None or not (1.0 <= it_holding_pct <= 3.0):
                continue

            rows.append(
                {
                    "stock_id": sid,
                    "stock_name": name,
                    "industry": industry,
                    "rev_yoy_3m": avg_yoy_3m,
                    "it_holding_pct": it_holding_pct,
                }
            )

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df = df.sort_values(
            ["industry", "rev_yoy_3m"], ascending=[True, False]
        ).reset_index(drop=True)
        return df

    # ========= daily_after_close：警訊累積 / 最終防線 =========

    def _get_otc_index(self, end_date: str, days: int = 120) -> pd.DataFrame:
        api = self._loader()
        if api is None:
            return pd.DataFrame()

        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)

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

    def detect_negative_alerts(
        self,
        symbol: str,
        name: str,
        breakout_low: float,
    ) -> Optional[Alert]:
        """
        回傳 Alert（含 reasons），daily_after_close 會：
        - 有任何一個 reason 就推到 position 頻道
        - 若 len(reasons) >= 2，額外由 daily_after_close 發一次「警示」到 exit 頻道（不出場）
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
            ma10_prev = (
                float(price["ma10"].iloc[-2])
                if len(price) >= 2 and not pd.isna(price["ma10"].iloc[-2])
                else None
            )
            ma20 = float(price["ma20"].iloc[-1]) if not pd.isna(price["ma20"].iloc[-1]) else None

            avg20_vol = (
                float(price["Trading_Volume"].rolling(20).mean().iloc[-1])
                if "Trading_Volume" in price.columns and len(price) >= 20
                else None
            )

            reasons: List[str] = []

            # 技術價量：高檔爆量收黑 / 長上影
            if avg20_vol and avg20_vol > 0:
                rng = max(high - low, 1e-9)
                upper_wick = high - max(open_, close)
                upper_wick_ratio = upper_wick / rng
                is_bear = close < open_
                is_spike = vol >= 5 * avg20_vol
                is_long_wick = upper_wick_ratio >= 0.6 and vol >= 3 * avg20_vol
                if (is_bear and is_spike) or is_long_wick:
                    reasons.append("技術：高檔爆量收黑 / 長上影，疑似出貨")

            # 技術價量：跌破起漲紅 K 低點
            if breakout_low is not None and close < float(breakout_low):
                reasons.append("技術：跌破起漲紅 K 低點（假突破警訊）")

            # 技術價量：破 10 日線且 10MA 下彎
            if ma10 and ma10_prev and close < ma10 and ma10 < ma10_prev:
                reasons.append("技術：跌破 10 日線且 10MA 下彎")

            # 籌碼：投信由買轉賣 / 連續賣超
            it = self._get_investment_trust_net(symbol, today, days=15)
            if not it.empty:
                last_n = it.tail(5).copy()
                sell_streak = 0
                for v in reversed(last_n["net"].tolist()):
                    if v < 0:
                        sell_streak += 1
                    else:
                        break
                if sell_streak >= 2:
                    reasons.append(f"籌碼：投信連續賣超 {sell_streak} 天")

                today_net = float(last_n["net"].iloc[-1])
                prev_pos_sum = float(
                    last_n["net"].iloc[:-1][last_n["net"].iloc[:-1] > 0].sum()
                )
                if today_net < 0 and prev_pos_sum > 0 and abs(today_net) > prev_pos_sum:
                    reasons.append("籌碼：投信單日賣超 > 前幾日買超總和")

            # 環境：櫃買指數跌破 20MA
            otc = self._get_otc_index(today, days=200)
            if not otc.empty and len(otc) >= 25:
                otc["ma20"] = otc["price"].rolling(20).mean()
                otc_close = float(otc["price"].iloc[-1])
                otc_ma20 = (
                    float(otc["ma20"].iloc[-1]) if not pd.isna(otc["ma20"].iloc[-1]) else None
                )
                if otc_ma20 and otc_close < otc_ma20:
                    reasons.append("環境：櫃買指數跌破 20 日線，中小型股轉弱")

            # 基本面：最新月營收 YoY 轉負
            rev = self._get_month_revenue(symbol, today, months=24)
            if not rev.empty:
                if "revenue_year" in rev.columns:
                    rev["y"] = pd.to_numeric(rev["revenue_year"], errors="coerce")
                else:
                    rev["y"] = rev["date"].dt.year
                if "revenue_month" in rev.columns:
                    rev["m"] = pd.to_numeric(rev["revenue_month"], errors="coerce")
                else:
                    rev["m"] = rev["date"].dt.month

                latest = rev.iloc[-1]
                y = int(latest["y"])
                m = int(latest["m"])
                cur_rev = float(latest["revenue"])
                prev = rev[(rev["y"] == y - 1) & (rev["m"] == m)]
                if not prev.empty:
                    prev_rev = float(prev.iloc[-1]["revenue"])
                    if prev_rev > 0:
                        yoy = (cur_rev - prev_rev) / prev_rev * 100
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
            return None

    def detect_exit_signals(
        self,
        symbol: str,
        name: str,
        breakout_low: float,
        entry_avg_price: float,
    ) -> Optional[ExitSignal]:
        """
        只負責「最終防線」：
        - 虧損達 -7%
        - 跌破 20 日線
        - 跌破起漲紅 K 低點
        """
        try:
            today = datetime.today().date().isoformat()

            price = self._get_stock_daily(symbol, today, days=120)
            if price.empty:
                return None

            last = price.iloc[-1]
            close = float(last["close"])

            pnl_pct = (close - float(entry_avg_price)) / float(entry_avg_price) * 100

            price["ma20"] = price["close"].rolling(20).mean()
            ma20 = (
                float(price["ma20"].iloc[-1])
                if not pd.isna(price["ma20"].iloc[-1])
                else None
            )

            # 防線 1：虧損達 -7%
            if pnl_pct <= -7:
                return ExitSignal(
                    symbol=symbol,
                    name=name,
                    price=close,
                    reason=f"虧損達 -7% 防線（目前 {pnl_pct:.2f}%）",
                )

            # 防線 2：跌破 20 日線
            if ma20 and close < ma20:
                return ExitSignal(
                    symbol=symbol,
                    name=name,
                    price=close,
                    reason="跌破 20 日線（月線）防線",
                )

            # 防線 3：跌破起漲紅 K 低點
            if breakout_low is not None and close < float(breakout_low):
                return ExitSignal(
                    symbol=symbol,
                    name=name,
                    price=close,
                    reason="跌破起漲紅 K 低點（假突破、真出貨）",
                )

            return None
        except Exception:
            return None
