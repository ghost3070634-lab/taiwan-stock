from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd


@dataclass
class BreakoutInstitutionalConfig:
    """
    後續若要把「營收 YoY、投信連買、技術面條件」做成可調參數，
    可以擴充在這個設定物件裡。
    """

    min_score: float = 0.0


@dataclass
class Alert:
    """
    每日收盤後的「持股風險警示」訊號。

    被使用的欄位：
    - symbol, name：在 Discord 文案裡顯示標的
    - reasons：一個文字列表，描述風險原因
    """
    symbol: str
    name: str
    breakout_low: float
    reasons: List[str]


@dataclass
class ExitSignal:
    """
    每日收盤後的「出場」訊號。

    被使用的欄位：
    - symbol, name：在 Discord 文案裡顯示標的
    - price：建議出場價
    - reason：文字說明
    """
    symbol: str
    name: str
    price: float
    reason: str


class BreakoutInstitutionalStrategy:
    """
    示意版的 Breakout + 投信策略骨架。

    目前實作內容：
    - weekly 選股：依主流產業 + 分數做挑選
    - daily_after_close：提供 detect_negative_alerts / detect_exit_signals 的介面（暫時回傳 None）

    之後若你要補上實際的風險／出場條件，只要修改這兩個方法的內部邏輯即可。
    """

    def __init__(
        self,
        leading_industries: Optional[Iterable[str]] = None,
        config: Optional[BreakoutInstitutionalConfig] = None,
    ) -> None:
        self.leading_industries = (
            set(leading_industries) if leading_industries is not None else None
        )
        self.config = config or BreakoutInstitutionalConfig()

    # ====== 給 weekly_recommendation 用的選股邏輯 ======

    def _filter_by_leading_industries(self, universe: pd.DataFrame) -> pd.DataFrame:
        if not self.leading_industries:
            return universe.copy()
        if "industry" not in universe.columns:
            raise ValueError("universe DataFrame 需要包含 'industry' 欄位才能依產業篩選。")
        return universe[universe["industry"].isin(self.leading_industries)].copy()

    def _apply_signal_conditions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        這裡應該放入實際的營收 / 籌碼 / 技術面條件。
        目前先用簡單示意分數欄位，讓 weekly job 可以先運作起來。
        """
        result = df.copy()
        if "score" not in result.columns:
            # 先給個 placeholder 分數，未來可替換成實際條件加總結果
            result["score"] = 1.0
        return result[result["score"] >= self.config.min_score]

    def pick(self, universe: pd.DataFrame) -> pd.DataFrame:
        """
        給定含有 `stock_id`, `stock_name`, `industry` 等欄位的 universe，
        回傳符合條件的推薦名單 DataFrame。
        """
        filtered = self._filter_by_leading_industries(universe)
        if filtered.empty:
            return filtered

        selected = self._apply_signal_conditions(filtered)
        if selected.empty:
            return selected

        # 最後按產業與分數排序，方便在推播中分組呈現
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

    # ====== 給 daily_after_close 用的介面（暫時空實作）======

    def detect_negative_alerts(
        self,
        symbol: str,
        name: str,
        breakout_low: float,
    ) -> Optional[Alert]:
        """
        檢查持股是否出現風險警示。

        現階段先回傳 None（不發任何警示），
        若你之後要加上真實邏輯，可以在這裡查價、比對停損線等，
        並在條件觸發時回傳 Alert(...)。
        """
        # TODO: 之後可根據當日收盤價 / 指標 判斷是否產生風險警示
        return None

    def detect_exit_signals(
        self,
        symbol: str,
        name: str,
        breakout_low: float,
        entry_avg_price: float,
    ) -> Optional[ExitSignal]:
        """
        檢查是否出現出場訊號。

        現階段先回傳 None（不發任何出場通知），
        未來你可以依照：
        - 達到停利目標
        - 跌破停損價
        - 其他技術/籌碼條件
        來決定何時回傳 ExitSignal(...)。
        """
        # TODO: 之後可根據當日收盤價 / 報酬率 判斷是否出場
        return None
