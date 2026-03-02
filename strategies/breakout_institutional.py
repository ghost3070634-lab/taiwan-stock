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


class BreakoutInstitutionalStrategy:
    """
    示意版的 Breakout + 投信策略骨架。

    目前僅實作「產業白名單」的串接，其他條件可視需求再加入：
    - 近 3 月營收 YoY > 0
    - 投信連買 / 持股比例
    - 技術面（多頭排列 + 橫盤突破）
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
