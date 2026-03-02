from dataclasses import dataclass


@dataclass(frozen=True)
class IndustryConfig:
    """
    產業相關可調參數集中管理，避免在邏輯中寫死數值。
    若之後需要改成從 YAML / 環境變數讀取，只要在這裡調整即可。
    """

    # 每次要挑出的主流產業數量
    top_industry_count: int = 3

    # 在「當日漲幅前 N 名」中，至少要有幾檔股票落在同一產業，才視為族群有明顯齊揚
    min_leading_stock_count_in_top20: int = 5

    # 用來判定「領漲榜」時的前 N 名，預設 20
    leading_rank_window: int = 20


INDUSTRY_CONFIG = IndustryConfig()
