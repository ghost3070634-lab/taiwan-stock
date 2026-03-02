from __future__ import annotations

from datetime import date
from typing import Dict, List

import pandas as pd
import requests

from config import INDUSTRY_CONFIG
from data.universe import load_stock_universe, pick_top_industries
from strategies.breakout_institutional import BreakoutInstitutionalStrategy


def build_weekly_recommendation(target_date: str) -> Dict[str, pd.DataFrame]:
    """
    完整執行一輪「主流產業 + BreakoutInstitutional」週選股流程。

    回傳 dict 方便測試與其他模組重用：
    - industries: DataFrame, 產業統計與分數
    - picks: DataFrame, 分產業的推薦股票清單
    """
    leading_industries, industry_stats = pick_top_industries(target_date)

    universe = load_stock_universe()
    universe = universe[universe["industry"].isin(leading_industries)].copy()

    strategy = BreakoutInstitutionalStrategy(leading_industries=leading_industries)
    picks = strategy.pick(universe)

    return {
        "industries": industry_stats,
        "picks": picks,
    }


def _format_discord_embed(
    target_date: str, industries: pd.DataFrame, picks: pd.DataFrame
) -> Dict:
    # 只拿前 N 個主流產業來呈現
    top_n = INDUSTRY_CONFIG.top_industry_count
    top_industries = industries.head(top_n)

    # 上半段：本週主流產業摘要
    lines: List[str] = []
    for _, row in top_industries.iterrows():
        name = row["industry"]
        turnover = row.get("total_turnover", 0.0)
        avg_ret = row.get("avg_return", 0.0)
        lead_cnt = int(row.get("leading_stock_count_in_top20", 0))
        lines.append(
            f"- {name}｜成交金額總和：{turnover:,.0f}｜平均漲幅：{avg_ret:.2%}｜領漲檔數：{lead_cnt}"
        )

    description = "本週主流產業（依綜合分數排序）：\n" + "\n".join(lines)

    # 下半段：每個產業的推薦股票列表
    fields: List[Dict] = []
    if not picks.empty:
        for industry in top_industries["industry"]:
            subset = picks[picks["industry"] == industry]
            if subset.empty:
                continue
            stock_lines = [
                f"{row['stock_id']} {row['stock_name']}"
                for _, row in subset.iterrows()
            ]
            # Discord field value 長度限制約 1024 字元，這裡做個基本保護
            value = "\n".join(stock_lines)
            if len(value) > 1000:
                value = value[:1000] + "\n... (更多標的已截斷)"

            fields.append(
                {
                    "name": industry,
                    "value": value or "本週無符合條件個股",
                    "inline": False,
                }
            )

    embed = {
        "title": f"本週主流產業選股（{target_date}）",
        "description": description,
        "color": 0x3498DB,
        "fields": fields,
    }
    return embed


def send_weekly_recommendation_to_discord(
    target_date: str,
    webhook_url: str,
) -> None:
    """
    將本週主流產業與推薦清單以 embed 形式推送到 Discord。

    注意：webhook_url 必須由外部（環境變數 / 呼叫端）提供，
    不要在程式碼裡寫死，避免洩漏到公開 repo。
    """
    if not webhook_url:
        raise ValueError("webhook_url is required")

    result = build_weekly_recommendation(target_date)
    embed = _format_discord_embed(target_date, result["industries"], result["picks"])

    payload = {"embeds": [embed]}
    requests.post(webhook_url, json=payload, timeout=10)


if __name__ == "__main__":
    today_str = date.today().strftime("%Y-%m-%d")
    # 本地測試可以這樣：
    #   import os
    #   url = os.environ["DISCORD_WEEKLY_WEBHOOK"]
    #   send_weekly_recommendation_to_discord(today_str, webhook_url=url)
    raise SystemExit("請在其他腳本或 CI 中傳入 webhook_url 後呼叫。")
