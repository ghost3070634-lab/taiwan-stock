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
    1. 挑出主流產業前 N 名
    2. 只在這些產業中，依指定條件（營收 YoY、投信、均線）選出推薦股
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
    top_n = INDUSTRY_CONFIG.top_industry_count
    top_industries = industries.head(top_n)

    # 主流產業摘要
    lines: List[str] = []
    for _, row in top_industries.iterrows():
        name = row["industry"]
        turnover = row.get("total_turnover", 0.0)
        avg_ret = row.get("avg_return", 0.0)
        lead_cnt = int(row.get("leading_stock_count_in_top20", 0))
        lines.append(
            f"- {name}｜成交金額：{turnover:,.0f}｜平均漲幅：{avg_ret:.2%}｜領漲檔數：{lead_cnt}"
        )

    description = "本週主流產業（依綜合分數排序）：\n" + "\n".join(lines)

    # 推薦股列表：顯示 3 個月 YoY 與投信持股比例
    fields: List[Dict] = []
    if not picks.empty:
        for industry in top_industries["industry"]:
            subset = picks[picks["industry"] == industry]
            if subset.empty:
                continue

            stock_lines = []
            for _, row in subset.iterrows():
                sid = row["stock_id"]
                name = row["stock_name"]
                yoy = row.get("rev_yoy_3m")
                it_pct = row.get("it_holding_pct")
                yoy_str = f"{yoy:.1f}%" if pd.notna(yoy) else "N/A"
                it_str = f"{it_pct:.2f}%" if pd.notna(it_pct) else "N/A"
                stock_lines.append(
                    f"{sid} {name}（{industry}）｜近3月營收 YoY 約 {yoy_str}｜投信持股約 {it_str}"
                )

            value = "\n".join(stock_lines)
            if len(value) > 1000:
                value = value[:1000] + "\n... (更多標的已截斷)"

            fields.append(
                {
                    "name": f"{industry} 推薦股",
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
    if not webhook_url:
        raise ValueError("webhook_url is required")

    result = build_weekly_recommendation(target_date)
    embed = _format_discord_embed(target_date, result["industries"], result["picks"])

    payload = {"embeds": [embed]}
    requests.post(webhook_url, json=payload, timeout=10)


if __name__ == "__main__":
    raise SystemExit("請在 CI 或其他腳本中呼叫 send_weekly_recommendation_to_discord。")
