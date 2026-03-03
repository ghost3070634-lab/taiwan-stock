from datetime import datetime

from notifier.discord_client import DiscordClient
from notifier.message_formatter import format_alert, format_exit
from portfolio.state_store import load_state, save_state, record_exit
from strategies.breakout_institutional import BreakoutInstitutionalStrategy


def main():
    dc = DiscordClient()
    strategy = BreakoutInstitutionalStrategy()
    state = load_state()

    today = datetime.today().date().isoformat()

    # 1) 檢查持股風險警示（進場追蹤）
    for sym, pos in state.positions.items():
        if pos.closed:
            continue
        alert = strategy.detect_negative_alerts(
            symbol=sym,
            name=pos.name,
            breakout_low=pos.breakout_low,
        )
        if alert:
            # 進場追蹤頻道：列出所有警訊
            dc.send("position", format_alert(alert))

            # 若同一天累積 >= 2 個警訊，再額外在 exit 頻道推一次「警示」，不出場
            if len(alert.reasons) >= 2:
                warn_msg = (
                    f"【警示】{alert.symbol} {alert.name}\n"
                    f"今日累積 {len(alert.reasons)} 項警訊，建議嚴格控管部位。"
                )
                dc.send("exit", warn_msg)

    # 2) 檢查最終出場訊號
    for sym, pos in state.positions.items():
        if pos.closed or not pos.entries:
            continue

        avg_entry = sum(e.price * e.size for e in pos.entries) / sum(
            e.size for e in pos.entries
        )
        exit_sig = strategy.detect_exit_signals(
            symbol=sym,
            name=pos.name,
            breakout_low=pos.breakout_low,
            entry_avg_price=avg_entry,
        )
        if exit_sig:
            exit_record = record_exit(
                state,
                symbol=sym,
                exit_price=exit_sig.price,
                reason=exit_sig.reason,
            )
            if exit_record:
                # 出場時顯示：出場價、報酬率、原因（message_formatter 已處理「止盈/止損」文案）
                dc.send(
                    "exit",
                    format_exit(exit_sig, exit_record.pnl_pct),
                )

    save_state(state)
    print(f"daily_after_close done for {today}")


if __name__ == "__main__":
    main()
