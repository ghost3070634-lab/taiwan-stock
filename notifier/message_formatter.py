from typing import List
from strategies.breakout_institutional import Alert, ExitSignal


def format_alert(alert: Alert) -> str:
    reasons_str = "\n".join(f"- {r}" for r in alert.reasons)
    return (
        f"【持股風險警示】{alert.symbol} {alert.name}\n"
        f"{reasons_str}"
    )


def format_exit(exit_sig: ExitSignal, pnl_pct: float) -> str:
    return (
        f"【出場通知】{exit_sig.symbol} {exit_sig.name}\n"
        f"出場價：{exit_sig.price:.2f}\n"
        f"報酬率：約 {pnl_pct:.2f}%\n"
        f"原因：{exit_sig.reason}"
    )
