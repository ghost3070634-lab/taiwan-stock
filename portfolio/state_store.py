import json
from dataclasses import dataclass, asdict, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional


STATE_FILE = Path("portfolio_breakout_institutional.json")


@dataclass
class EntryRecord:
    date: str
    price: float
    size: float  # 部位比例（0~1）


@dataclass
class ExitRecord:
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    size: float
    pnl_pct: float
    reason: str


@dataclass
class Position:
    symbol: str
    name: str
    breakout_low: Optional[float] = None  # 起漲紅K低點
    entries: List[EntryRecord] = field(default_factory=list)
    exits: List[ExitRecord] = field(default_factory=list)
    closed: bool = False


@dataclass
class PortfolioState:
    positions: Dict[str, Position] = field(default_factory=dict)


def load_state() -> PortfolioState:
    if not STATE_FILE.exists():
        return PortfolioState()
    data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    positions = {}
    for sym, v in data.get("positions", {}).items():
        pos = Position(
            symbol=v["symbol"],
            name=v["name"],
            breakout_low=v.get("breakout_low"),
            entries=[EntryRecord(**e) for e in v.get("entries", [])],
            exits=[ExitRecord(**e) for e in v.get("exits", [])],
            closed=v.get("closed", False),
        )
        positions[sym] = pos
    return PortfolioState(positions=positions)


def save_state(state: PortfolioState):
    data = {
        "positions": {
            sym: asdict(pos)
            for sym, pos in state.positions.items()
        }
    }
    STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def record_entry(
    state: PortfolioState,
    symbol: str,
    name: str,
    price: float,
    size: float,
    breakout_low: Optional[float] = None,
    today: Optional[date] = None,
):
    d = (today or date.today()).isoformat()
    pos = state.positions.get(symbol)
    if not pos:
        pos = Position(symbol=symbol, name=name, breakout_low=breakout_low)
        state.positions[symbol] = pos
    pos.closed = False
    pos.entries.append(EntryRecord(date=d, price=price, size=size))


def record_exit(
    state: PortfolioState,
    symbol: str,
    exit_price: float,
    reason: str,
    today: Optional[date] = None,
):
    d = (today or date.today()).isoformat()
    pos = state.positions.get(symbol)
    if not pos or not pos.entries:
        return None

    total_size = sum(e.size for e in pos.entries)
    avg_entry = sum(e.price * e.size for e in pos.entries) / total_size
    pnl_pct = (exit_price - avg_entry) / avg_entry * 100

    exit_record = ExitRecord(
        entry_date=pos.entries[0].date,
        exit_date=d,
        entry_price=avg_entry,
        exit_price=exit_price,
        size=total_size,
        pnl_pct=pnl_pct,
        reason=reason,
    )
    pos.exits.append(exit_record)
    pos.closed = True
    return exit_record
