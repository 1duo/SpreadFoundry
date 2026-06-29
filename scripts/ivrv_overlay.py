#!/usr/bin/env python3
"""Reproduce the IV/RV long-vol overlay diagnostic.

This is a research-only cache diagnostic. It intentionally keeps existing
selector trades first and only lets the IV/RV sleeve fill open slots.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any


FROM = date(2016, 1, 1)
TO = date(2026, 6, 28)
COST_PER_TRADE = 25.0
CAPITAL_BUDGET = 100_000.0
TOP_PROFILE = (
    "selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_"
    "non_tsla_sofi_call_debits_only"
)
CACHE_FILE = re.compile(
    r"research(_call)?_(greeks|oi)_(\d{8})_(\d{8})_(\d{8})\.json$"
)


@dataclass
class OptionRow:
    strike: float
    bid: float
    ask: float
    delta: float
    underlying: float
    iv: float
    oi: int


@dataclass
class Trade:
    source: str
    symbol: str
    strategy: str
    entry_date: date
    exit_date: date
    expiration: date
    capital_at_risk: float
    pnl: float
    features: dict[str, float] = field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "symbol": self.symbol,
            "strategy": self.strategy,
            "entry_date": self.entry_date.isoformat(),
            "exit_date": self.exit_date.isoformat(),
            "expiration": self.expiration.isoformat(),
            "capital_at_risk": self.capital_at_risk,
            "pnl": self.pnl,
            "features": self.features,
        }


def parse_date_yyyymmdd(value: str) -> date:
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


def parse_row_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def strike_key(value: Any) -> str:
    return f"{float(value):.3f}"


def load_selector_trades(run_path: Path, profile_name: str) -> list[Trade]:
    data = json.loads(run_path.read_text())
    for profile in data["profiles"]:
        if profile["profile"]["name"] != profile_name:
            continue
        out = []
        for item in profile["trades"]:
            trade = item["trade"]
            out.append(
                Trade(
                    source="selector",
                    symbol=item["symbol"],
                    strategy=item["strategy"],
                    entry_date=date.fromisoformat(trade["entry_date"]),
                    exit_date=date.fromisoformat(trade["exit_date"]),
                    expiration=date.fromisoformat(trade["expiration"]),
                    capital_at_risk=float(item["capital_at_risk"]),
                    pnl=float(trade["pnl"]),
                )
            )
        return out
    raise SystemExit(f"profile not found in {run_path}: {profile_name}")


def load_option_cache(raw_dir: Path) -> tuple[
    dict[str, dict[tuple[date, date], dict[float, OptionRow]]],
    dict[date, float],
]:
    oi: dict[str, dict[tuple[date, date, str], int]] = {
        "put": defaultdict(int),
        "call": defaultdict(int),
    }
    rows: dict[str, dict[tuple[date, date], dict[float, OptionRow]]] = {
        "put": defaultdict(dict),
        "call": defaultdict(dict),
    }
    underlying: dict[date, float] = {}
    files = list(raw_dir.glob("research*_*.json"))

    for path in files:
        match = CACHE_FILE.match(path.name)
        if not match or match.group(2) != "oi":
            continue
        right = "call" if match.group(1) else "put"
        expiration = parse_date_yyyymmdd(match.group(3))
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        for contract in data.get("response") or []:
            contract_data = contract.get("contract") or {}
            key = strike_key(contract_data.get("strike", contract.get("strike", 0)))
            for row in contract.get("data") or []:
                if "timestamp" not in row:
                    continue
                row_dt = parse_row_date(row["timestamp"])
                if FROM <= row_dt <= TO:
                    oi[right][(expiration, row_dt, key)] = max(
                        oi[right][(expiration, row_dt, key)],
                        int(row.get("open_interest") or 0),
                    )

    for path in files:
        match = CACHE_FILE.match(path.name)
        if not match or match.group(2) != "greeks":
            continue
        right = "call" if match.group(1) else "put"
        expiration = parse_date_yyyymmdd(match.group(3))
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        for contract in data.get("response") or []:
            contract_data = contract.get("contract") or {}
            key = strike_key(contract_data.get("strike", contract.get("strike", 0)))
            strike = float(key)
            for row in contract.get("data") or []:
                if "timestamp" not in row:
                    continue
                row_dt = parse_row_date(row["timestamp"])
                if not (FROM <= row_dt <= TO):
                    continue
                bid = float(row.get("bid") or 0)
                ask = float(row.get("ask") or 0)
                delta = float(row.get("delta") or 0)
                underlying_price = float(row.get("underlying_price") or 0)
                iv = float(row.get("implied_vol") or 0)
                if bid <= 0 or ask <= 0 or ask < bid or underlying_price <= 0 or iv <= 0:
                    continue
                underlying.setdefault(row_dt, underlying_price)
                rows[right][(expiration, row_dt)][strike] = OptionRow(
                    strike=strike,
                    bid=bid,
                    ask=ask,
                    delta=delta,
                    underlying=underlying_price,
                    iv=iv,
                    oi=oi[right].get((expiration, row_dt, key), 0),
                )

    return rows, dict(sorted(underlying.items()))


def realized_vol_by_date(underlying: dict[date, float]) -> dict[date, float | None]:
    dates = sorted(underlying)
    prices = [underlying[dt] for dt in dates]
    out: dict[date, float | None] = {}
    for idx, dt in enumerate(dates):
        start = dt - timedelta(days=20)
        start_idx = 0
        while start_idx < len(dates) and dates[start_idx] < start:
            start_idx += 1
        values = prices[start_idx : idx + 1]
        returns = [
            math.log(curr / prev)
            for prev, curr in zip(values, values[1:])
            if prev > 0 and curr > 0
        ]
        if len(returns) < 2:
            out[dt] = None
            continue
        mean = sum(returns) / len(returns)
        variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
        out[dt] = math.sqrt(variance) * math.sqrt(252)
    return out


def lookback_prices(
    underlying: dict[date, float],
    end_date: date,
    days: int,
) -> list[float]:
    start_date = end_date - timedelta(days=days)
    return [
        price
        for row_date, price in underlying.items()
        if start_date <= row_date <= end_date and price > 0
    ]


def trailing_return(
    underlying: dict[date, float],
    end_date: date,
    days: int,
) -> float | None:
    prices = lookback_prices(underlying, end_date, days)
    if len(prices) < 2 or prices[0] <= 0:
        return None
    return prices[-1] / prices[0] - 1.0


def current_drawdown_from_high(
    underlying: dict[date, float],
    end_date: date,
    days: int,
) -> float | None:
    prices = lookback_prices(underlying, end_date, days)
    if len(prices) < 2:
        return None
    high = max(prices)
    if high <= 0:
        return None
    return max(0.0, high - prices[-1]) / high


def quote_ok(row: OptionRow) -> bool:
    mid = (row.bid + row.ask) / 2
    return mid > 0 and (row.ask - row.bid) <= max(mid * 0.20, 0.25)


def close_strangle(
    candidate: dict[str, Any],
    puts: dict[float, OptionRow],
    calls: dict[float, OptionRow],
) -> float | None:
    put = puts.get(candidate["put"].strike)
    call = calls.get(candidate["call"].strike)
    if put is None or call is None:
        return None
    return max(0.0, put.bid) + max(0.0, call.bid)


def close_candidate(
    candidate: dict[str, Any],
    puts: dict[float, OptionRow],
    calls: dict[float, OptionRow],
    structure: str,
) -> float | None:
    if structure == "strangle":
        return close_strangle(candidate, puts, calls)
    if structure == "long-put":
        put = puts.get(candidate["put"].strike)
        return None if put is None else max(0.0, put.bid)
    if structure == "long-call":
        call = calls.get(candidate["call"].strike)
        return None if call is None else max(0.0, call.bid)
    if structure == "put-debit":
        long_put = puts.get(candidate["long"].strike)
        short_put = puts.get(candidate["short"].strike)
        if long_put is None or short_put is None:
            return None
        return max(0.0, long_put.bid - short_put.ask)
    if structure == "call-debit":
        long_call = calls.get(candidate["long"].strike)
        short_call = calls.get(candidate["short"].strike)
        if long_call is None or short_call is None:
            return None
        return max(0.0, long_call.bid - short_call.ask)
    raise ValueError(f"unknown structure: {structure}")


def build_features(
    underlying: dict[date, float],
    entry_date: date,
    dte: int,
    entry_rv: float,
    avg_iv: float,
    debit: float,
    underlying_price: float,
    put: OptionRow | None,
    call: OptionRow | None,
    term_iv_ratio: float | None = None,
) -> dict[str, float]:
    put_mid = (put.bid + put.ask) / 2 if put is not None else 0.0
    call_mid = (call.bid + call.ask) / 2 if call is not None else 0.0
    abs_deltas = [
        abs(row.delta)
        for row in [put, call]
        if row is not None
    ]
    return {
        "dte": float(dte),
        "rv20": entry_rv,
        "avg_iv": avg_iv,
        "ivrv": avg_iv / entry_rv,
        "term_iv_ratio": term_iv_ratio or 0.0,
        "debit_underlying": debit / underlying_price,
        "avg_abs_delta": sum(abs_deltas) / len(abs_deltas) if abs_deltas else 0.0,
        "delta_imbalance": abs(abs(put.delta) - abs(call.delta))
        if put is not None and call is not None
        else 0.0,
        "put_spread_pct": (put.ask - put.bid) / put_mid
        if put is not None and put_mid > 0
        else 0.0,
        "call_spread_pct": (call.ask - call.bid) / call_mid
        if call is not None and call_mid > 0
        else 0.0,
        "min_oi": float(
            min(row.oi for row in [put, call] if row is not None)
        ),
        "return20": trailing_return(underlying, entry_date, 20) or 0.0,
        "return60": trailing_return(underlying, entry_date, 60) or 0.0,
        "drawdown20": current_drawdown_from_high(
            underlying,
            entry_date,
            20,
        )
        or 0.0,
    }


def comparable_leg_iv(
    rows: dict[tuple[date, date], dict[float, OptionRow]],
    entry_date: date,
    current_expiration: date,
    underlying_price: float,
    target_abs_delta: float,
    min_days_farther: int = 5,
    max_days_farther: int = 45,
) -> float | None:
    candidates: list[tuple[int, float, OptionRow]] = []
    for expiration, row_date in rows:
        if row_date != entry_date or expiration <= current_expiration:
            continue
        days_farther = (expiration - current_expiration).days
        if days_farther < min_days_farther or days_farther > max_days_farther:
            continue
        for row in rows[(expiration, row_date)].values():
            if row.underlying <= 0 or abs(row.underlying - underlying_price) > underlying_price * 0.05:
                continue
            if row.oi < 50 or not quote_ok(row):
                continue
            candidates.append(
                (
                    days_farther,
                    abs(abs(row.delta) - target_abs_delta),
                    row,
                )
            )
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2].iv


def term_iv_ratio_for_candidate(
    rows: dict[str, dict[tuple[date, date], dict[float, OptionRow]]],
    entry_date: date,
    expiration: date,
    underlying_price: float,
    avg_iv: float,
    put: OptionRow | None,
    call: OptionRow | None,
) -> float | None:
    comps = []
    if put is not None:
        comp = comparable_leg_iv(
            rows["put"],
            entry_date,
            expiration,
            underlying_price,
            abs(put.delta),
        )
        if comp is not None:
            comps.append(comp)
    if call is not None:
        comp = comparable_leg_iv(
            rows["call"],
            entry_date,
            expiration,
            underlying_price,
            abs(call.delta),
        )
        if comp is not None:
            comps.append(comp)
    if not comps:
        return None
    next_avg_iv = sum(comps) / len(comps)
    if next_avg_iv <= 0:
        return None
    return avg_iv / next_avg_iv


def generate_ivrv_trades(raw_dir: Path, symbol: str, structure: str) -> list[Trade]:
    rows, underlying = load_option_cache(raw_dir)
    rv20 = realized_vol_by_date(underlying)
    if structure == "strangle":
        candidate_keys = set(rows["put"]) & set(rows["call"])
    elif structure in {"long-put", "put-debit"}:
        candidate_keys = set(rows["put"])
    elif structure in {"long-call", "call-debit"}:
        candidate_keys = set(rows["call"])
    else:
        raise ValueError(f"unknown structure: {structure}")
    keys = sorted(candidate_keys, key=lambda value: (value[1], value[0]))
    by_entry: dict[date, list[dict[str, Any]]] = defaultdict(list)

    for expiration, entry_date in keys:
        dte = (expiration - entry_date).days
        entry_rv = rv20.get(entry_date)
        if dte < 1 or dte > 14 or entry_rv is None or entry_rv < 0.30:
            continue
        puts = rows["put"].get((expiration, entry_date), {})
        calls = rows["call"].get((expiration, entry_date), {})
        all_rows = list(puts.values()) + list(calls.values())
        if not all_rows:
            continue
        underlying_price = all_rows[0].underlying
        min_delta, max_delta = 0.45, 0.70
        target_delta = (min_delta + max_delta) / 2
        put_candidates = [
            row
            for row in puts.values()
            if row.strike <= underlying_price
            and min_delta <= abs(row.delta) <= max_delta
            and row.oi >= 50
            and quote_ok(row)
        ]
        call_candidates = [
            row
            for row in calls.values()
            if row.strike >= underlying_price
            and min_delta <= abs(row.delta) <= max_delta
            and row.oi >= 50
            and quote_ok(row)
        ]
        best: tuple[tuple[float, ...], dict[str, Any]] | None = None
        ranked_puts = sorted(
            put_candidates,
            key=lambda row: (abs(abs(row.delta) - target_delta), row.ask),
        )[:8]
        ranked_calls = sorted(
            call_candidates,
            key=lambda row: (abs(abs(row.delta) - target_delta), row.ask),
        )[:8]
        short_puts = sorted(
            [
                row
                for row in puts.values()
                if row.strike < underlying_price
                and 0.20 <= abs(row.delta) <= 0.45
                and row.oi >= 50
                and quote_ok(row)
            ],
            key=lambda row: (abs(abs(row.delta) - 0.30), -row.bid),
        )[:12]
        short_calls = sorted(
            [
                row
                for row in calls.values()
                if row.strike > underlying_price
                and 0.20 <= abs(row.delta) <= 0.45
                and row.oi >= 50
                and quote_ok(row)
            ],
            key=lambda row: (abs(abs(row.delta) - 0.30), -row.bid),
        )[:12]
        if structure == "strangle":
            for put in ranked_puts:
                for call in ranked_calls:
                    avg_iv = (put.iv + call.iv) / 2
                    if avg_iv / entry_rv < 1.25:
                        continue
                    debit = put.ask + call.ask
                    if debit <= 0 or debit / underlying_price > 0.12:
                        continue
                    features = build_features(
                        underlying,
                        entry_date,
                        dte,
                        entry_rv,
                        avg_iv,
                        debit,
                        underlying_price,
                        put,
                        call,
                        term_iv_ratio_for_candidate(
                            rows,
                            entry_date,
                            expiration,
                            underlying_price,
                            avg_iv,
                            put,
                            call,
                        ),
                    )
                    candidate = {
                        "entry_date": entry_date,
                        "expiration": expiration,
                        "put": put,
                        "call": call,
                        "debit": debit,
                        "underlying": underlying_price,
                        "features": features,
                    }
                    key = (
                        -debit / underlying_price,
                        -abs(abs(put.delta) - abs(call.delta)),
                        -(
                            abs(abs(put.delta) - target_delta)
                            + abs(abs(call.delta) - target_delta)
                        ),
                    )
                    if best is None or key > best[0]:
                        best = (key, candidate)
        elif structure == "long-put":
            for put in ranked_puts:
                avg_iv = put.iv
                if avg_iv / entry_rv < 1.25:
                    continue
                debit = put.ask
                if debit <= 0 or debit / underlying_price > 0.08:
                    continue
                features = build_features(
                    underlying,
                    entry_date,
                    dte,
                    entry_rv,
                    avg_iv,
                    debit,
                    underlying_price,
                    put,
                    None,
                    term_iv_ratio_for_candidate(
                        rows,
                        entry_date,
                        expiration,
                        underlying_price,
                        avg_iv,
                        put,
                        None,
                    ),
                )
                candidate = {
                    "entry_date": entry_date,
                    "expiration": expiration,
                    "put": put,
                    "call": None,
                    "debit": debit,
                    "underlying": underlying_price,
                    "features": features,
                }
                key = (
                    -debit / underlying_price,
                    -abs(abs(put.delta) - target_delta),
                )
                if best is None or key > best[0]:
                    best = (key, candidate)
        elif structure == "long-call":
            for call in ranked_calls:
                avg_iv = call.iv
                if avg_iv / entry_rv < 1.25:
                    continue
                debit = call.ask
                if debit <= 0 or debit / underlying_price > 0.08:
                    continue
                features = build_features(
                    underlying,
                    entry_date,
                    dte,
                    entry_rv,
                    avg_iv,
                    debit,
                    underlying_price,
                    None,
                    call,
                    term_iv_ratio_for_candidate(
                        rows,
                        entry_date,
                        expiration,
                        underlying_price,
                        avg_iv,
                        None,
                        call,
                    ),
                )
                candidate = {
                    "entry_date": entry_date,
                    "expiration": expiration,
                    "put": None,
                    "call": call,
                    "debit": debit,
                    "underlying": underlying_price,
                    "features": features,
                }
                key = (
                    -debit / underlying_price,
                    -abs(abs(call.delta) - target_delta),
                )
                if best is None or key > best[0]:
                    best = (key, candidate)
        elif structure == "put-debit":
            for long_put in ranked_puts:
                for short_put in short_puts:
                    width = long_put.strike - short_put.strike
                    if width < 5.0 or width > 25.0:
                        continue
                    avg_iv = (long_put.iv + short_put.iv) / 2
                    if avg_iv / entry_rv < 1.25:
                        continue
                    debit = long_put.ask - short_put.bid
                    if debit <= 0 or debit >= width or debit / underlying_price > 0.08:
                        continue
                    features = build_features(
                        underlying,
                        entry_date,
                        dte,
                        entry_rv,
                        avg_iv,
                        debit,
                        underlying_price,
                        long_put,
                        None,
                        term_iv_ratio_for_candidate(
                            rows,
                            entry_date,
                            expiration,
                            underlying_price,
                            avg_iv,
                            long_put,
                            None,
                        ),
                    )
                    features.update(
                        {
                            "width": width,
                            "max_profit_debit": (width - debit) / debit,
                            "min_oi": float(min(long_put.oi, short_put.oi)),
                            "short_abs_delta": abs(short_put.delta),
                        }
                    )
                    candidate = {
                        "entry_date": entry_date,
                        "expiration": expiration,
                        "put": long_put,
                        "call": None,
                        "long": long_put,
                        "short": short_put,
                        "debit": debit,
                        "underlying": underlying_price,
                        "features": features,
                    }
                    key = (
                        -debit / underlying_price,
                        features["max_profit_debit"],
                        -abs(abs(long_put.delta) - target_delta),
                        -abs(abs(short_put.delta) - 0.30),
                    )
                    if best is None or key > best[0]:
                        best = (key, candidate)
        elif structure == "call-debit":
            for long_call in ranked_calls:
                for short_call in short_calls:
                    width = short_call.strike - long_call.strike
                    if width < 5.0 or width > 25.0:
                        continue
                    avg_iv = (long_call.iv + short_call.iv) / 2
                    if avg_iv / entry_rv < 1.25:
                        continue
                    debit = long_call.ask - short_call.bid
                    if debit <= 0 or debit >= width or debit / underlying_price > 0.08:
                        continue
                    features = build_features(
                        underlying,
                        entry_date,
                        dte,
                        entry_rv,
                        avg_iv,
                        debit,
                        underlying_price,
                        None,
                        long_call,
                        term_iv_ratio_for_candidate(
                            rows,
                            entry_date,
                            expiration,
                            underlying_price,
                            avg_iv,
                            None,
                            long_call,
                        ),
                    )
                    features.update(
                        {
                            "width": width,
                            "max_profit_debit": (width - debit) / debit,
                            "min_oi": float(min(long_call.oi, short_call.oi)),
                            "short_abs_delta": abs(short_call.delta),
                        }
                    )
                    candidate = {
                        "entry_date": entry_date,
                        "expiration": expiration,
                        "put": None,
                        "call": long_call,
                        "long": long_call,
                        "short": short_call,
                        "debit": debit,
                        "underlying": underlying_price,
                        "features": features,
                    }
                    key = (
                        -debit / underlying_price,
                        features["max_profit_debit"],
                        -abs(abs(long_call.delta) - target_delta),
                        -abs(abs(short_call.delta) - 0.30),
                    )
                    if best is None or key > best[0]:
                        best = (key, candidate)
        if best is not None:
            by_entry[entry_date].append(best[1])

    trades: list[Trade] = []
    next_entry_date = FROM
    for entry_date in sorted(by_entry):
        if entry_date < next_entry_date:
            continue
        candidate = max(by_entry[entry_date], key=lambda row: -row["debit"] / row["underlying"])
        current = entry_date + timedelta(days=1)
        exit_result: tuple[date, float] | None = None
        while current <= candidate["expiration"]:
            key = (candidate["expiration"], current)
            if key in rows["put"] or key in rows["call"]:
                credit = close_candidate(
                    candidate,
                    rows["put"].get(key, {}),
                    rows["call"].get(key, {}),
                    structure,
                )
                if credit is not None:
                    held = (current - entry_date).days
                    dte = (candidate["expiration"] - current).days
                    if (
                        credit >= candidate["debit"] * 1.33
                        or credit <= candidate["debit"] * 0.50
                        or held >= 7
                        or dte <= 1
                    ):
                        exit_result = (current, credit)
                        break
            current += timedelta(days=1)
        if exit_result is None:
            continue
        exit_date, exit_credit = exit_result
        max_loss = candidate["debit"] * 100
        pnl = max(-max_loss, (exit_credit - candidate["debit"]) * 100)
        trades.append(
            Trade(
                source=f"{symbol.lower()}_{structure.replace('-', '_')}_ivrv",
                symbol=symbol,
                strategy=f"{structure}_ivrv",
                entry_date=entry_date,
                exit_date=exit_date,
                expiration=candidate["expiration"],
                capital_at_risk=max_loss,
                pnl=pnl,
                features=candidate["features"],
            )
        )
        next_entry_date = exit_date + timedelta(days=1)
    return trades


def allocate_overlay(
    selector_trades: list[Trade],
    overlay_trades: list[Trade],
    max_open: int,
    max_symbol_open: int,
    policy: str,
    raw_dd_allowance: float,
    cost_dd_allowance: float,
) -> tuple[list[Trade], list[Trade]]:
    accepted: list[Trade] = []
    accepted_overlay: list[Trade] = []
    rejected: list[Trade] = []
    base_summary = summarize(selector_trades)
    all_trades = sorted(
        selector_trades + overlay_trades,
        key=lambda trade: (
            trade.entry_date,
            0 if trade.source == "selector" else 1,
            trade.exit_date,
        ),
    )
    for trade in all_trades:
        if trade.source == "selector":
            accepted.append(trade)
            continue
        open_now = [
            prior
            for prior in accepted
            if prior.entry_date <= trade.entry_date < prior.exit_date
        ]
        open_symbol = [prior for prior in open_now if prior.symbol == trade.symbol]
        if len(open_now) >= max_open or len(open_symbol) >= max_symbol_open:
            rejected.append(trade)
            continue
        if overlay_policy_rejects(
            selector_trades,
            accepted_overlay,
            trade,
            base_summary,
            policy,
            raw_dd_allowance,
            cost_dd_allowance,
        ):
            rejected.append(trade)
            continue
        accepted.append(trade)
        accepted_overlay.append(trade)
    return accepted, rejected


def overlay_policy_rejects(
    selector_trades: list[Trade],
    accepted_overlay: list[Trade],
    candidate: Trade,
    base_summary: dict[str, Any],
    policy: str,
    raw_dd_allowance: float,
    cost_dd_allowance: float,
) -> bool:
    if policy == "slot-only":
        return False
    if policy.startswith("pre-entry-"):
        base_before = summarize_before(selector_trades, candidate.entry_date)
        candidate_before = summarize_before(
            selector_trades + accepted_overlay,
            candidate.entry_date,
        )
        raw_worse = (
            candidate_before["drawdown"]
            > base_before["drawdown"] + raw_dd_allowance + 1e-9
        )
        cost_worse = (
            candidate_before["drawdown25"]
            > base_before["drawdown25"] + cost_dd_allowance + 1e-9
        )
        if policy == "pre-entry-no-worse-raw-dd":
            return raw_worse
        if policy == "pre-entry-no-worse-cost-dd":
            return cost_worse
        if policy == "pre-entry-no-worse-any-dd":
            return raw_worse or cost_worse
        raise ValueError(f"unknown overlay policy: {policy}")
    candidate_summary = summarize(selector_trades + accepted_overlay + [candidate])
    raw_worse = (
        candidate_summary["drawdown"]
        > base_summary["drawdown"] + raw_dd_allowance + 1e-9
    )
    cost_worse = (
        candidate_summary["drawdown25"]
        > base_summary["drawdown25"] + cost_dd_allowance + 1e-9
    )
    if policy == "no-worse-raw-dd":
        return raw_worse
    if policy == "no-worse-cost-dd":
        return cost_worse
    if policy == "no-worse-any-dd":
        return raw_worse or cost_worse
    raise ValueError(f"unknown overlay policy: {policy}")


def summarize_before(trades: list[Trade], as_of: date) -> dict[str, Any]:
    return summarize([trade for trade in trades if trade.exit_date < as_of])


def summarize(trades: list[Trade]) -> dict[str, Any]:
    ordered = sorted(trades, key=lambda trade: (trade.exit_date, trade.entry_date))
    pnl = sum(trade.pnl for trade in ordered)
    pnl25 = pnl - COST_PER_TRADE * len(ordered)
    equity = high_water = drawdown = 0.0
    equity25 = high_water25 = drawdown25 = 0.0
    years: dict[int, float] = defaultdict(float)
    years25: dict[int, float] = defaultdict(float)
    symbols: dict[str, float] = defaultdict(float)
    sources: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
    for trade in ordered:
        equity += trade.pnl
        high_water = max(high_water, equity)
        drawdown = max(drawdown, high_water - equity)
        adjusted = trade.pnl - COST_PER_TRADE
        equity25 += adjusted
        high_water25 = max(high_water25, equity25)
        drawdown25 = max(drawdown25, high_water25 - equity25)
        years[trade.exit_date.year] += trade.pnl
        years25[trade.exit_date.year] += adjusted
        symbols[trade.symbol] += trade.pnl
        sources[trade.source][0] += 1
        sources[trade.source][1] += trade.pnl
    best_year25 = max(years25.values(), default=0.0)
    return {
        "trades": len(ordered),
        "pnl": pnl,
        "pnl25": pnl25,
        "drawdown": drawdown,
        "drawdown_pct": drawdown / CAPITAL_BUDGET,
        "drawdown25": drawdown25,
        "drawdown25_pct": drawdown25 / CAPITAL_BUDGET,
        "negative_years": sum(1 for value in years.values() if value < 0),
        "negative_years25": sum(1 for value in years25.values() if value < 0),
        "active_years25": len(years25),
        "best_year25": best_year25,
        "pnl25_ex_best_year": pnl25 - best_year25,
        "years25": dict(sorted(years25.items())),
        "symbols": dict(sorted(symbols.items())),
        "sources": {
            source: {
                "trades": int(values[0]),
                "pnl": values[1],
                "pnl25": values[1] - COST_PER_TRADE * values[0],
            }
            for source, values in sorted(sources.items())
        },
    }


def print_summary(label: str, summary: dict[str, Any]) -> None:
    print(
        f"{label}: trades={summary['trades']} pnl={summary['pnl']:.0f} "
        f"pnl25={summary['pnl25']:.0f} dd={summary['drawdown']:.0f} "
        f"dd_pct={summary['drawdown_pct'] * 100:.2f}% "
        f"dd25={summary['drawdown25']:.0f} "
        f"dd25_pct={summary['drawdown25_pct'] * 100:.2f}% "
        f"neg_years={summary['negative_years']} "
        f"neg_years25={summary['negative_years25']} "
        f"pnl25_ex_best_year={summary['pnl25_ex_best_year']:.0f}"
    )
    print(f"  sources={summary['sources']}")
    print(f"  symbols={summary['symbols']}")
    print(f"  years25={summary['years25']}")


def feature_value(trade: Trade, name: str) -> float:
    return float(trade.features.get(name, 0.0))


def feature_sweep_rows(
    selector_trades: list[Trade],
    overlay_trades: list[Trade],
    max_open: int,
    max_symbol_open: int,
) -> list[dict[str, Any]]:
    filters = [
        ("all_slot_only", lambda trade: True),
        ("ivrv_ge_1p50", lambda trade: feature_value(trade, "ivrv") >= 1.50),
        ("ivrv_ge_1p75", lambda trade: feature_value(trade, "ivrv") >= 1.75),
        ("ivrv_ge_2p00", lambda trade: feature_value(trade, "ivrv") >= 2.00),
        ("term_front_rich_ge_1p05", lambda trade: feature_value(trade, "term_iv_ratio") >= 1.05),
        ("term_front_rich_ge_1p10", lambda trade: feature_value(trade, "term_iv_ratio") >= 1.10),
        (
            "term_not_front_rich_le_1p00",
            lambda trade: 0.0 < feature_value(trade, "term_iv_ratio") <= 1.00,
        ),
        (
            "term_back_rich_le_0p95",
            lambda trade: 0.0 < feature_value(trade, "term_iv_ratio") <= 0.95,
        ),
        ("rv20_le_0p80", lambda trade: feature_value(trade, "rv20") <= 0.80),
        ("rv20_le_0p60", lambda trade: feature_value(trade, "rv20") <= 0.60),
        ("dte_le_7", lambda trade: feature_value(trade, "dte") <= 7),
        (
            "debit_underlying_le_8pct",
            lambda trade: feature_value(trade, "debit_underlying") <= 0.08,
        ),
        (
            "debit_underlying_le_6pct",
            lambda trade: feature_value(trade, "debit_underlying") <= 0.06,
        ),
        ("return20_ge_0", lambda trade: feature_value(trade, "return20") >= 0.0),
        ("return20_lt_0", lambda trade: feature_value(trade, "return20") < 0.0),
        ("return60_ge_0", lambda trade: feature_value(trade, "return60") >= 0.0),
        ("return60_lt_0", lambda trade: feature_value(trade, "return60") < 0.0),
        ("drawdown20_le_5pct", lambda trade: feature_value(trade, "drawdown20") <= 0.05),
        ("drawdown20_le_10pct", lambda trade: feature_value(trade, "drawdown20") <= 0.10),
        ("drawdown20_gt_10pct", lambda trade: feature_value(trade, "drawdown20") > 0.10),
        ("min_oi_ge_200", lambda trade: feature_value(trade, "min_oi") >= 200),
        (
            "tight_quotes_15pct",
            lambda trade: max(
                feature_value(trade, "put_spread_pct"),
                feature_value(trade, "call_spread_pct"),
            )
            <= 0.15,
        ),
        (
            "ivrv_ge_1p50_drawdown20_le_10pct",
            lambda trade: feature_value(trade, "ivrv") >= 1.50
            and feature_value(trade, "drawdown20") <= 0.10,
        ),
        (
            "ivrv_ge_1p50_return20_ge_0",
            lambda trade: feature_value(trade, "ivrv") >= 1.50
            and feature_value(trade, "return20") >= 0.0,
        ),
        (
            "return60_ge_0_term_front_rich_ge_1p05",
            lambda trade: feature_value(trade, "return60") >= 0.0
            and feature_value(trade, "term_iv_ratio") >= 1.05,
        ),
        (
            "return60_ge_0_term_not_front_rich",
            lambda trade: feature_value(trade, "return60") >= 0.0
            and 0.0 < feature_value(trade, "term_iv_ratio") <= 1.00,
        ),
        (
            "rv20_le_0p80_debit_le_8pct",
            lambda trade: feature_value(trade, "rv20") <= 0.80
            and feature_value(trade, "debit_underlying") <= 0.08,
        ),
    ]
    rows = []
    for name, predicate in filters:
        filtered = [trade for trade in overlay_trades if predicate(trade)]
        merged, rejected = allocate_overlay(
            selector_trades,
            filtered,
            max_open=max_open,
            max_symbol_open=max_symbol_open,
            policy="slot-only",
            raw_dd_allowance=0.0,
            cost_dd_allowance=0.0,
        )
        accepted = [trade for trade in merged if trade.source != "selector"]
        rows.append(
            {
                "filter": name,
                "standalone": summarize(filtered),
                "accepted_overlay": summarize(accepted),
                "rejected_overlay": summarize(rejected),
                "merged": summarize(merged),
            }
        )
    return rows


def print_feature_sweep(rows: list[dict[str, Any]]) -> None:
    print(
        "filter overlay_trades overlay_pnl25 overlay_dd25_pct "
        "overlay_neg_years25 overlay_pnl25_ex_best accepted_trades "
        "accepted_pnl25 merged_trades merged_pnl25 merged_dd_pct merged_dd25_pct"
    )
    for row in sorted(
        rows,
        key=lambda item: (
            -item["merged"]["pnl25"],
            item["merged"]["drawdown25_pct"],
            -item["merged"]["trades"],
        ),
    ):
        standalone = row["standalone"]
        accepted = row["accepted_overlay"]
        merged = row["merged"]
        print(
            f"{row['filter']} "
            f"{standalone['trades']} "
            f"{standalone['pnl25']:.0f} "
            f"{standalone['drawdown25_pct'] * 100:.2f}% "
            f"{standalone['negative_years25']} "
            f"{standalone['pnl25_ex_best_year']:.0f} "
            f"{accepted['trades']} "
            f"{accepted['pnl25']:.0f} "
            f"{merged['trades']} "
            f"{merged['pnl25']:.0f} "
            f"{merged['drawdown_pct'] * 100:.2f}% "
            f"{merged['drawdown25_pct'] * 100:.2f}%"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run",
        type=Path,
        default=Path("runs/portfolio-weekly-selector-research-20260628T211847.489213000Z/portfolio_research.json"),
    )
    parser.add_argument("--profile", default=TOP_PROFILE)
    parser.add_argument("--symbol", default="ORCL")
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument(
        "--structure",
        choices=["strangle", "long-put", "long-call", "put-debit", "call-debit"],
        default="strangle",
        help="Overlay structure to test with the same IV/RV entry framework.",
    )
    parser.add_argument("--max-open", type=int, default=3)
    parser.add_argument("--max-symbol-open", type=int, default=2)
    parser.add_argument(
        "--policy",
        choices=[
            "slot-only",
            "no-worse-raw-dd",
            "no-worse-cost-dd",
            "no-worse-any-dd",
            "pre-entry-no-worse-raw-dd",
            "pre-entry-no-worse-cost-dd",
            "pre-entry-no-worse-any-dd",
        ],
        default="slot-only",
        help=(
            "Overlay acceptance policy after open-position checks. The no-worse-* "
            "policies use completed candidate PnL and are research upper bounds, "
            "not deployable live rules. The pre-entry-* policies only use closed "
            "trades before the candidate entry."
        ),
    )
    parser.add_argument(
        "--raw-dd-allowance-pct",
        type=float,
        default=0.0,
        help="Allowed raw drawdown increase as a fraction of the capital budget.",
    )
    parser.add_argument(
        "--cost-dd-allowance-pct",
        type=float,
        default=0.0,
        help="Allowed $25-cost drawdown increase as a fraction of the capital budget.",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--feature-sweep",
        action="store_true",
        help="Run a pre-declared entry-feature sweep with slot-only allocation.",
    )
    args = parser.parse_args()

    symbol = args.symbol.upper()
    raw_dir = args.raw_dir or Path("data/raw/theta") / symbol
    selector_trades = load_selector_trades(args.run, args.profile)
    overlay_trades = generate_ivrv_trades(raw_dir, symbol, args.structure)
    if args.feature_sweep:
        rows = feature_sweep_rows(
            selector_trades,
            overlay_trades,
            max_open=args.max_open,
            max_symbol_open=args.max_symbol_open,
        )
        if args.json:
            print(json.dumps(rows, indent=2, sort_keys=True))
        else:
            print_feature_sweep(rows)
        return

    merged, rejected = allocate_overlay(
        selector_trades,
        overlay_trades,
        max_open=args.max_open,
        max_symbol_open=args.max_symbol_open,
        policy=args.policy,
        raw_dd_allowance=args.raw_dd_allowance_pct * CAPITAL_BUDGET,
        cost_dd_allowance=args.cost_dd_allowance_pct * CAPITAL_BUDGET,
    )

    overlay_source = f"{symbol.lower()}_{args.structure.replace('-', '_')}_ivrv"
    accepted_overlay = [trade for trade in merged if trade.source == overlay_source]
    payload = {
        "run": str(args.run),
        "profile": args.profile,
        "symbol": symbol,
        "structure": args.structure,
        "raw_dir": str(raw_dir),
        "constraints": {
            "max_open": args.max_open,
            "max_symbol_open": args.max_symbol_open,
            "policy": args.policy,
            "raw_dd_allowance_pct": args.raw_dd_allowance_pct,
            "cost_dd_allowance_pct": args.cost_dd_allowance_pct,
        },
        "base": summarize(selector_trades),
        "overlay_standalone": summarize(overlay_trades),
        "merged": summarize(merged),
        "accepted_overlay": summarize(accepted_overlay),
        "rejected_overlay": summarize(rejected),
        "accepted_overlay_trades": [trade.as_json() for trade in accepted_overlay],
        "rejected_overlay_trades": [trade.as_json() for trade in rejected],
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print_summary("BASE", payload["base"])
    print_summary("OVERLAY_STANDALONE", payload["overlay_standalone"])
    print_summary("ACCEPTED_OVERLAY", payload["accepted_overlay"])
    print_summary("REJECTED_OVERLAY", payload["rejected_overlay"])
    print_summary("MERGED", payload["merged"])


if __name__ == "__main__":
    main()
