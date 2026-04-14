from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


SEVERITY_RANK = {"LOW": 0, "MODERATE": 1, "HIGH": 2, "CRITICAL": 3}
RANK_TO_SEVERITY = {rank: severity for severity, rank in SEVERITY_RANK.items()}


@dataclass(frozen=True)
class RuleSpec:
    rule_id: str
    name: str
    severity: str


RULE_METADATA: Dict[str, RuleSpec] = {
    "R01": RuleSpec("R01", "Impossible Entry", "CRITICAL"),
    "R02": RuleSpec("R02", "Impossible Day", "CRITICAL"),
    "R03": RuleSpec("R03", "Extreme Day", "CRITICAL"),
    "R04": RuleSpec("R04", "Very Long Day", "HIGH"),
    "R05": RuleSpec("R05", "Overtime Day", "MODERATE"),
    "R06": RuleSpec("R06", "Extreme Week", "HIGH"),
    "R07": RuleSpec("R07", "High Week", "MODERATE"),
    "R08": RuleSpec("R08", "Extreme Month", "HIGH"),
    "R09": RuleSpec("R09", "High Month", "MODERATE"),
    "R10": RuleSpec("R10", "Weekend Work", "MODERATE"),
    "R11": RuleSpec("R11", "Holiday Overtime", "HIGH"),
    "R12": RuleSpec("R12", "Suspicious Uniformity", "MODERATE"),
    "R13": RuleSpec("R13", "Missing Task", "MODERATE"),
    "R14": RuleSpec("R14", "Chronic Overtime", "HIGH"),
    "R15": RuleSpec("R15", "Burnout Pattern", "CRITICAL"),
    "R16": RuleSpec("R16", "Escalating Hours", "MODERATE"),
    "R17": RuleSpec("R17", "Ghost Entry", "HIGH"),
    "R18": RuleSpec("R18", "Round-Number Bias", "MODERATE"),
    "R19": RuleSpec("R19", "Duplicate Entry", "HIGH"),
    "R20": RuleSpec("R20", "Project Hopping", "MODERATE"),
}


def severity_from_score(score: float) -> str:
    if score >= 85:
        return "CRITICAL"
    if score >= 65:
        return "HIGH"
    if score >= 45:
        return "MODERATE"
    return "LOW"


def combine_severity(ml_severity: str, rule_severity: str) -> str:
    return ml_severity if SEVERITY_RANK[ml_severity] >= SEVERITY_RANK[rule_severity] else rule_severity


def apply_rules(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    burnout_flags = (
        data.groupby(["employee", "year_month"], as_index=False)
        .agg(
            has_weekend=("is_weekend", "max"),
            has_over10_day=("daily_total_hours", lambda s: bool((s > 10).any())),
            month_total=("monthly_total_hours", "max"),
        )
    )
    burnout_flags["burnout_pattern"] = (
        burnout_flags["has_weekend"].astype(bool)
        & burnout_flags["has_over10_day"].astype(bool)
        & (burnout_flags["month_total"] > 200)
    )
    burnout_lookup = burnout_flags.set_index(["employee", "year_month"])["burnout_pattern"]
    data["burnout_pattern"] = [
        bool(burnout_lookup.get((emp, ym), False)) for emp, ym in zip(data["employee"], data["year_month"])
    ]

    task_blank = data["task"].fillna("").astype(str).str.strip().eq("")

    rule_conditions = {
        "R01": data["hours"] > 24,
        "R02": data["daily_total_hours"] > 24,
        "R03": data["daily_total_hours"] > 16,
        "R04": (data["daily_total_hours"] > 12) & (data["daily_total_hours"] <= 16),
        "R05": (data["daily_total_hours"] > 10) & (data["daily_total_hours"] <= 12),
        "R06": data["weekly_total_hours"] > 60,
        "R07": (data["weekly_total_hours"] > 50) & (data["weekly_total_hours"] <= 60),
        "R08": data["monthly_total_hours"] > 220,
        "R09": (data["monthly_total_hours"] > 200) & (data["monthly_total_hours"] <= 220),
        "R10": data["is_weekend"].astype(bool),
        "R11": data["is_holiday"].astype(bool) & (data["daily_total_hours"] > 8),
        "R12": data["suspicious_round_hours"].astype(bool),
        "R13": (data["hours"] > 0) & task_blank,
        "R14": data["consecutive_over9_workdays"] > 5,
        "R15": data["burnout_pattern"].astype(bool),
        "R16": data.get("weekly_hours_trend", pd.Series(0.0, index=data.index)) > 2.0,
        "R17": pd.Series(False, index=data.index),  # placeholder, computed below
        "R18": data.get("round_number_ratio", pd.Series(0.0, index=data.index)) > 0.80,
        "R19": data.get("is_duplicate_entry", pd.Series(False, index=data.index)).astype(bool),
        "R20": data.get("daily_unique_clients", pd.Series(0, index=data.index)) > 5,
    }

    # R17: Ghost Entry — hours on dates when very few other employees worked
    date_emp_counts = data.groupby("date_parsed")["employee"].transform("nunique")
    rule_conditions["R17"] = (date_emp_counts <= 1) & (data["hours"] > 0)

    triggered: List[List[str]] = [[] for _ in range(len(data))]
    max_rule_rank = np.zeros(len(data), dtype=int)

    for rule_id, condition in rule_conditions.items():
        mask = condition.fillna(False).to_numpy()
        if not mask.any():
            continue
        rank = SEVERITY_RANK[RULE_METADATA[rule_id].severity]
        indices = np.where(mask)[0]
        for idx in indices:
            triggered[idx].append(rule_id)
            if rank > max_rule_rank[idx]:
                max_rule_rank[idx] = rank

    data["rules_triggered"] = triggered
    data["rule_max_severity"] = [RANK_TO_SEVERITY[int(rank)] for rank in max_rule_rank]
    data["rule_max_rank"] = max_rule_rank

    return data


def flag_for_review(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``ai_recommended`` boolean: True when the AI recommends human review."""
    data = df.copy()
    score_flag = data["composite_score"] >= 65
    rule_flag = data["rule_max_rank"] >= SEVERITY_RANK["HIGH"]
    reviewer_flag = pd.Series(False, index=data.index)
    if "reviewer_proba" in data.columns:
        reviewer_flag = pd.to_numeric(data["reviewer_proba"], errors="coerce").fillna(0.0) >= 0.60
    data["ai_recommended"] = (score_flag | rule_flag | reviewer_flag).astype(bool)
    return data
