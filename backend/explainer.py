from __future__ import annotations

from typing import Iterable, List

import pandas as pd


SEVERITY_ICON = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "MODERATE": "🟡",
    "LOW": "⚪",
}

RULE_FINDINGS = {
    "R01": "Single entry exceeds 24 hours and is physically impossible.",
    "R02": "Daily total exceeds 24 hours and is impossible.",
    "R03": "Daily total exceeds 16 hours (extreme day).",
    "R04": "Daily total exceeds 12 hours (very long day).",
    "R05": "Daily total exceeds 10 hours (overtime day).",
    "R06": "Weekly total exceeds 60 hours (extreme week).",
    "R07": "Weekly total exceeds 50 hours (high week).",
    "R08": "Monthly total exceeds 220 hours (extreme month).",
    "R09": "Monthly total exceeds 200 hours (high month).",
    "R10": "Work logged on a weekend.",
    "R11": "Holiday-tagged day exceeded 8 hours.",
    "R12": "Uniform 8-hour pattern persisted for more than 15 workdays.",
    "R13": "Hours logged with missing task description.",
    "R14": "More than 5 consecutive workdays above 9 hours.",
    "R15": "Burnout pattern detected (weekend + long days + high monthly total).",
    "R16": "Weekly hours escalating for 4+ consecutive weeks.",
    "R17": "Hours logged on a date with no other employees working (ghost entry).",
    "R18": "Over 80% of entries are exact whole numbers — likely estimated, not tracked.",
    "R19": "Duplicate entry detected: same employee, date, task, and hours repeated.",
    "R20": "More than 5 unique clients/projects in a single day (project hopping).",
}

CONFIDENCE_LABELS = {
    5: "Very High",
    4: "High",
    3: "Moderate",
    2: "Low",
    1: "Low",
    0: "None",
}


def _format_date(value: object) -> str:
    try:
        dt = pd.to_datetime(value)
        if pd.isna(dt):
            return "Unknown Date"
        return dt.strftime("%d-%b-%Y")
    except Exception:
        return "Unknown Date"


def _triggered_models(row: pd.Series) -> List[str]:
    triggered: List[str] = []
    if float(row.get("if_norm", 0.0)) >= 0.75 or int(row.get("if_pred", 1)) == -1:
        triggered.append(f"Isolation Forest ({row.get('if_norm', 0.0):.2f})")
    if float(row.get("lof_norm", 0.0)) >= 0.75 or int(row.get("lof_pred", 1)) == -1:
        triggered.append(f"Local Outlier Factor ({row.get('lof_norm', 0.0):.2f})")
    if float(row.get("dbscan_noise", 0.0)) >= 1.0:
        triggered.append("DBSCAN (noise point)")
    z_max = float(row.get("zscore_max", 0.0))
    if z_max >= 2.0:
        triggered.append(f"Z-Score (|z|={z_max:.2f})")
    reviewer = row.get("reviewer_proba", None)
    if reviewer is not None:
        try:
            reviewer_value = float(reviewer)
            if reviewer_value >= 0.60:
                triggered.append(f"Reviewer Model (p={reviewer_value:.2f})")
        except Exception:
            pass
    return triggered


def _main_finding(rule_ids: Iterable[str], row: pd.Series) -> str:
    for rule_id in rule_ids:
        finding = RULE_FINDINGS.get(rule_id)
        if finding:
            return finding

    score = float(row.get("composite_score", 0.0))
    if score >= 85:
        return "Composite anomaly score is in the critical range."
    if score >= 65:
        return "Composite anomaly score is in the high-risk range."
    if score >= 45:
        return "Composite anomaly score indicates a moderate anomaly."
    return "No strong anomaly signal; entry remains in expected range."


def _recommendation(severity: str, rule_ids: List[str]) -> str:
    if severity == "CRITICAL":
        if "R01" in rule_ids or "R02" in rule_ids:
            return "Correct this entry immediately; likely data entry error."
        return "Escalate to reviewer and validate source records immediately."
    if severity == "HIGH":
        return "Review supporting context and request employee clarification."
    if severity == "MODERATE":
        return "Verify the task context and approve only if justified."
    return "No action required unless additional concerns are raised."


def build_explanation(row: pd.Series) -> str:
    severity = str(row.get("severity", "LOW")).upper()
    icon = SEVERITY_ICON.get(severity, "⚪")
    employee = str(row.get("employee", "Unknown Employee")) or "Unknown Employee"
    date_text = _format_date(row.get("date_parsed", row.get("Date", "")))
    rules = list(row.get("rules_triggered", []))
    finding = _main_finding(rules, row)

    evidence = (
        f"Entry={float(row.get('hours', 0.0)):.2f}h, "
        f"Daily={float(row.get('daily_total_hours', 0.0)):.2f}h, "
        f"Weekly={float(row.get('weekly_total_hours', 0.0)):.2f}h, "
        f"Monthly={float(row.get('monthly_total_hours', 0.0)):.2f}h, "
        f"Composite Score={float(row.get('composite_score', 0.0)):.1f}."
    )
    context = (
        f"Employee mean daily hours={float(row.get('emp_mean_daily_hours', 0.0)):.2f}, "
        f"daily z-score={float(row.get('emp_z_score_daily', 0.0)):.2f}, "
        f"entry z-score={float(row.get('entry_z_score', 0.0)):.2f}."
    )

    model_triggers = _triggered_models(row)
    rule_text = ", ".join(rules) if rules else "None"
    model_text = ", ".join(model_triggers) if model_triggers else "Composite anomaly ensemble"
    triggered_by = f"Rules: {rule_text}; Models: {model_text}."

    recommendation = _recommendation(severity, rules)

    # Confidence from model agreement
    agreement = int(row.get("model_agreement", 0))
    confidence = CONFIDENCE_LABELS.get(agreement, "Low")
    confidence_line = f"Confidence: {confidence} ({agreement}/5 models agree)."

    # Peer context
    peer_ratio = float(row.get("peer_ratio", 1.0))
    peer_pct = float(row.get("peer_rank_percentile", 50.0))
    peer_line = f"Peer standing: {peer_ratio:.1f}x department median, {peer_pct:.0f}th percentile."

    return (
        f"{icon} {severity} — {employee} on {date_text}\n\n"
        f"Finding: {finding}\n"
        f"Evidence: {evidence}\n"
        f"Context: {context}\n"
        f"{peer_line}\n"
        f"{confidence_line}\n"
        f"Triggered by: {triggered_by}\n"
        f"Recommendation: {recommendation}"
    )


def build_flag_reason(row: pd.Series) -> str:
    """Return a single human-readable sentence explaining why this entry was flagged."""
    rules = list(row.get("rules_triggered", []))
    hours = float(row.get("hours", 0.0))
    daily = float(row.get("daily_total_hours", 0.0))
    peer_ratio = float(row.get("peer_ratio", 1.0))
    score = float(row.get("composite_score", 0.0))

    if "R01" in rules:
        return f"{hours:.1f}h in one entry — physically impossible."
    if "R02" in rules:
        return f"{daily:.1f}h daily total — exceeds 24h limit."
    if "R03" in rules:
        return f"{daily:.1f}h daily total — extreme workday (>16h)."
    if "R15" in rules:
        return "Burnout pattern: weekend work + long days + high monthly total."
    if "R19" in rules:
        return "Duplicate entry: same employee, date, task, and hours repeated."
    if "R17" in rules:
        return "Ghost entry: hours logged when no other employees worked."
    if "R14" in rules:
        return "Chronic overtime: >5 consecutive workdays above 9 hours."
    if "R06" in rules:
        return f"Extreme week: {float(row.get('weekly_total_hours', 0.0)):.0f}h weekly total."
    if "R08" in rules:
        return f"Extreme month: {float(row.get('monthly_total_hours', 0.0)):.0f}h monthly total."
    if "R12" in rules:
        return "Suspicious uniformity: exactly 8h for 15+ consecutive workdays."
    if "R18" in rules:
        return f"{float(row.get('round_number_ratio', 0.0)) * 100:.0f}% round-number entries — likely estimated."
    if "R20" in rules:
        return f"{int(row.get('daily_unique_clients', 0))} clients in one day — excessive project hopping."
    if "R16" in rules:
        return "Weekly hours escalating steadily over 4+ weeks."
    if "R04" in rules or "R05" in rules:
        return f"{daily:.1f}h daily total — above normal threshold."
    if "R11" in rules:
        return "Extended hours on a holiday-tagged day."
    if "R10" in rules:
        return "Work logged on a weekend."
    if "R13" in rules:
        return "Hours logged with no task description."
    if peer_ratio >= 2.5:
        return f"{peer_ratio:.1f}x department average — significant outlier."
    if score >= 85:
        return f"Composite score {score:.0f} — critical anomaly range."
    if score >= 65:
        return f"Composite score {score:.0f} — high-risk anomaly range."
    return f"Anomaly score {score:.0f} — flagged for review."


def generate_explanations(df: pd.DataFrame) -> pd.Series:
    return df.apply(build_explanation, axis=1)


def generate_flag_reasons(df: pd.DataFrame) -> pd.Series:
    return df.apply(build_flag_reason, axis=1)
