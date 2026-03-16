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

    return (
        f"{icon} {severity} — {employee} on {date_text}\n\n"
        f"Finding: {finding}\n"
        f"Evidence: {evidence}\n"
        f"Context: {context}\n"
        f"Triggered by: {triggered_by}\n"
        f"Recommendation: {recommendation}"
    )


def generate_explanations(df: pd.DataFrame) -> pd.Series:
    return df.apply(build_explanation, axis=1)
