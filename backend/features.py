from __future__ import annotations

import numpy as np
import pandas as pd


def _is_next_workday(prev_date: pd.Timestamp, curr_date: pd.Timestamp) -> bool:
    prev = np.datetime64(prev_date.date(), "D")
    curr = np.datetime64(curr_date.date(), "D")
    return int(np.busday_count(prev, curr)) == 1


def _compute_streak(
    group: pd.DataFrame,
    *,
    condition_col: str,
    output_col: str,
    require_workday_gap: bool = False,
) -> pd.DataFrame:
    group = group.sort_values("date_parsed").copy()
    streak = np.zeros(len(group), dtype=int)

    prev_date: pd.Timestamp | None = None
    prev_condition = False
    run = 0

    cond_values = group[condition_col].astype(bool).to_numpy()
    date_values = group["date_parsed"].to_numpy()

    for idx, (cond, curr_date) in enumerate(zip(cond_values, date_values)):
        curr_ts = pd.Timestamp(curr_date)
        if not cond:
            run = 0
            prev_condition = False
            prev_date = curr_ts
            streak[idx] = run
            continue

        if prev_condition and prev_date is not None:
            if require_workday_gap:
                contiguous = _is_next_workday(prev_date, curr_ts)
            else:
                contiguous = (curr_ts.date() - prev_date.date()).days == 1
            run = run + 1 if contiguous else 1
        else:
            run = 1

        prev_condition = True
        prev_date = curr_ts
        streak[idx] = run

    group[output_col] = streak
    return group


def _compute_weekend_frequency_30d(daily: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for _, grp in daily.groupby("employee", sort=False):
        g = grp.sort_values("date_parsed").copy()
        g = g.set_index("date_parsed")
        g["weekend_frequency_30d"] = g["is_weekend"].astype(int).rolling("30D").sum().astype(float)
        parts.append(g.reset_index())
    return pd.concat(parts, ignore_index=True)


def _apply_streak_by_employee(
    daily: pd.DataFrame,
    *,
    condition_col: str,
    output_col: str,
    require_workday_gap: bool = False,
) -> pd.DataFrame:
    parts = []
    for _, grp in daily.groupby("employee", sort=False):
        parts.append(
            _compute_streak(
                grp,
                condition_col=condition_col,
                output_col=output_col,
                require_workday_gap=require_workday_gap,
            )
        )
    return pd.concat(parts, ignore_index=True)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["employee"] = data["Employee"].fillna("").astype(str).str.strip()
    data["department"] = data["Department"].fillna("Unknown").replace("", "Unknown").astype(str).str.strip()
    data["client"] = data["Client"].fillna("").astype(str).str.strip()
    data["product"] = data["Product"].fillna("").astype(str).str.strip()
    data["task"] = data["Task"].fillna("").astype(str).str.strip()
    data["notes"] = data["Notes"].fillna("").astype(str).str.strip()
    data["reviewers_comments"] = data["Reviewer's Comments"].fillna("").astype(str).str.strip()
    data["hours"] = pd.to_numeric(data["Hours"], errors="coerce").fillna(0.0).clip(lower=0.0)

    daily = (
        data.groupby(["employee", "date_parsed"], as_index=False)
        .agg(
            department=("department", "first"),
            daily_total_hours=("hours", "sum"),
            daily_entry_count=("hours", "size"),
            daily_unique_clients=("client", pd.Series.nunique),
            daily_unique_tasks=("task", pd.Series.nunique),
            daily_max_single_entry=("hours", "max"),
            day_of_week=("day_of_week", "first"),
            is_weekend=("is_weekend", "max"),
            year=("year", "first"),
            week_number=("week_number", "first"),
            year_month=("year_month", "first"),
            is_holiday_day=("is_holiday", "max"),
        )
        .sort_values(["employee", "date_parsed"])
        .reset_index(drop=True)
    )

    emp_daily = daily.groupby("employee")["daily_total_hours"]
    daily["emp_mean_daily_hours"] = emp_daily.transform("mean")
    daily["emp_std_daily_hours"] = emp_daily.transform("std")

    dept_daily = daily.groupby("department")["daily_total_hours"]
    daily["dept_mean_daily_hours"] = dept_daily.transform("mean")
    daily["dept_std_daily_hours"] = dept_daily.transform("std")

    global_daily_std = float(daily["daily_total_hours"].std() or 0.0)
    daily["emp_std_daily_hours"] = daily["emp_std_daily_hours"].replace(0, np.nan)
    daily["dept_std_daily_hours"] = daily["dept_std_daily_hours"].replace(0, np.nan)

    daily["emp_z_score_daily"] = (
        (daily["daily_total_hours"] - daily["emp_mean_daily_hours"]) / daily["emp_std_daily_hours"]
    )
    dept_fallback = (daily["daily_total_hours"] - daily["dept_mean_daily_hours"]) / daily["dept_std_daily_hours"]
    daily["emp_z_score_daily"] = daily["emp_z_score_daily"].fillna(dept_fallback)
    if global_daily_std > 0:
        global_fallback = (daily["daily_total_hours"] - daily["daily_total_hours"].mean()) / global_daily_std
        daily["emp_z_score_daily"] = daily["emp_z_score_daily"].fillna(global_fallback)
    daily["emp_z_score_daily"] = daily["emp_z_score_daily"].fillna(0.0)

    daily["dept_z_score_daily"] = (
        (daily["daily_total_hours"] - daily["dept_mean_daily_hours"]) / daily["dept_std_daily_hours"]
    ).fillna(0.0)

    weekly_totals = (
        daily.groupby(["employee", "year", "week_number"], as_index=False)["daily_total_hours"]
        .sum()
        .rename(columns={"daily_total_hours": "weekly_total_hours"})
    )
    monthly_totals = (
        daily.groupby(["employee", "year_month"], as_index=False)["daily_total_hours"]
        .sum()
        .rename(columns={"daily_total_hours": "monthly_total_hours"})
    )
    daily = daily.merge(weekly_totals, on=["employee", "year", "week_number"], how="left")
    daily = daily.merge(monthly_totals, on=["employee", "year_month"], how="left")

    daily["is_overtime_gt8"] = daily["daily_total_hours"] > 8
    daily["is_overtime_gt9_workday"] = (daily["daily_total_hours"] > 9) & (daily["day_of_week"] < 5)
    daily["is_exact8_workday"] = np.isclose(daily["daily_total_hours"], 8.0, atol=1e-2) & (daily["day_of_week"] < 5)

    daily = _apply_streak_by_employee(
        daily,
        condition_col="is_overtime_gt8",
        output_col="consecutive_overtime_days",
    )
    daily = _apply_streak_by_employee(
        daily,
        condition_col="is_overtime_gt9_workday",
        output_col="consecutive_over9_workdays",
        require_workday_gap=True,
    )
    daily = _apply_streak_by_employee(
        daily,
        condition_col="is_exact8_workday",
        output_col="exact8_workday_streak",
        require_workday_gap=True,
    )
    daily["suspicious_round_hours"] = daily["exact8_workday_streak"] > 15

    daily = _compute_weekend_frequency_30d(daily)

    data = data.merge(
        daily[
            [
                "employee",
                "date_parsed",
                "daily_total_hours",
                "daily_entry_count",
                "daily_unique_clients",
                "daily_unique_tasks",
                "daily_max_single_entry",
                "emp_mean_daily_hours",
                "emp_std_daily_hours",
                "emp_z_score_daily",
                "dept_mean_daily_hours",
                "dept_std_daily_hours",
                "dept_z_score_daily",
                "weekly_total_hours",
                "monthly_total_hours",
                "consecutive_overtime_days",
                "consecutive_over9_workdays",
                "weekend_frequency_30d",
                "suspicious_round_hours",
            ]
        ],
        on=["employee", "date_parsed"],
        how="left",
    )

    entry_stats_emp = data.groupby("employee")["hours"]
    data["emp_mean_entry_hours"] = entry_stats_emp.transform("mean")
    data["emp_std_entry_hours"] = entry_stats_emp.transform("std").replace(0, np.nan)

    entry_stats_dept = data.groupby("department")["hours"]
    data["dept_mean_entry_hours"] = entry_stats_dept.transform("mean")
    data["dept_std_entry_hours"] = entry_stats_dept.transform("std").replace(0, np.nan)

    entry_z_emp = (data["hours"] - data["emp_mean_entry_hours"]) / data["emp_std_entry_hours"]
    entry_z_dept = (data["hours"] - data["dept_mean_entry_hours"]) / data["dept_std_entry_hours"]
    data["entry_z_score"] = entry_z_emp.fillna(entry_z_dept).fillna(0.0)

    task_count = data.groupby(["employee", "task"])["task"].transform("count")
    emp_total = data.groupby("employee")["task"].transform("count").replace(0, np.nan)
    data["task_rarity_score"] = (1.0 - (task_count / emp_total)).fillna(1.0)
    data.loc[data["task"] == "", "task_rarity_score"] = 1.0

    data["hours_deviation_from_8"] = data["hours"] - 8.0
    data["daily_overtime_flag"] = data["daily_total_hours"] > 8.0
    data["extreme_day_flag"] = data["daily_total_hours"] > 12.0
    data["impossible_flag"] = (data["hours"] > 24.0) | (data["daily_total_hours"] > 24.0)
    data["suspicious_round_hours"] = data["suspicious_round_hours"].fillna(False)

    numeric_fill_zero = [
        "daily_total_hours",
        "daily_entry_count",
        "daily_unique_clients",
        "daily_unique_tasks",
        "daily_max_single_entry",
        "emp_mean_daily_hours",
        "emp_std_daily_hours",
        "emp_z_score_daily",
        "dept_mean_daily_hours",
        "dept_std_daily_hours",
        "dept_z_score_daily",
        "emp_mean_entry_hours",
        "entry_z_score",
        "weekly_total_hours",
        "monthly_total_hours",
        "consecutive_overtime_days",
        "consecutive_over9_workdays",
        "weekend_frequency_30d",
        "task_rarity_score",
        "hours_deviation_from_8",
    ]
    for col in numeric_fill_zero:
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)

    return data.reset_index(drop=True)
