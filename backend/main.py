from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .database import (
    clear_findings,
    create_upload,
    get_findings,
    get_history,
    get_results,
    get_summary,
    get_upload,
    init_db,
    insert_findings,
    insert_summary,
    update_upload,
)
from .explainer import generate_explanations, generate_flag_reasons
from .features import engineer_features
from .ingestion import load_and_clean_timesheet
from .models import run_model_ensemble
from .rules import combine_severity, severity_from_score, apply_rules, flag_for_review


APP_TITLE = "TimesheetIQ API"
ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".csv"}

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
EXPORT_DIR = DATA_DIR / "exports"
REVIEWER_MODEL_PATH = DATA_DIR / "trained_model.pkl"

app = FastAPI(title=APP_TITLE, version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _severity_counts(series: pd.Series) -> Dict[str, int]:
    counts = series.value_counts().to_dict()
    return {
        "CRITICAL": int(counts.get("CRITICAL", 0)),
        "HIGH": int(counts.get("HIGH", 0)),
        "MODERATE": int(counts.get("MODERATE", 0)),
        "LOW": int(counts.get("LOW", 0)),
    }


def _build_summary(df: pd.DataFrame, model_meta: Dict[str, Any]) -> Dict[str, Any]:
    counts = _severity_counts(df["severity"])
    total_entries = int(len(df))
    total_hours = float(df["hours"].sum())
    billable_hours = float(df.loc[df["work_type"] == "Billable", "hours"].sum())
    utilization = (billable_hours / total_hours * 100.0) if total_hours > 0 else 0.0

    dept_breakdown = (
        df.groupby(["department", "severity"], as_index=False)
        .size()
        .pivot(index="department", columns="severity", values="size")
        .fillna(0)
        .reset_index()
    )
    for sev in ["CRITICAL", "HIGH", "MODERATE", "LOW"]:
        if sev not in dept_breakdown.columns:
            dept_breakdown[sev] = 0

    hist_bins = pd.cut(df["composite_score"], bins=[0, 45, 65, 85, 100], include_lowest=True)
    histogram = (
        hist_bins.value_counts(sort=False)
        .rename_axis("range")
        .reset_index(name="count")
    )
    histogram["range"] = histogram["range"].astype(str)

    summary_json = {
        "severity_counts": counts,
        "weights": model_meta.get("weights", {}),
        "reviewer_model": model_meta.get("reviewer", {}),
        "department_breakdown": dept_breakdown.to_dict(orient="records"),
        "score_histogram": histogram.to_dict(orient="records"),
    }

    # ── Flag statistics ──
    flagged_count = int(df["ai_recommended"].sum()) if "ai_recommended" in df.columns else 0
    flagged_pct = round(flagged_count / total_entries * 100.0, 1) if total_entries > 0 else 0.0
    flagged_hours = float(df.loc[df.get("ai_recommended", False) == True, "hours"].sum()) if "ai_recommended" in df.columns else 0.0
    summary_json["flagged_count"] = flagged_count
    summary_json["flagged_pct"] = flagged_pct
    summary_json["flagged_hours"] = round(flagged_hours, 2)

    # ── Employee risk scores (top 10) ──
    if "ai_recommended" in df.columns:
        emp_risk = (
            df.groupby("employee")
            .agg(
                mean_score=("composite_score", "mean"),
                max_score=("composite_score", "max"),
                flagged_entries=("ai_recommended", "sum"),
                total_entries=("hours", "size"),
            )
            .assign(risk_score=lambda x: (x["mean_score"] * 0.4 + x["max_score"] * 0.6))
            .sort_values("risk_score", ascending=False)
            .head(10)
            .reset_index()
        )
        emp_risk["flagged_entries"] = emp_risk["flagged_entries"].astype(int)
        emp_risk["total_entries"] = emp_risk["total_entries"].astype(int)
        emp_risk = emp_risk.round({"mean_score": 1, "max_score": 1, "risk_score": 1})
        summary_json["top_risk_employees"] = emp_risk.to_dict(orient="records")

    return {
        "total_entries": total_entries,
        "critical_count": counts["CRITICAL"],
        "high_count": counts["HIGH"],
        "moderate_count": counts["MODERATE"],
        "low_count": counts["LOW"],
        "billable_utilization": round(utilization, 2),
        "total_hours": round(total_hours, 2),
        "employee_count": int(df["employee"].nunique()),
        "department_count": int(df["department"].nunique()),
        "summary_json": summary_json,
    }


def _to_finding_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        ml_scores = {
            "isolation_forest": round(float(row.get("if_norm", 0.0)), 4),
            "lof": round(float(row.get("lof_norm", 0.0)), 4),
            "dbscan": round(float(row.get("dbscan_norm", 0.0)), 4),
            "zscore": round(float(row.get("zscore_max", 0.0)), 4),
            "reviewer": (
                round(float(row["reviewer_proba"]), 4)
                if pd.notna(row.get("reviewer_proba"))
                else None
            ),
            "daily_total_hours": round(float(row.get("daily_total_hours", 0.0)), 4),
            "emp_mean_daily_hours": round(float(row.get("emp_mean_daily_hours", 0.0)), 4),
            "emp_z_score_daily": round(float(row.get("emp_z_score_daily", 0.0)), 4),
            "entry_z_score": round(float(row.get("entry_z_score", 0.0)), 4),
        }
        records.append(
            {
                "row_index": int(row.get("source_row_index", -1)),
                "employee": str(row.get("employee", "")),
                "department": str(row.get("department", "")),
                "date": pd.to_datetime(row.get("date_parsed")).strftime("%Y-%m-%d"),
                "hours": float(row.get("hours", 0.0)),
                "task": str(row.get("task", "")),
                "client": str(row.get("client", "")),
                "composite_score": round(float(row.get("composite_score", 0.0)), 2),
                "severity": str(row.get("severity", "LOW")),
                "rules_triggered": list(row.get("rules_triggered", [])),
                "ml_scores": ml_scores,
                "explanation": str(row.get("explanation", "")),
                "ai_recommended": int(bool(row.get("ai_recommended", False))),
                "flag_reason": str(row.get("flag_reason", "")),
                "model_agreement": int(row.get("model_agreement", 0)),
            }
        )
    return records


def _run_analysis_pipeline(upload_id: int, file_path: str) -> None:
    try:
        raw = load_and_clean_timesheet(file_path)
        featured = engineer_features(raw)
        modeled, model_meta = run_model_ensemble(featured, REVIEWER_MODEL_PATH)
        ruled = apply_rules(modeled)
        ruled["ml_severity"] = ruled["composite_score"].apply(severity_from_score)
        ruled["severity"] = [
            combine_severity(ml, rule)
            for ml, rule in zip(ruled["ml_severity"], ruled["rule_max_severity"])
        ]
        ruled = flag_for_review(ruled)
        ruled["explanation"] = generate_explanations(ruled)
        ruled["flag_reason"] = generate_flag_reasons(ruled)

        clear_findings(upload_id)
        finding_records = _to_finding_records(ruled)
        insert_findings(upload_id, finding_records)

        summary = _build_summary(ruled, model_meta)
        insert_summary(upload_id, summary)

        update_upload(
            upload_id,
            row_count=int(len(ruled)),
            employee_count=int(ruled["employee"].nunique()),
            date_range_start=ruled["date_parsed"].min().strftime("%Y-%m-%d"),
            date_range_end=ruled["date_parsed"].max().strftime("%Y-%m-%d"),
            status="completed",
            error_message=None,
        )
    except Exception as exc:
        update_upload(upload_id, status="failed", error_message=str(exc))


def _safe_filename(name: str) -> str:
    keep = [ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in name]
    return "".join(keep)


def _build_export(upload_id: int) -> Path:
    summary = get_summary(upload_id)
    findings = get_findings(upload_id, limit=500000, offset=0)
    if summary is None:
        raise HTTPException(status_code=404, detail="Summary not found.")

    export_path = EXPORT_DIR / f"timesheetiq_report_{upload_id}.xlsx"
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    summary_table = pd.DataFrame(
        [
            {"Metric": "Total Entries", "Value": summary["total_entries"]},
            {"Metric": "Critical", "Value": summary["critical_count"]},
            {"Metric": "High", "Value": summary["high_count"]},
            {"Metric": "Moderate", "Value": summary["moderate_count"]},
            {"Metric": "Low", "Value": summary["low_count"]},
            {"Metric": "Billable Utilization %", "Value": summary["billable_utilization"]},
            {"Metric": "Total Hours", "Value": summary["total_hours"]},
            {"Metric": "Employees", "Value": summary["employee_count"]},
            {"Metric": "Departments", "Value": summary["department_count"]},
        ]
    )

    findings_df = pd.DataFrame(findings)
    if not findings_df.empty:
        findings_df["rules_triggered"] = findings_df["rules_triggered"].apply(lambda x: ", ".join(x))
        findings_df["ml_scores"] = findings_df["ml_scores"].apply(json.dumps)
        critical_df = findings_df[findings_df["severity"] == "CRITICAL"].copy()
    else:
        critical_df = pd.DataFrame()

    with pd.ExcelWriter(export_path, engine="xlsxwriter") as writer:
        summary_table.to_excel(writer, sheet_name="Summary", index=False)
        findings_df.to_excel(writer, sheet_name="All Findings", index=False)
        critical_df.to_excel(writer, sheet_name="Critical Items", index=False)

        workbook = writer.book
        ws_summary = writer.sheets["Summary"]

        severity_sheet_data = pd.DataFrame(
            {
                "Severity": ["CRITICAL", "HIGH", "MODERATE", "LOW"],
                "Count": [
                    summary["critical_count"],
                    summary["high_count"],
                    summary["moderate_count"],
                    summary["low_count"],
                ],
            }
        )
        severity_sheet_data.to_excel(writer, sheet_name="Charts", startrow=0, startcol=0, index=False)
        ws_charts = writer.sheets["Charts"]

        chart = workbook.add_chart({"type": "column"})
        chart.add_series(
            {
                "name": "Severity Distribution",
                "categories": ["Charts", 1, 0, 4, 0],
                "values": ["Charts", 1, 1, 4, 1],
                "data_labels": {"value": True},
            }
        )
        chart.set_title({"name": "Findings by Severity"})
        chart.set_y_axis({"name": "Count"})
        chart.set_style(10)
        ws_summary.insert_chart("D2", chart, {"x_scale": 1.25, "y_scale": 1.25})

        pie = workbook.add_chart({"type": "pie"})
        pie.add_series(
            {
                "name": "Severity Mix",
                "categories": ["Charts", 1, 0, 4, 0],
                "values": ["Charts", 1, 1, 4, 1],
                "data_labels": {"percentage": True},
            }
        )
        pie.set_title({"name": "Severity Mix"})
        ws_charts.insert_chart("D2", pie, {"x_scale": 1.25, "y_scale": 1.25})

    return export_path


@app.on_event("startup")
def startup_event() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    init_db()


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "timesheetiq"}


@app.post("/api/upload")
async def upload_timesheet(background_tasks: BackgroundTasks, file: UploadFile = File(...)) -> Dict[str, Any]:
    filename = file.filename or "timesheet.xlsx"
    suffix = Path(filename).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported file type. Use .xlsx, .xls, or .csv")

    upload_id = create_upload(filename)
    safe_name = _safe_filename(filename)
    stored_path = UPLOAD_DIR / f"{upload_id}_{safe_name}"

    with stored_path.open("wb") as handle:
        shutil.copyfileobj(file.file, handle)

    background_tasks.add_task(_run_analysis_pipeline, upload_id, str(stored_path))
    return {"upload_id": upload_id, "status": "processing"}


@app.get("/api/status/{upload_id}")
def status(upload_id: int) -> Dict[str, Any]:
    upload = get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    return upload


@app.get("/api/results/{upload_id}")
def results(
    upload_id: int,
    limit: int = Query(10000, ge=1, le=500000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    upload = get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    return get_results(upload_id, limit=limit, offset=offset)


@app.get("/api/findings/{upload_id}")
def findings(
    upload_id: int,
    severity: Optional[str] = Query(None, description="CRITICAL, HIGH, MODERATE, LOW"),
    employee: Optional[str] = Query(None),
    department: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(10000, ge=1, le=500000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    upload = get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    data = get_findings(
        upload_id,
        severity=severity.upper() if severity else None,
        employee=employee,
        department=department,
        search=search,
        limit=limit,
        offset=offset,
    )
    return {"upload_id": upload_id, "count": len(data), "items": data}


@app.get("/api/summary/{upload_id}")
def summary(upload_id: int) -> Dict[str, Any]:
    upload = get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    result = get_summary(upload_id)
    if not result:
        raise HTTPException(status_code=404, detail="Summary not found")
    return result


@app.get("/api/export/{upload_id}")
def export(upload_id: int) -> FileResponse:
    upload = get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    path = _build_export(upload_id)
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=path.name,
    )


@app.get("/api/history")
def history(limit: int = Query(100, ge=1, le=1000)) -> Dict[str, Any]:
    return {"items": get_history(limit=limit)}
