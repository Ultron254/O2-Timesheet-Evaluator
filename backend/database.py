from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "timesheetiq.db"


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER,
                employee_count INTEGER,
                date_range_start DATE,
                date_range_end DATE,
                status TEXT DEFAULT 'processing',
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                upload_id INTEGER REFERENCES uploads(id),
                row_index INTEGER,
                employee TEXT,
                department TEXT,
                date TEXT,
                hours REAL,
                task TEXT,
                client TEXT,
                composite_score REAL,
                severity TEXT,
                rules_triggered TEXT,
                ml_scores TEXT,
                explanation TEXT,
                ai_recommended INTEGER DEFAULT 0,
                flag_reason TEXT DEFAULT '',
                model_agreement INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS upload_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                upload_id INTEGER REFERENCES uploads(id),
                total_entries INTEGER,
                critical_count INTEGER,
                high_count INTEGER,
                moderate_count INTEGER,
                low_count INTEGER,
                billable_utilization REAL,
                total_hours REAL,
                employee_count INTEGER,
                department_count INTEGER,
                summary_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_findings_upload ON findings(upload_id);
            CREATE INDEX IF NOT EXISTS idx_findings_severity ON findings(severity);
            CREATE INDEX IF NOT EXISTS idx_findings_employee ON findings(employee);
            CREATE INDEX IF NOT EXISTS idx_findings_department ON findings(department);
            CREATE INDEX IF NOT EXISTS idx_summaries_upload ON upload_summaries(upload_id);
            """
        )
        conn.commit()


def create_upload(filename: str) -> int:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO uploads(filename, status) VALUES(?, 'processing')", (filename,))
        conn.commit()
        return int(cur.lastrowid)


def update_upload(upload_id: int, **fields: Any) -> None:
    if not fields:
        return
    columns = ", ".join(f"{key}=?" for key in fields.keys())
    values = list(fields.values()) + [upload_id]
    query = f"UPDATE uploads SET {columns} WHERE id=?"
    with get_connection() as conn:
        conn.execute(query, values)
        conn.commit()


def get_upload(upload_id: int) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM uploads WHERE id=?", (upload_id,)).fetchone()
        return dict(row) if row else None


def insert_findings(upload_id: int, findings: Iterable[Dict[str, Any]]) -> None:
    rows = [
        (
            upload_id,
            int(item.get("row_index", -1)),
            str(item.get("employee", "")),
            str(item.get("department", "")),
            str(item.get("date", "")),
            float(item.get("hours", 0.0)),
            str(item.get("task", "")),
            str(item.get("client", "")),
            float(item.get("composite_score", 0.0)),
            str(item.get("severity", "LOW")),
            json.dumps(item.get("rules_triggered", [])),
            json.dumps(item.get("ml_scores", {})),
            str(item.get("explanation", "")),
            int(item.get("ai_recommended", 0)),
            str(item.get("flag_reason", "")),
            int(item.get("model_agreement", 0)),
        )
        for item in findings
    ]
    if not rows:
        return
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO findings(
                upload_id, row_index, employee, department, date, hours, task, client,
                composite_score, severity, rules_triggered, ml_scores, explanation,
                ai_recommended, flag_reason, model_agreement
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def insert_summary(upload_id: int, summary: Dict[str, Any]) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO upload_summaries(
                upload_id, total_entries, critical_count, high_count, moderate_count, low_count,
                billable_utilization, total_hours, employee_count, department_count, summary_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                upload_id,
                int(summary.get("total_entries", 0)),
                int(summary.get("critical_count", 0)),
                int(summary.get("high_count", 0)),
                int(summary.get("moderate_count", 0)),
                int(summary.get("low_count", 0)),
                float(summary.get("billable_utilization", 0.0)),
                float(summary.get("total_hours", 0.0)),
                int(summary.get("employee_count", 0)),
                int(summary.get("department_count", 0)),
                json.dumps(summary.get("summary_json", {})),
            ),
        )
        conn.commit()


def get_summary(upload_id: int) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT upload_id, total_entries, critical_count, high_count, moderate_count, low_count,
                   billable_utilization, total_hours, employee_count, department_count, summary_json
            FROM upload_summaries
            WHERE upload_id=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (upload_id,),
        ).fetchone()
        if not row:
            return None
        data = dict(row)
        try:
            data["summary_json"] = json.loads(data.get("summary_json") or "{}")
        except json.JSONDecodeError:
            data["summary_json"] = {}
        return data


def get_findings(
    upload_id: int,
    severity: Optional[str] = None,
    employee: Optional[str] = None,
    department: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 10000,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    clauses = ["upload_id=?"]
    params: List[Any] = [upload_id]

    if severity:
        clauses.append("severity=?")
        params.append(severity.upper())
    if employee:
        clauses.append("employee LIKE ?")
        params.append(f"%{employee}%")
    if department:
        clauses.append("department LIKE ?")
        params.append(f"%{department}%")
    if search:
        clauses.append("(employee LIKE ? OR task LIKE ? OR client LIKE ? OR explanation LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like, like, like])

    query = f"""
        SELECT *
        FROM findings
        WHERE {' AND '.join(clauses)}
        ORDER BY composite_score DESC, id ASC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()

    result: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("rules_triggered", "ml_scores"):
            value = item.get(key)
            if value is None:
                item[key] = [] if key == "rules_triggered" else {}
                continue
            try:
                item[key] = json.loads(value)
            except json.JSONDecodeError:
                item[key] = [] if key == "rules_triggered" else {}
        result.append(item)
    return result


def get_results(upload_id: int, limit: int = 10000, offset: int = 0) -> Dict[str, Any]:
    upload = get_upload(upload_id)
    summary = get_summary(upload_id)
    findings = get_findings(upload_id, limit=limit, offset=offset)
    return {"upload": upload, "summary": summary, "findings": findings}


def get_history(limit: int = 100) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT u.id, u.filename, u.uploaded_at, u.row_count, u.employee_count,
                   u.date_range_start, u.date_range_end, u.status,
                   COALESCE(s.critical_count, 0) AS critical_count,
                   COALESCE(s.high_count, 0) AS high_count,
                   COALESCE(s.moderate_count, 0) AS moderate_count
            FROM uploads u
            LEFT JOIN upload_summaries s
              ON s.upload_id = u.id
            ORDER BY u.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def clear_findings(upload_id: int) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM findings WHERE upload_id=?", (upload_id,))
        conn.execute("DELETE FROM upload_summaries WHERE upload_id=?", (upload_id,))
        conn.commit()
