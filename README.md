# TimesheetIQ (O2 Timesheet Evaluator)

TimesheetIQ is a self-contained upload-and-audit system for O2 timesheets:

1. Upload Excel/CSV timesheet.
2. Run ingestion, cleaning, feature engineering, ML ensemble, and rule checks.
3. Persist all findings and summaries in SQLite.
4. Visualize anomalies in React UI.
5. Export a formatted Excel report.

## Project Structure

```text
timesheetiq/
├── backend/
│   ├── main.py
│   ├── ingestion.py
│   ├── features.py
│   ├── models.py
│   ├── rules.py
│   ├── explainer.py
│   ├── database.py
│   └── schemas.py
├── frontend/
│   ├── App.jsx
│   ├── main.jsx
│   ├── styles.css
│   ├── index.html
│   ├── package.json
│   ├── vite.config.js
│   ├── tailwind.config.cjs
│   └── postcss.config.cjs
├── data/
│   └── trained_model.pkl (created after first reviewer-model training)
└── requirements.txt
```

## Backend Run

```bash
cd timesheetiq
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

## Frontend Run

```bash
cd timesheetiq/frontend
npm install
npm run dev
```

Set optional API base URL:

```bash
# .env (inside timesheetiq/frontend)
VITE_API_BASE=http://localhost:8000
```

If `VITE_API_BASE` is not set, the frontend auto-discovers a running TimesheetIQ backend
across common local URLs (`/api`, `127.0.0.1:8000`, `127.0.0.1:8790`, etc.).
Vite dev server also proxies `/api` to `VITE_API_BASE` (or `VITE_PROXY_TARGET`) when provided.

## API Endpoints

- `POST /api/upload` - upload and start analysis
- `GET /api/status/{id}` - processing status
- `GET /api/results/{id}` - upload + summary + findings
- `GET /api/findings/{id}` - filterable findings
- `GET /api/summary/{id}` - summary statistics
- `GET /api/export/{id}` - download Excel report
- `GET /api/history` - upload history

## Notes

- Reviewer model is cached at `data/trained_model.pkl` after first successful training.
- If reviewer labels are missing/sparse, reviewer model is skipped and ensemble weights are redistributed.
- SQLite DB file is created at `data/timesheetiq.db`.
- On some Windows environments, pandas may hang on WMI queries; this project patches platform
  architecture lookup at backend import time to prevent that startup stall.
