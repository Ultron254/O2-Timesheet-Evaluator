import React, { useEffect, useMemo, useRef, useState, useCallback } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

function normalizeBase(base) {
  if (!base) return "";
  return base.replace(/\/+$/, "");
}

const API_ENV_BASE = normalizeBase(import.meta.env.VITE_API_BASE || "");
const API_CANDIDATES = Array.from(
  new Set(
    [
      API_ENV_BASE,
      "",
      typeof window !== "undefined" ? normalizeBase(window.location.origin) : "",
      "http://127.0.0.1:8000",
      "http://localhost:8000",
      "http://127.0.0.1:8790",
      "http://localhost:8790",
    ].filter(Boolean)
  )
);

async function fetchWithTimeout(url, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function getErrorMessage(response, fallback) {
  try {
    const ct = response.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      const body = await response.json();
      return body.detail || body.message || body.error || fallback;
    }
    const text = await response.text();
    return text.slice(0, 200) || fallback;
  } catch (_) {
    return fallback;
  }
}

// ─── Constants ─────────────────────────────────────────

const SEVERITY_COLORS = {
  CRITICAL: "#ef4444",
  HIGH: "#f97316",
  MODERATE: "#eab308",
  LOW: "#64748b",
};

const SEVERITY_GLOW = {
  CRITICAL: "rgba(239, 68, 68, 0.25)",
  HIGH: "rgba(249, 115, 22, 0.2)",
  MODERATE: "rgba(234, 179, 8, 0.15)",
  LOW: "rgba(100, 116, 139, 0.1)",
};

const SEVERITY_BG = {
  CRITICAL: "rgba(239, 68, 68, 0.1)",
  HIGH: "rgba(249, 115, 22, 0.1)",
  MODERATE: "rgba(234, 179, 8, 0.08)",
  LOW: "rgba(100, 116, 139, 0.08)",
};

const SEVERITY_ICON = {
  CRITICAL: "\u{1F534}",
  HIGH: "\u{1F7E0}",
  MODERATE: "\u{1F7E1}",
  LOW: "\u26AA",
};

const SCORE_CARDS = [
  { key: "critical_count", label: "CRITICAL", severity: "CRITICAL" },
  { key: "high_count", label: "HIGH", severity: "HIGH" },
  { key: "moderate_count", label: "MODERATE", severity: "MODERATE" },
  { key: "low_count", label: "NORMAL", severity: "LOW" },
];

const RULE_EXPLANATIONS = [
  { id: "R01", name: "Impossible Entry", severity: "CRITICAL", condition: "Single entry > 24 hours", rationale: "A single timesheet line cannot physically be more than 24 hours in one day. This almost always means data-entry error." },
  { id: "R02", name: "Impossible Day", severity: "CRITICAL", condition: "Daily total > 24 hours", rationale: "Even when multiple tasks are logged, a calendar day has only 24 hours. Totals above that indicate invalid records." },
  { id: "R03", name: "Extreme Day", severity: "CRITICAL", condition: "Daily total > 16 hours", rationale: "Very high daily totals are high risk for payroll, compliance, and employee wellness; needs urgent validation." },
  { id: "R04", name: "Very Long Day", severity: "HIGH", condition: "Daily total > 12 hours", rationale: "Long workdays may be legitimate but are uncommon and often require manager context, especially if repeated." },
  { id: "R05", name: "Overtime Day", severity: "MODERATE", condition: "Daily total > 10 hours and <= 12", rationale: "Overtime is not always wrong, but it is outside normal baseline and should be reviewed for consistency." },
  { id: "R06", name: "Extreme Week", severity: "HIGH", condition: "Weekly total > 60 hours", rationale: "Very high weekly totals can indicate overbilling, duplicated entries, or risky workload concentration." },
  { id: "R07", name: "High Week", severity: "MODERATE", condition: "Weekly total > 50 hours", rationale: "Elevated weekly load is a meaningful outlier against standard utilization patterns." },
  { id: "R08", name: "Extreme Month", severity: "HIGH", condition: "Monthly total > 220 hours", rationale: "Monthly hours above this level are uncommon and may indicate systemic over-reporting or allocation issues." },
  { id: "R09", name: "High Month", severity: "MODERATE", condition: "Monthly total > 200 hours", rationale: "High monthly totals warrant verification of client allocation, overtime justification, and task realism." },
  { id: "R10", name: "Weekend Work", severity: "MODERATE", condition: "Entry on Saturday or Sunday", rationale: "Weekend work can be valid but should be checked for approval and policy alignment." },
  { id: "R11", name: "Holiday Overtime", severity: "HIGH", condition: "Holiday-tagged day with > 8 hours", rationale: "Extended holiday work is uncommon and may require explicit project or manager justification." },
  { id: "R12", name: "Suspicious Uniformity", severity: "MODERATE", condition: "Exactly 8.0h for > 15 consecutive workdays", rationale: "Perfectly repetitive patterns often indicate auto-filled timesheets rather than activity-based logging." },
  { id: "R13", name: "Missing Task", severity: "MODERATE", condition: "Hours logged but task field empty", rationale: "Hours without task description reduce auditability and make client/billing validation difficult." },
  { id: "R14", name: "Chronic Overtime", severity: "HIGH", condition: "> 5 consecutive workdays with daily total > 9h", rationale: "Persistent overtime suggests sustained risk patterns and should be investigated beyond a one-off day." },
  { id: "R15", name: "Burnout Pattern", severity: "CRITICAL", condition: "Weekend work + > 10h days + > 200h month in same period", rationale: "This compound pattern combines multiple stress signals and is a strong risk indicator for severe anomaly." },
  { id: "R16", name: "Escalating Hours", severity: "MODERATE", condition: "Weekly hours increasing for 4+ consecutive weeks", rationale: "A steadily rising workload over multiple weeks may indicate scope creep, overbilling, or unsustainable patterns." },
  { id: "R17", name: "Ghost Entry", severity: "HIGH", condition: "Hours logged on a date with no other employees working", rationale: "Entries on dates when nobody else logged work could indicate fabricated or misattributed timesheets." },
  { id: "R18", name: "Round-Number Bias", severity: "MODERATE", condition: "> 80% of entries are exact whole numbers", rationale: "Predominantly round numbers suggest estimation rather than actual time tracking — reduces audit reliability." },
  { id: "R19", name: "Duplicate Entry", severity: "HIGH", condition: "Same employee, date, task, and hours appears multiple times", rationale: "Identical entries are likely copy-paste errors or accidental double submissions." },
  { id: "R20", name: "Project Hopping", severity: "MODERATE", condition: "> 5 clients/projects in a single day", rationale: "Excessive context switching in one day is unusual and may indicate padding or misallocation of hours." },
];

const TECHNICAL_TERMS = [
  { term: "Composite Anomaly Score (0-100)", explanation: "A combined risk score from rules and models. Higher means the entry looks more unusual and needs more attention." },
  { term: "Severity (Critical/High/Moderate/Low)", explanation: "Priority level for action. Critical should be checked immediately. Low is usually normal activity." },
  { term: "Outlier / Anomaly", explanation: "A data point that differs strongly from normal behavior for that person, team, or dataset." },
  { term: "Z-Score", explanation: "How far an entry is from normal in standard-deviation units. Around 0 is normal; large absolute values are unusual." },
  { term: "Isolation Forest", explanation: "A model that finds entries that are globally unusual across the whole dataset." },
  { term: "Local Outlier Factor (LOF)", explanation: "A model that detects values unusual relative to their close neighbors (same context, task, department)." },
  { term: "DBSCAN", explanation: "A clustering method. Points marked as 'noise' do not fit natural clusters and may be suspicious." },
  { term: "Reviewer Model Probability", explanation: "Estimated chance a human reviewer would flag the entry based on historical reviewer decisions." },
  { term: "Billable Utilization", explanation: "Share of total logged hours tagged as client billable work versus internal/overhead work." },
  { term: "Task Rarity", explanation: "How uncommon a task is for a specific employee. Rare tasks can be valid but may need context." },
  { term: "Consecutive Overtime Days", explanation: "Number of back-to-back days above overtime thresholds. Long streaks often indicate sustained risk." },
];

const numberFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });
const PAGE_SIZE = 50;

function formatDate(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
}

function severityRank(value) {
  if (value === "CRITICAL") return 3;
  if (value === "HIGH") return 2;
  if (value === "MODERATE") return 1;
  return 0;
}

function buildHistogram(findings) {
  const bins = [];
  for (let start = 0; start < 100; start += 10) {
    bins.push({ bucket: `${start}-${start + 9}`, count: 0, start, end: start + 10 });
  }
  findings.forEach((row) => {
    const score = Number(row.composite_score || 0);
    const index = Math.min(9, Math.max(0, Math.floor(score / 10)));
    bins[index].count += 1;
  });
  return bins;
}

function buildDeptBreakdown(findings) {
  const map = new Map();
  findings.forEach((row) => {
    const dept = row.department || "Unknown";
    if (!map.has(dept)) {
      map.set(dept, { department: dept, CRITICAL: 0, HIGH: 0, MODERATE: 0, LOW: 0 });
    }
    map.get(dept)[row.severity] += 1;
  });
  return Array.from(map.values()).sort((a, b) => {
    const sumA = a.CRITICAL + a.HIGH + a.MODERATE + a.LOW;
    const sumB = b.CRITICAL + b.HIGH + b.MODERATE + b.LOW;
    return sumB - sumA;
  });
}

function buildTimeline(findings) {
  return findings
    .map((row) => ({
      ...row,
      ts: new Date(row.date).getTime(),
      scoreSize: Math.max(4, Number(row.composite_score || 0) / 20),
      hoursValue: Number(row.hours || 0),
    }))
    .filter((row) => Number.isFinite(row.ts))
    .sort((a, b) => a.ts - b.ts);
}

// ─── Chart Theme ─────────────────────────────────────

const CHART_THEME = {
  bg: "#0d1520",
  grid: "rgba(198, 133, 80, 0.06)",
  axis: "#475569",
  tooltip: {
    backgroundColor: "rgba(11, 17, 32, 0.95)",
    border: "1px solid rgba(198, 133, 80, 0.25)",
    borderRadius: "12px",
    color: "#f1f5f9",
    fontSize: "0.8125rem",
    padding: "8px 14px",
  },
};

// ─── Animated Number ───────────────────────────────────

function AnimatedNumber({ value, duration = 800 }) {
  const [display, setDisplay] = useState(0);
  const prev = useRef(0);

  useEffect(() => {
    const from = prev.current;
    const to = Number(value) || 0;
    if (from === to) {
      setDisplay(to);
      return;
    }
    const start = performance.now();
    let raf;
    const step = (now) => {
      const t = Math.min((now - start) / duration, 1);
      const ease = 1 - Math.pow(1 - t, 3);
      setDisplay(Math.round(from + (to - from) * ease));
      if (t < 1) raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    prev.current = to;
    return () => cancelAnimationFrame(raf);
  }, [value, duration]);

  return <>{numberFmt.format(display)}</>;
}

// ─── Upload Zone ──────────────────────────────────────

function UploadZone({ onFile, uploading }) {
  const [dragActive, setDragActive] = useState(false);
  const zoneRef = useRef(null);

  const handleFiles = (files) => {
    if (!files || files.length === 0) return;
    onFile(files[0]);
  };

  const handleMouseMove = useCallback((e) => {
    if (!zoneRef.current) return;
    const rect = zoneRef.current.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width - 0.5;
    const y = (e.clientY - rect.top) / rect.height - 0.5;
    zoneRef.current.style.transform = `perspective(800px) rotateY(${x * 6}deg) rotateX(${-y * 6}deg) translateY(-4px)`;
  }, []);

  const handleMouseLeave = useCallback(() => {
    if (zoneRef.current) {
      zoneRef.current.style.transform = "perspective(800px) rotateY(0deg) rotateX(0deg) translateY(0)";
    }
  }, []);

  return (
    <div
      ref={zoneRef}
      onDragOver={(e) => { e.preventDefault(); setDragActive(true); }}
      onDragLeave={(e) => { e.preventDefault(); setDragActive(false); }}
      onDrop={(e) => { e.preventDefault(); setDragActive(false); handleFiles(e.dataTransfer.files); }}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      className={`upload-zone animate-in ${dragActive ? "drag-active" : ""}`}
      style={{ padding: "4rem 2rem", textAlign: "center", cursor: "pointer" }}
    >
      <div style={{ maxWidth: "480px", margin: "0 auto" }}>
        <div className="upload-icon" style={{ marginBottom: "1.5rem", opacity: 0.9 }}>
          <svg width="56" height="56" viewBox="0 0 56 56" fill="none" style={{ margin: "0 auto" }}>
            <rect x="4" y="4" width="48" height="48" rx="16" fill="url(#uploadGrad)" fillOpacity="0.12" />
            <path d="M28 18v14m0 0l-5-5m5 5l5-5" stroke="url(#uploadGrad)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M20 36h16" stroke="url(#uploadGrad)" strokeWidth="2.5" strokeLinecap="round" />
            <defs>
              <linearGradient id="uploadGrad" x1="4" y1="4" x2="52" y2="52" gradientUnits="userSpaceOnUse">
                <stop stopColor="#e8612d" />
                <stop offset="1" stopColor="#f59e0b" />
              </linearGradient>
            </defs>
          </svg>
        </div>
        <p style={{ fontSize: "1.5rem", fontWeight: 600, color: "#f1f5f9", marginBottom: "0.5rem", letterSpacing: "-0.02em" }}>
          Drop your timesheet here
        </p>
        <p style={{ fontSize: "0.875rem", color: "#94a3b8", marginBottom: "1.5rem" }}>
          or click to browse — accepts .xlsx, .xls, .csv
        </p>
        <label className="btn-primary" style={{ display: "inline-flex", alignItems: "center", gap: "0.5rem", cursor: "pointer" }}>
          {uploading ? (
            <>
              <span style={{ display: "inline-block", width: "14px", height: "14px", border: "2px solid rgba(255,255,255,0.3)", borderTopColor: "#fff", borderRadius: "50%", animation: "borderRotate 0.8s linear infinite" }} />
              Uploading...
            </>
          ) : "Select File"}
          <input
            type="file"
            style={{ display: "none" }}
            accept=".xlsx,.xls,.csv"
            disabled={uploading}
            onChange={(e) => handleFiles(e.target.files)}
          />
        </label>
      </div>
    </div>
  );
}

// ─── Score Cards ──────────────────────────────────────

function ScoreCards({ summary }) {
  if (!summary) return null;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "1rem" }}>
      {SCORE_CARDS.map((card, i) => (
        <div
          key={card.key}
          className={`score-card animate-in animate-in-delay-${i + 1}`}
          style={{ "--card-accent": SEVERITY_COLORS[card.severity], "--card-glow": SEVERITY_GLOW[card.severity] }}
        >
          <div className="score-value" style={{ color: SEVERITY_COLORS[card.severity] }}>
            <AnimatedNumber value={summary[card.key] || 0} />
          </div>
          <div className="score-label">{card.label}</div>
        </div>
      ))}
      <div
        className="score-card animate-in animate-in-delay-5"
        style={{ "--card-accent": "#10b981", "--card-glow": "rgba(16, 185, 129, 0.15)" }}
      >
        <div className="score-value" style={{ color: "#10b981" }}>
          <AnimatedNumber value={Math.round(summary.billable_utilization || 0)} />
          <span style={{ fontSize: "1.2rem", opacity: 0.7 }}>%</span>
        </div>
        <div className="score-label">UTILIZATION</div>
      </div>
    </div>
  );
}

// ─── Skeleton Results ─────────────────────────────────

function SkeletonResults() {
  return (
    <div className="animate-in" style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "1rem" }}>
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="skeleton" style={{ height: "90px" }} />
        ))}
      </div>
      <div className="skeleton" style={{ height: "320px" }} />
      <div className="skeleton" style={{ height: "280px" }} />
    </div>
  );
}

// ─── Severity Badge ──────────────────────────────────

function SeverityBadge({ severity }) {
  return (
    <span
      className="severity-badge"
      style={{
        background: SEVERITY_BG[severity],
        borderColor: `${SEVERITY_COLORS[severity]}44`,
        color: SEVERITY_COLORS[severity],
      }}
    >
      {SEVERITY_ICON[severity]} {severity}
    </span>
  );
}

// ─── Explainers Page ─────────────────────────────────

function ExplainersPage({ onOpenAnalyzer }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>
      {/* Header */}
      <div className="glass-card animate-in" style={{ padding: "2rem" }}>
        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "1rem" }}>
          <div>
            <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: "0.6875rem", textTransform: "uppercase", letterSpacing: "0.15em", color: "var(--accent-orange)", marginBottom: "0.5rem" }}>
              User Guide
            </p>
            <h2 className="gradient-text" style={{ fontSize: "1.75rem", fontWeight: 700, letterSpacing: "-0.02em" }}>
              Technical Terms & Why They Matter
            </h2>
            <p style={{ fontSize: "0.875rem", color: "var(--text-secondary)", marginTop: "0.5rem" }}>
              Every non-obvious term used in TimesheetIQ, explained in plain language.
            </p>
          </div>
          <button onClick={onOpenAnalyzer} className="btn-secondary">
            ← Back to Analyzer
          </button>
        </div>
      </div>

      {/* Rules */}
      <div className="glass-card animate-in animate-in-delay-1" style={{ padding: "2rem" }}>
        <h3 className="section-accent" style={{ fontSize: "1.25rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: "0.5rem" }}>
          Rule Engine (R01–R20)
        </h3>
        <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", marginBottom: "1.25rem" }}>
          Deterministic checks. If a condition matches, the entry is flagged even if ML confidence is low.
        </p>
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          {RULE_EXPLANATIONS.map((rule, i) => (
            <div key={rule.id} className={`rule-card animate-in animate-in-delay-${Math.min(i % 6 + 1, 6)}`}>
              <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "0.5rem", marginBottom: "0.75rem" }}>
                <span style={{ fontWeight: 600, color: "var(--text-primary)", fontSize: "0.9375rem" }}>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", color: "var(--accent-orange)", marginRight: "0.5rem" }}>{rule.id}</span>
                  {rule.name}
                </span>
                <SeverityBadge severity={rule.severity} />
              </div>
              <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", marginBottom: "0.35rem" }}>
                <span style={{ color: "var(--text-muted)", fontWeight: 600 }}>Trigger:</span> {rule.condition}
              </p>
              <p style={{ fontSize: "0.8125rem", color: "var(--text-muted)" }}>
                <span style={{ fontWeight: 600 }}>Why:</span> {rule.rationale}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Glossary */}
      <div className="glass-card animate-in animate-in-delay-2" style={{ padding: "2rem" }}>
        <h3 className="section-accent" style={{ fontSize: "1.25rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: "0.5rem" }}>
          Model & Score Glossary
        </h3>
        <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)", marginBottom: "1.25rem" }}>
          Use this when reading table columns, expanded finding details, and chart views.
        </p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: "0.75rem" }}>
          {TECHNICAL_TERMS.map((item) => (
            <div key={item.term} className="rule-card">
              <p style={{ fontWeight: 600, color: "var(--text-primary)", fontSize: "0.875rem", marginBottom: "0.35rem" }}>{item.term}</p>
              <p style={{ fontSize: "0.8125rem", color: "var(--text-muted)" }}>{item.explanation}</p>
            </div>
          ))}
        </div>
      </div>

      {/* How to Read */}
      <div className="glass-card animate-in animate-in-delay-3" style={{ padding: "2rem" }}>
        <h3 className="section-accent" style={{ fontSize: "1.25rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: "1rem" }}>
          How to Read a Finding
        </h3>
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          {[
            { step: "01", text: "Start with Severity to know urgency." },
            { step: "02", text: "Check Rules Triggered to see hard policy conditions." },
            { step: "03", text: "Review Composite Score and ML Bars to understand model confidence." },
            { step: "04", text: "Read Context (employee mean and z-scores) before final decision." },
          ].map((item) => (
            <div key={item.step} style={{ display: "flex", alignItems: "flex-start", gap: "1rem" }}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: "0.8125rem", color: "var(--accent-orange)", fontWeight: 700, minWidth: "28px" }}>
                {item.step}
              </span>
              <p style={{ fontSize: "0.875rem", color: "var(--text-secondary)", margin: 0 }}>{item.text}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Plain Language Help ─────────────────────────────

function PlainLanguageHelp({ onOpenExplanations }) {
  return (
    <div className="glass-card animate-in" style={{ padding: "1.75rem" }}>
      <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "1rem", marginBottom: "1.25rem" }}>
        <div>
          <h3 style={{ fontSize: "1.125rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: "0.35rem" }}>
            Need a Plain-Language Explanation?
          </h3>
          <p style={{ fontSize: "0.8125rem", color: "var(--text-secondary)" }}>
            Key terms explained below. Open the full guide for all rules and rationale.
          </p>
        </div>
        <button onClick={onOpenExplanations} className="btn-primary">
          Open Full Guide →
        </button>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: "0.75rem" }}>
        {TECHNICAL_TERMS.slice(0, 8).map((item) => (
          <div key={item.term} className="rule-card">
            <p style={{ fontWeight: 600, color: "var(--text-primary)", fontSize: "0.8125rem", marginBottom: "0.25rem" }}>{item.term}</p>
            <p style={{ fontSize: "0.8125rem", color: "var(--text-muted)" }}>{item.explanation}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Custom Chart Tooltip ────────────────────────────

function ChartTooltipContent({ active, payload, label, formatter, labelFormatter }) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div style={CHART_THEME.tooltip}>
      {label != null && (
        <p style={{ fontSize: "0.75rem", color: "#94a3b8", marginBottom: "4px" }}>
          {labelFormatter ? labelFormatter(label) : label}
        </p>
      )}
      {payload.map((entry, i) => {
        const [val, name] = formatter ? formatter(entry.value, entry.name || entry.dataKey) : [entry.value, entry.name || entry.dataKey];
        return (
          <p key={i} style={{ color: entry.color || "#00d4ff", fontSize: "0.8125rem", fontWeight: 600 }}>
            {Array.isArray(val) ? val[0] : val} {Array.isArray(val) ? "" : name}
          </p>
        );
      })}
    </div>
  );
}

// ─── Main App ────────────────────────────────────────

function App() {
  const [apiBase, setApiBase] = useState(API_ENV_BASE);
  const [uploadId, setUploadId] = useState(null);
  const [uploadName, setUploadName] = useState("");
  const [status, setStatus] = useState("idle");
  const [statusMessage, setStatusMessage] = useState("");
  const [summary, setSummary] = useState(null);
  const [findings, setFindings] = useState([]);
  const [error, setError] = useState("");
  const [severityFilter, setSeverityFilter] = useState("ALL");
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [departmentFilter, setDepartmentFilter] = useState("ALL");
  const [employeeFilter, setEmployeeFilter] = useState("ALL");
  const [sortBy, setSortBy] = useState("composite_score");
  const [sortDir, setSortDir] = useState("desc");
  const [expandedId, setExpandedId] = useState(null);
  const [chartTab, setChartTab] = useState("distribution");
  const [activePage, setActivePage] = useState("analyzer");
  const [currentPage, setCurrentPage] = useState(0);
  const [showFlaggedOnly, setShowFlaggedOnly] = useState(true);
  const pollFailureRef = useRef(0);
  const searchTimerRef = useRef(null);

  // Debounce search input by 300ms
  const handleSearchChange = useCallback((value) => {
    setSearch(value);
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(() => setDebouncedSearch(value), 300);
  }, []);

  const discoverApiBase = async () => {
    if (apiBase) return apiBase;
    for (const candidate of API_CANDIDATES) {
      try {
        const probe = await fetchWithTimeout(`${candidate}/api/health`, {}, 2500);
        if (!probe.ok) continue;
        const payload = await probe.json().catch(() => null);
        const isTimesheetIq =
          payload &&
          payload.status === "ok" &&
          (payload.service === "timesheetiq" || payload.service === undefined);
        if (isTimesheetIq) {
          setApiBase(candidate);
          return candidate;
        }
      } catch (_) { }
    }
    throw new Error("Cannot reach the backend API. Start FastAPI and/or set VITE_API_BASE to your backend URL.");
  };

  const apiFetch = async (path, options = {}, timeoutMs = 60000) => {
    const base = await discoverApiBase();
    return fetchWithTimeout(`${base}${path}`, options, timeoutMs);
  };

  useEffect(() => {
    const warmup = async () => {
      try {
        await discoverApiBase();
        setError("");
      } catch (err) {
        setError(err.message || "Cannot connect to backend.");
      }
    };
    warmup();
  }, []);

  useEffect(() => {
    const pollStatus = async () => {
      if (!uploadId || status !== "processing") return;
      try {
        const res = await apiFetch(`/api/status/${uploadId}`);
        if (!res.ok) throw new Error(await getErrorMessage(res, "Failed to fetch status."));
        const payload = await res.json();
        pollFailureRef.current = 0;
        setStatusMessage(
          payload.status === "processing"
            ? "Analyzing entries and calculating anomaly signals..."
            : payload.status === "completed"
              ? "Analysis complete. Loading findings..."
              : payload.error_message || "Analysis failed."
        );
        if (payload.status === "completed") {
          setStatus("loading_summary");
        } else if (payload.status === "failed") {
          setStatus("failed");
          setError(payload.error_message || "Analysis failed.");
        }
      } catch (err) {
        pollFailureRef.current += 1;
        if (pollFailureRef.current >= 6) {
          setStatus("failed");
          setError(err.message || "Status polling failed.");
        } else {
          setStatusMessage("Temporary connection issue while polling status. Retrying...");
        }
      }
    };
    if (status !== "processing") return undefined;
    pollStatus();
    const interval = setInterval(pollStatus, 1500);
    return () => clearInterval(interval);
  }, [uploadId, status, apiBase]);

  useEffect(() => {
    const loadSummary = async () => {
      if (!uploadId || status !== "loading_summary") return;
      try {
        const res = await apiFetch(`/api/summary/${uploadId}`);
        if (!res.ok) throw new Error(await getErrorMessage(res, "Failed to load summary."));
        const payload = await res.json();
        setSummary(payload);
        setStatus("loading_findings");
      } catch (err) {
        setStatus("failed");
        setError(err.message || "Summary load failed.");
      }
    };
    loadSummary();
  }, [uploadId, status, apiBase]);

  useEffect(() => {
    const loadFindings = async () => {
      if (!uploadId || status !== "loading_findings") return;
      try {
        const res = await apiFetch(`/api/findings/${uploadId}?limit=50000`, {}, 120000);
        if (!res.ok) throw new Error(await getErrorMessage(res, "Failed to load findings."));
        const payload = await res.json();
        setFindings(payload.items || []);
        setCurrentPage(0);
        setStatus("ready");
      } catch (err) {
        setStatus("failed");
        setError(err.message || "Findings load failed.");
      }
    };
    loadFindings();
  }, [uploadId, status, apiBase]);

  const departments = useMemo(
    () => ["ALL", ...new Set(findings.map((x) => x.department || "Unknown"))],
    [findings]
  );

  const employees = useMemo(
    () => ["ALL", ...new Set(findings.map((x) => x.employee || "Unknown"))],
    [findings]
  );

  const filteredFindings = useMemo(() => {
    let rows = findings.slice();
    if (showFlaggedOnly) rows = rows.filter((x) => x.ai_recommended);
    if (severityFilter !== "ALL") rows = rows.filter((x) => x.severity === severityFilter);
    if (departmentFilter !== "ALL") rows = rows.filter((x) => x.department === departmentFilter);
    if (employeeFilter !== "ALL") rows = rows.filter((x) => x.employee === employeeFilter);
    if (debouncedSearch.trim()) {
      const q = debouncedSearch.trim().toLowerCase();
      rows = rows.filter((x) =>
        [x.employee, x.department, x.task, x.client, x.explanation].join(" ").toLowerCase().includes(q)
      );
    }
    rows.sort((a, b) => {
      let left = a[sortBy];
      let right = b[sortBy];
      if (sortBy === "date") { left = new Date(left).getTime(); right = new Date(right).getTime(); }
      if (sortBy === "severity") { left = severityRank(left); right = severityRank(right); }
      if (left === right) return 0;
      if (sortDir === "asc") return left > right ? 1 : -1;
      return left < right ? 1 : -1;
    });
    return rows;
  }, [findings, showFlaggedOnly, severityFilter, debouncedSearch, departmentFilter, employeeFilter, sortBy, sortDir]);

  // Reset page when filters change
  useEffect(() => { setCurrentPage(0); }, [showFlaggedOnly, severityFilter, debouncedSearch, departmentFilter, employeeFilter]);

  const totalPages = Math.max(1, Math.ceil(filteredFindings.length / PAGE_SIZE));
  const pagedFindings = useMemo(
    () => filteredFindings.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE),
    [filteredFindings, currentPage]
  );

  const histogram = useMemo(() => buildHistogram(findings), [findings]);
  const deptBreakdown = useMemo(() => buildDeptBreakdown(findings), [findings]);
  const timeline = useMemo(() => buildTimeline(findings), [findings]);

  const selectedRow = useMemo(
    () => findings.find((row) => row.id === expandedId) || null,
    [findings, expandedId]
  );

  const employeeTrend = useMemo(() => {
    if (!selectedRow) return [];
    const byDate = new Map();
    findings
      .filter((x) => x.employee === selectedRow.employee)
      .forEach((x) => {
        const key = x.date;
        if (!byDate.has(key)) byDate.set(key, { date: key, dailyHours: 0 });
        byDate.get(key).dailyHours += Number(x.hours || 0);
      });
    return Array.from(byDate.values()).sort((a, b) => new Date(a.date) - new Date(b.date));
  }, [findings, selectedRow]);

  const uploadFile = async (file) => {
    setError("");
    setStatus("uploading");
    setUploadName(file.name);
    setSummary(null);
    setFindings([]);
    setExpandedId(null);
    const formData = new FormData();
    formData.append("file", file);
    try {
      const res = await apiFetch(`/api/upload`, { method: "POST", body: formData }, 120000);
      if (!res.ok) throw new Error(await getErrorMessage(res, "Upload failed."));
      const payload = await res.json();
      setUploadId(payload.upload_id);
      setStatus("processing");
      setStatusMessage("Upload received. Starting analysis...");
    } catch (err) {
      setStatus("failed");
      setError(err.message || "Upload failed.");
    }
  };

  const exportReport = async () => {
    if (!uploadId) return;
    try {
      const res = await apiFetch(`/api/export/${uploadId}`, {}, 120000);
      if (!res.ok) throw new Error(await getErrorMessage(res, "Export failed."));
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `timesheetiq_report_${uploadId}.xlsx`;
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      setError(err.message || "Export failed.");
    }
  };

  const toggleSort = (column) => {
    if (sortBy === column) {
      setSortDir((dir) => (dir === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(column);
      setSortDir(column === "employee" || column === "department" ? "asc" : "desc");
    }
  };

  const loading = ["uploading", "processing", "loading_summary", "loading_findings"].includes(status);

  // ─── Render ──────────────────────────────────────

  return (
    <div style={{ minHeight: "100vh", position: "relative" }}>
      {/* Ambient background glow */}
      <div className="ambient-glow" />

      <div style={{ position: "relative", zIndex: 1, maxWidth: "1280px", margin: "0 auto", padding: "2rem 1.5rem" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>

          {/* ── Header ─────────────────────────────── */}
          <header className="glass-dark animate-in" style={{ borderRadius: "var(--radius-xl)", padding: "1.75rem 2rem", position: "relative", overflow: "hidden" }}>
            {/* Gradient accent line */}
            <div style={{ position: "absolute", bottom: 0, left: 0, right: 0, height: "2px", background: "linear-gradient(90deg, var(--accent-orange), var(--accent-warm), var(--accent-orange-light))", opacity: 0.7 }} />

            <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "1rem" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "1.25rem" }}>
                <img src="/oxygene-logo.png" alt="Oxygène" style={{ height: "40px", width: "auto", objectFit: "contain" }} />
                <div>
                  <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: "0.6875rem", textTransform: "uppercase", letterSpacing: "0.2em", color: "var(--accent-orange)", marginBottom: "0.35rem" }}>
                    TimesheetIQ
                  </p>
                  <h1 className="gradient-text" style={{ fontSize: "clamp(1.35rem, 2.8vw, 1.85rem)", fontWeight: 700, letterSpacing: "-0.03em", margin: 0 }}>
                    AI-Powered Timesheet Anomaly Detection
                  </h1>
                  <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: "0.6rem", textTransform: "uppercase", letterSpacing: "0.18em", color: "var(--text-muted)", marginTop: "0.4rem" }}>
                    Precise. Transparent. Accountable.
                  </p>
                </div>
              </div>

              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", flexWrap: "wrap" }}>
                <button
                  onClick={() => setActivePage("analyzer")}
                  className={`nav-pill ${activePage === "analyzer" ? "active" : ""}`}
                >
                  Analyzer
                </button>
                <button
                  onClick={() => setActivePage("explanations")}
                  className={`nav-pill ${activePage === "explanations" ? "active" : ""}`}
                >
                  Explanations
                </button>
                <button
                  onClick={exportReport}
                  disabled={!uploadId || loading}
                  className="btn-primary"
                  style={{ marginLeft: "0.5rem" }}
                >
                  ↓ Download Report
                </button>
              </div>
            </div>

            {uploadName && (
              <p style={{ fontSize: "0.8125rem", color: "var(--text-muted)", marginTop: "1rem" }}>
                Current file: <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{uploadName}</span>
              </p>
            )}
            {statusMessage && (
              <p style={{ fontSize: "0.8125rem", color: "var(--accent-orange)", marginTop: "0.5rem", display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <span style={{ display: "inline-block", width: "8px", height: "8px", borderRadius: "50%", background: "var(--accent-orange)", animation: "pulseGlow 1.5s infinite" }} />
                {statusMessage}
              </p>
            )}
            {error && (
              <p style={{ fontSize: "0.8125rem", color: "#ef4444", marginTop: "0.5rem" }}>
                ⚠ {error}
              </p>
            )}
          </header>

          {/* ── Content ────────────────────────────── */}
          {activePage === "explanations" ? (
            <ExplainersPage onOpenAnalyzer={() => setActivePage("analyzer")} />
          ) : (
            <>
              {!uploadId && <UploadZone onFile={uploadFile} uploading={loading} />}
              {loading && uploadId && <SkeletonResults />}

              {!loading && summary && (
                <>
                  <ScoreCards summary={summary} />

                  {/* Flag Statistics Banner */}
                  {summary.summary_json?.flagged_count != null && (
                    <div className="glass-card animate-in animate-in-delay-1" style={{ padding: "1.25rem 1.75rem", display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "1rem" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
                        <span style={{ fontSize: "1.5rem" }}>🔍</span>
                        <div>
                          <p style={{ fontSize: "0.9375rem", fontWeight: 600, color: "var(--text-primary)", margin: 0 }}>
                            <span style={{ color: "var(--accent-orange)" }}>{summary.summary_json.flagged_count.toLocaleString()}</span> entries flagged for review
                            <span style={{ color: "var(--text-muted)", fontWeight: 400 }}> out of {summary.total_entries.toLocaleString()} total ({summary.summary_json.flagged_pct}%)</span>
                          </p>
                          <p style={{ fontSize: "0.8125rem", color: "var(--text-muted)", margin: "0.25rem 0 0" }}>
                            Estimated <span style={{ color: "var(--accent-warm)", fontWeight: 600 }}>{numberFmt.format(summary.summary_json.flagged_hours)}</span> hours at risk
                          </p>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Top Risk Employees */}
                  {summary.summary_json?.top_risk_employees?.length > 0 && (
                    <div className="glass-card animate-in animate-in-delay-2" style={{ padding: "1.5rem" }}>
                      <h3 className="section-accent" style={{ fontSize: "1rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: "1rem" }}>
                        Top Risk Employees
                      </h3>
                      <div className="data-table-wrap" style={{ maxHeight: "18rem", overflow: "auto" }}>
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>Employee</th>
                              <th>Risk Score</th>
                              <th>Avg Score</th>
                              <th>Max Score</th>
                              <th>Flagged</th>
                              <th>Total</th>
                            </tr>
                          </thead>
                          <tbody>
                            {summary.summary_json.top_risk_employees.map((emp) => (
                              <tr key={emp.employee}>
                                <td style={{ color: "var(--text-primary)", fontWeight: 500 }}>{emp.employee}</td>
                                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontWeight: 600, color: emp.risk_score >= 65 ? "#ef4444" : emp.risk_score >= 45 ? "#f97316" : "var(--text-secondary)" }}>
                                  {numberFmt.format(emp.risk_score)}
                                </td>
                                <td style={{ fontFamily: "'JetBrains Mono', monospace" }}>{numberFmt.format(emp.mean_score)}</td>
                                <td style={{ fontFamily: "'JetBrains Mono', monospace" }}>{numberFmt.format(emp.max_score)}</td>
                                <td style={{ fontFamily: "'JetBrains Mono', monospace", color: "var(--accent-orange)" }}>{emp.flagged_entries}</td>
                                <td style={{ fontFamily: "'JetBrains Mono', monospace" }}>{emp.total_entries}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}

                  {/* Findings Table */}
                  <section className="glass-card animate-in animate-in-delay-2" style={{ padding: "1.5rem" }}>
                    {/* Filters */}
                    <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "0.75rem", marginBottom: "1.25rem" }}>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.35rem", alignItems: "center" }}>
                        <button
                          onClick={() => setShowFlaggedOnly(!showFlaggedOnly)}
                          className={`filter-chip ${showFlaggedOnly ? "active" : ""}`}
                          style={showFlaggedOnly ? { background: "rgba(232, 97, 45, 0.15)", borderColor: "rgba(232, 97, 45, 0.4)" } : {}}
                        >
                          {showFlaggedOnly ? "🔍 AI Flagged" : "📋 All Entries"}
                        </button>
                        <span style={{ width: "1px", height: "20px", background: "var(--border-subtle)", margin: "0 0.25rem" }} />
                        {["ALL", "CRITICAL", "HIGH", "MODERATE", "LOW"].map((sev) => (
                          <button
                            key={sev}
                            onClick={() => setSeverityFilter(sev)}
                            className={`filter-chip ${severityFilter === sev ? "active" : ""}`}
                          >
                            {sev === "ALL" ? "All" : `${SEVERITY_ICON[sev]} ${sev}`}
                          </button>
                        ))}
                      </div>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
                        <input
                          value={search}
                          onChange={(e) => handleSearchChange(e.target.value)}
                          placeholder="Search employee, task, client..."
                          className="input-field"
                          style={{ minWidth: "200px" }}
                        />
                        <select value={departmentFilter} onChange={(e) => setDepartmentFilter(e.target.value)} className="input-field">
                          {departments.map((dept) => <option key={dept} value={dept}>{dept}</option>)}
                        </select>
                        <select value={employeeFilter} onChange={(e) => setEmployeeFilter(e.target.value)} className="input-field">
                          {employees.map((emp) => <option key={emp} value={emp}>{emp}</option>)}
                        </select>
                      </div>
                    </div>

                    {/* Table */}
                    <div className="data-table-wrap" style={{ maxHeight: "34rem", overflow: "auto" }}>
                      <table className="data-table">
                        <thead>
                          <tr>
                            {[
                              { key: "severity", label: "Sev" },
                              { key: "employee", label: "Employee" },
                              { key: "department", label: "Department" },
                              { key: "date", label: "Date" },
                              { key: "hours", label: "Hours" },
                              { key: "composite_score", label: "Score" },
                              { key: "explanation", label: "Explanation" },
                            ].map((col) => (
                              <th key={col.key} onClick={() => toggleSort(col.key)}>
                                {col.label}
                                {sortBy === col.key && (
                                  <span style={{ marginLeft: "4px", opacity: 0.5 }}>
                                    {sortDir === "asc" ? "↑" : "↓"}
                                  </span>
                                )}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {pagedFindings.map((row) => (
                            <React.Fragment key={row.id}>
                              <tr onClick={() => setExpandedId(expandedId === row.id ? null : row.id)}>
                                <td>
                                  <span style={{ color: SEVERITY_COLORS[row.severity] }}>{SEVERITY_ICON[row.severity]}</span>
                                  {row.model_agreement >= 3 && (
                                    <span style={{ fontSize: "0.6rem", color: "var(--accent-copper)", marginLeft: "4px", fontFamily: "'JetBrains Mono', monospace" }}>
                                      {row.model_agreement}/5
                                    </span>
                                  )}
                                </td>
                                <td style={{ color: "var(--text-primary)", fontWeight: 500 }}>{row.employee}</td>
                                <td>{row.department}</td>
                                <td>{formatDate(row.date)}</td>
                                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontWeight: 500 }}>{numberFmt.format(Number(row.hours || 0))}</td>
                                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontWeight: 600, color: SEVERITY_COLORS[row.severity] }}>
                                  {numberFmt.format(Number(row.composite_score || 0))}
                                </td>
                                <td style={{ maxWidth: "320px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                  {row.flag_reason || row.explanation}
                                </td>
                              </tr>
                              {expandedId === row.id && (
                                <tr className="expanded-row">
                                  <td colSpan={7}>
                                    <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
                                      <div>
                                        <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: "0.6875rem", textTransform: "uppercase", letterSpacing: "0.1em", color: "var(--text-muted)", marginBottom: "0.5rem" }}>
                                          Full Explanation
                                        </p>
                                        <pre style={{ whiteSpace: "pre-wrap", fontSize: "0.8125rem", color: "var(--text-secondary)", margin: 0, lineHeight: 1.6 }}>
                                          {row.explanation}
                                        </pre>
                                      </div>
                                      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.35rem" }}>
                                        {(row.rules_triggered || []).map((rule) => (
                                          <span key={rule} style={{
                                            fontFamily: "'JetBrains Mono', monospace",
                                            fontSize: "0.6875rem",
                                            fontWeight: 600,
                                            padding: "0.25rem 0.65rem",
                                            borderRadius: "8px",
                                            background: "rgba(232, 97, 45, 0.1)",
                                            color: "var(--accent-orange)",
                                            border: "1px solid rgba(232, 97, 45, 0.2)",
                                          }}>
                                            {rule}
                                          </span>
                                        ))}
                                      </div>
                                      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))", gap: "1rem" }}>
                                        <div style={{ height: "180px", borderRadius: "16px", border: "1px solid var(--border-subtle)", background: CHART_THEME.bg, padding: "0.75rem" }}>
                                          <ResponsiveContainer width="100%" height="100%">
                                            <BarChart data={[
                                              { model: "IF", score: Number(row.ml_scores?.isolation_forest || 0) },
                                              { model: "LOF", score: Number(row.ml_scores?.lof || 0) },
                                              { model: "DB", score: Number(row.ml_scores?.dbscan || 0) },
                                              { model: "Z", score: Number(row.ml_scores?.zscore || 0) / 4 },
                                              { model: "REV", score: Number(row.ml_scores?.reviewer || 0) },
                                            ]}>
                                              <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.grid} />
                                              <XAxis dataKey="model" tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                                              <YAxis domain={[0, 1]} tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                                              <Tooltip contentStyle={CHART_THEME.tooltip} cursor={{ fill: "rgba(232, 97, 45, 0.05)" }} />
                                              <Bar dataKey="score" fill="#e8612d" radius={[4, 4, 0, 0]} />
                                            </BarChart>
                                          </ResponsiveContainer>
                                        </div>
                                        <div style={{ height: "180px", borderRadius: "16px", border: "1px solid var(--border-subtle)", background: CHART_THEME.bg, padding: "0.75rem" }}>
                                          <ResponsiveContainer width="100%" height="100%">
                                            <LineChart data={employeeTrend}>
                                              <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.grid} />
                                              <XAxis dataKey="date" tickFormatter={(d) => formatDate(d).slice(0, 6)} tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                                              <YAxis tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                                              <Tooltip contentStyle={CHART_THEME.tooltip} formatter={(value) => numberFmt.format(value)} labelFormatter={(label) => formatDate(label)} />
                                              <Line type="monotone" dataKey="dailyHours" stroke="#f59e0b" strokeWidth={2} dot={false} />
                                            </LineChart>
                                          </ResponsiveContainer>
                                        </div>
                                      </div>
                                    </div>
                                  </td>
                                </tr>
                              )}
                            </React.Fragment>
                          ))}
                          {filteredFindings.length === 0 && (
                            <tr>
                              <td colSpan={7} style={{ textAlign: "center", padding: "2.5rem 1rem", color: "var(--text-muted)" }}>
                                No findings match your filters.
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>

                    {/* Pagination Controls */}
                    {filteredFindings.length > PAGE_SIZE && (
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: "1rem", padding: "0.75rem 0.5rem", borderTop: "1px solid var(--border-subtle)" }}>
                        <span style={{ fontSize: "0.8125rem", color: "var(--text-muted)" }}>
                          Showing {currentPage * PAGE_SIZE + 1}–{Math.min((currentPage + 1) * PAGE_SIZE, filteredFindings.length)} of {filteredFindings.length.toLocaleString()} findings
                        </span>
                        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                          <button
                            onClick={() => setCurrentPage(0)}
                            disabled={currentPage === 0}
                            className="btn-secondary"
                            style={{ padding: "0.35rem 0.75rem", fontSize: "0.8125rem" }}
                          >
                            ⟪ First
                          </button>
                          <button
                            onClick={() => setCurrentPage((p) => Math.max(0, p - 1))}
                            disabled={currentPage === 0}
                            className="btn-secondary"
                            style={{ padding: "0.35rem 0.75rem", fontSize: "0.8125rem" }}
                          >
                            ← Prev
                          </button>
                          <span style={{ fontSize: "0.8125rem", color: "var(--text-primary)", fontWeight: 600, padding: "0 0.5rem" }}>
                            Page {currentPage + 1} of {totalPages}
                          </span>
                          <button
                            onClick={() => setCurrentPage((p) => Math.min(totalPages - 1, p + 1))}
                            disabled={currentPage >= totalPages - 1}
                            className="btn-secondary"
                            style={{ padding: "0.35rem 0.75rem", fontSize: "0.8125rem" }}
                          >
                            Next →
                          </button>
                          <button
                            onClick={() => setCurrentPage(totalPages - 1)}
                            disabled={currentPage >= totalPages - 1}
                            className="btn-secondary"
                            style={{ padding: "0.35rem 0.75rem", fontSize: "0.8125rem" }}
                          >
                            Last ⟫
                          </button>
                        </div>
                      </div>
                    )}
                  </section>

                  {/* Charts Section */}
                  <section className="glass-card animate-in animate-in-delay-3" style={{ padding: "1.5rem" }}>
                    <div style={{ display: "flex", gap: "0.25rem", marginBottom: "1.25rem", borderBottom: "1px solid var(--border-subtle)", paddingBottom: "0.75rem" }}>
                      {[
                        { key: "distribution", label: "Anomaly Distribution" },
                        { key: "department", label: "Department Breakdown" },
                        { key: "timeline", label: "Timeline" },
                      ].map((tab) => (
                        <button
                          key={tab.key}
                          onClick={() => setChartTab(tab.key)}
                          className={`chart-tab ${chartTab === tab.key ? "active" : ""}`}
                        >
                          {tab.label}
                        </button>
                      ))}
                    </div>

                    {chartTab === "distribution" && (
                      <div style={{ height: "320px", borderRadius: "16px", background: CHART_THEME.bg, padding: "1rem", border: "1px solid var(--border-subtle)" }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={histogram}>
                            <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.grid} />
                            <XAxis dataKey="bucket" tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                            <YAxis tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                            <Tooltip contentStyle={CHART_THEME.tooltip} cursor={{ fill: "rgba(0, 212, 255, 0.05)" }} />
                            <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                              {histogram.map((bin) => {
                                const color =
                                  bin.start >= 85 ? SEVERITY_COLORS.CRITICAL
                                    : bin.start >= 65 ? SEVERITY_COLORS.HIGH
                                      : bin.start >= 45 ? SEVERITY_COLORS.MODERATE
                                        : SEVERITY_COLORS.LOW;
                                return <Cell key={bin.bucket} fill={color} />;
                              })}
                            </Bar>
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    )}

                    {chartTab === "department" && (
                      <div style={{ height: "320px", borderRadius: "16px", background: CHART_THEME.bg, padding: "1rem", border: "1px solid var(--border-subtle)" }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={deptBreakdown}>
                            <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.grid} />
                            <XAxis dataKey="department" interval={0} angle={-20} textAnchor="end" height={70} tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                            <YAxis tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                            <Tooltip contentStyle={CHART_THEME.tooltip} />
                            <Legend wrapperStyle={{ fontSize: "0.75rem", color: "var(--text-muted)" }} />
                            <Bar dataKey="CRITICAL" stackId="a" fill={SEVERITY_COLORS.CRITICAL} radius={[0, 0, 0, 0]} />
                            <Bar dataKey="HIGH" stackId="a" fill={SEVERITY_COLORS.HIGH} />
                            <Bar dataKey="MODERATE" stackId="a" fill={SEVERITY_COLORS.MODERATE} />
                            <Bar dataKey="LOW" stackId="a" fill={SEVERITY_COLORS.LOW} radius={[4, 4, 0, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    )}

                    {chartTab === "timeline" && (
                      <div style={{ height: "320px", borderRadius: "16px", background: CHART_THEME.bg, padding: "1rem", border: "1px solid var(--border-subtle)" }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <ScatterChart>
                            <CartesianGrid stroke={CHART_THEME.grid} />
                            <XAxis
                              type="number"
                              dataKey="ts"
                              domain={["dataMin", "dataMax"]}
                              tickFormatter={(ts) => formatDate(new Date(ts))}
                              tick={{ fill: CHART_THEME.axis, fontSize: 11 }}
                              axisLine={{ stroke: CHART_THEME.grid }}
                            />
                            <YAxis type="number" dataKey="hoursValue" tick={{ fill: CHART_THEME.axis, fontSize: 11 }} axisLine={{ stroke: CHART_THEME.grid }} />
                            <Tooltip
                              contentStyle={CHART_THEME.tooltip}
                              cursor={{ strokeDasharray: "3 3", stroke: "rgba(148, 163, 184, 0.2)" }}
                              formatter={(value, key) =>
                                key === "hoursValue" ? [`${numberFmt.format(value)}h`, "Hours"] : [value, key]
                              }
                              labelFormatter={(label) => formatDate(new Date(label))}
                            />
                            <Scatter
                              data={timeline}
                              shape={(props) => {
                                const { cx, cy, payload } = props;
                                return (
                                  <circle
                                    cx={cx}
                                    cy={cy}
                                    r={payload.scoreSize}
                                    fill={SEVERITY_COLORS[payload.severity]}
                                    fillOpacity={0.75}
                                    stroke={SEVERITY_COLORS[payload.severity]}
                                    strokeOpacity={0.3}
                                    strokeWidth={payload.scoreSize * 0.6}
                                  />
                                );
                              }}
                            />
                          </ScatterChart>
                        </ResponsiveContainer>
                      </div>
                    )}
                  </section>

                  <PlainLanguageHelp onOpenExplanations={() => setActivePage("explanations")} />
                </>
              )}

              {!loading && !summary && !uploadId && (
                <div className="glass-card animate-in animate-in-delay-1" style={{ padding: "2rem", textAlign: "center" }}>
                  <div style={{ maxWidth: "420px", margin: "0 auto" }}>
                    <h3 style={{ fontSize: "1.25rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: "0.75rem" }}>
                      New to TimesheetIQ?
                    </h3>
                    <p style={{ fontSize: "0.875rem", color: "var(--text-secondary)", lineHeight: 1.6, marginBottom: "1.25rem" }}>
                      Explore the full plain-language guide on all rules, model scores, and decision rationale before analyzing your first sheet.
                    </p>
                    <button onClick={() => setActivePage("explanations")} className="btn-primary">
                      Explore Explanations →
                    </button>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Footer */}
          <footer className="animate-in" style={{ textAlign: "center", padding: "1.5rem 0 0.5rem", color: "var(--text-muted)", fontSize: "0.75rem", letterSpacing: "0.03em" }}>
            TimesheetIQ · AI-Powered Anomaly Detection · Oxygène
          </footer>
        </div>
      </div>
    </div>
  );
}

export default App;
