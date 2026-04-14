from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import LocalOutlierFactor, NearestNeighbors
from sklearn.preprocessing import StandardScaler
from sklearn.semi_supervised import LabelSpreading

IF_FEATURES = [
    "hours",
    "daily_total_hours",
    "daily_entry_count",
    "emp_z_score_daily",
    "dept_z_score_daily",
    "entry_z_score",
    "is_weekend",
    "weekly_total_hours",
    "monthly_total_hours",
]

LOF_FEATURES = IF_FEATURES + ["task_rarity_score", "consecutive_overtime_days"]
DBSCAN_FEATURES = ["hours", "daily_total_hours", "emp_z_score_daily", "is_weekend"]

REVIEWER_NUMERIC_FEATURES = [
    "hours",
    "daily_total_hours",
    "daily_entry_count",
    "daily_unique_clients",
    "daily_unique_tasks",
    "daily_max_single_entry",
    "emp_mean_daily_hours",
    "emp_std_daily_hours",
    "emp_z_score_daily",
    "dept_mean_daily_hours",
    "dept_z_score_daily",
    "emp_mean_entry_hours",
    "entry_z_score",
    "weekly_total_hours",
    "monthly_total_hours",
    "consecutive_overtime_days",
    "weekend_frequency_30d",
    "task_rarity_score",
    "hours_deviation_from_8",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "is_leave",
    "is_internal",
    "impossible_flag",
    "suspicious_round_hours",
]

REVIEWER_CATEGORY_FEATURES = ["task", "client", "product", "department"]
FLAGGED_LABELS = {"hours look large", "check", "exess hours", "kinda ok"}
APPROVED_LABELS = {"ok"}
LARGE_LOF_THRESHOLD = 25000
LARGE_DBSCAN_THRESHOLD = 20000


def _robust_minmax(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    normed = np.zeros_like(arr, dtype=float)
    finite = np.isfinite(arr)
    if not finite.any():
        return normed

    lo, hi = np.nanpercentile(arr[finite], [1, 99])
    if hi - lo < 1e-9:
        lo, hi = float(np.nanmin(arr[finite])), float(np.nanmax(arr[finite]))
    if hi - lo < 1e-9:
        return normed

    normed[finite] = (arr[finite] - lo) / (hi - lo)
    np.clip(normed, 0.0, 1.0, out=normed)
    return normed


def _standardize_frame(df: pd.DataFrame, cols: list[str]) -> np.ndarray:
    x = df[cols].copy()
    for col in cols:
        x[col] = pd.to_numeric(x[col], errors="coerce").fillna(0.0)
    return x.to_numpy(dtype=np.float32)


def _compute_statistical_scores(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    weekly_mean = data.groupby("employee")["weekly_total_hours"].transform("mean")
    weekly_std = data.groupby("employee")["weekly_total_hours"].transform("std").replace(0, np.nan)
    monthly_mean = data.groupby("employee")["monthly_total_hours"].transform("mean")
    monthly_std = data.groupby("employee")["monthly_total_hours"].transform("std").replace(0, np.nan)

    data["weekly_z_score"] = ((data["weekly_total_hours"] - weekly_mean) / weekly_std).fillna(0.0)
    data["monthly_z_score"] = ((data["monthly_total_hours"] - monthly_mean) / monthly_std).fillna(0.0)
    data["entry_z_score_abs"] = data["entry_z_score"].abs()
    data["daily_z_score_abs"] = data["emp_z_score_daily"].abs()
    data["weekly_z_score_abs"] = data["weekly_z_score"].abs()
    data["monthly_z_score_abs"] = data["monthly_z_score"].abs()

    data["zscore_max"] = data[
        ["entry_z_score_abs", "daily_z_score_abs", "weekly_z_score_abs", "monthly_z_score_abs"]
    ].max(axis=1)
    data["zscore_flag_moderate"] = data["zscore_max"] > 2
    data["zscore_flag_severe"] = data["zscore_max"] > 3
    return data


def _normalize_comment(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )


def _target_encode_fit(values: pd.Series, labels: np.ndarray, smoothing: float = 10.0) -> Tuple[Dict[str, float], float]:
    frame = pd.DataFrame({"value": values.astype(str), "label": labels})
    labeled = frame[frame["label"] != -1]
    if labeled.empty:
        return {}, 0.5

    global_mean = float(labeled["label"].mean())
    grouped = labeled.groupby("value")["label"].agg(["mean", "count"])
    encoded = (
        (grouped["mean"] * grouped["count"] + global_mean * smoothing)
        / (grouped["count"] + smoothing)
    ).to_dict()
    return {str(k): float(v) for k, v in encoded.items()}, global_mean


def _prepare_reviewer_features(
    df: pd.DataFrame,
    labels: Optional[np.ndarray] = None,
    encoders: Optional[Dict[str, Any]] = None,
    fit: bool = False,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    numeric = df[REVIEWER_NUMERIC_FEATURES].copy()
    for col in REVIEWER_NUMERIC_FEATURES:
        numeric[col] = pd.to_numeric(numeric[col], errors="coerce").fillna(0.0)
    num_matrix = numeric.to_numpy(dtype=float)

    if fit:
        if labels is None:
            raise ValueError("labels are required when fit=True")
        encoder_data: Dict[str, Any] = {"maps": {}, "defaults": {}}
        enc_cols = []
        for col in REVIEWER_CATEGORY_FEATURES:
            values = df[col].fillna("").astype(str).str.lower().str.strip()
            mapping, default = _target_encode_fit(values, labels)
            encoder_data["maps"][col] = mapping
            encoder_data["defaults"][col] = default
            enc_cols.append(values.map(mapping).fillna(default).to_numpy(dtype=float))
        enc_matrix = np.vstack(enc_cols).T if enc_cols else np.empty((len(df), 0))
        return np.hstack([num_matrix, enc_matrix]), encoder_data

    if encoders is None:
        encoders = {"maps": {}, "defaults": {}}
    enc_cols = []
    for col in REVIEWER_CATEGORY_FEATURES:
        values = df[col].fillna("").astype(str).str.lower().str.strip()
        mapping = encoders.get("maps", {}).get(col, {})
        default = float(encoders.get("defaults", {}).get(col, 0.5))
        enc_cols.append(values.map(mapping).fillna(default).to_numpy(dtype=float))
    enc_matrix = np.vstack(enc_cols).T if enc_cols else np.empty((len(df), 0))
    return np.hstack([num_matrix, enc_matrix]), encoders


def _train_reviewer_model(df: pd.DataFrame) -> Tuple[Optional[np.ndarray], Optional[Dict[str, Any]]]:
    comments = _normalize_comment(df["reviewers_comments"])
    labels = np.full(len(df), -1, dtype=int)
    labels[comments.isin(APPROVED_LABELS)] = 0
    labels[comments.isin(FLAGGED_LABELS)] = 1

    labeled_mask = labels != -1
    if labeled_mask.sum() < 30:
        return None, None
    if len(np.unique(labels[labeled_mask])) < 2:
        return None, None

    X, encoders = _prepare_reviewer_features(df, labels=labels, fit=True)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    flagged_count = int((labels == 1).sum())

    if flagged_count < 100:
        rng = np.random.default_rng(42)
        labeled_idx = np.where(labeled_mask)[0]
        unlabeled_idx = np.where(~labeled_mask)[0]
        max_training_rows = 15000
        keep_unlabeled = min(len(unlabeled_idx), max(0, max_training_rows - len(labeled_idx)))
        if keep_unlabeled > 0:
            sampled_unlabeled = rng.choice(unlabeled_idx, size=keep_unlabeled, replace=False)
            train_idx = np.concatenate([labeled_idx, sampled_unlabeled])
        else:
            train_idx = labeled_idx

        X_train = X_scaled[train_idx]
        y_train = labels[train_idx]
        model = LabelSpreading(kernel="rbf", alpha=0.2, max_iter=50)
        model.fit(X_train, y_train)
        proba = model.predict_proba(X_scaled)[:, 1]
        artifact = {
            "version": 1,
            "model_kind": "label_spreading",
            "model": model,
            "scaler": scaler,
            "encoders": encoders,
        }
        return proba.astype(float), artifact

    X_train = X_scaled[labeled_mask]
    y_train = labels[labeled_mask]
    model = LogisticRegression(
        random_state=42,
        class_weight="balanced",
        max_iter=500,
        solver="lbfgs",
    )
    model.fit(X_train, y_train)
    proba = model.predict_proba(X_scaled)[:, 1]
    artifact = {
        "version": 1,
        "model_kind": "logistic_regression",
        "model": model,
        "scaler": scaler,
        "encoders": encoders,
    }
    return proba.astype(float), artifact


def _save_reviewer_cache(path: Path, artifact: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        pickle.dump(artifact, handle)


def _load_reviewer_cache(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with path.open("rb") as handle:
        artifact = pickle.load(handle)
    if not isinstance(artifact, dict):
        return None
    if artifact.get("version") != 1:
        return None
    return artifact


def _predict_with_reviewer_cache(df: pd.DataFrame, artifact: Dict[str, Any]) -> Optional[np.ndarray]:
    try:
        X, _ = _prepare_reviewer_features(df, encoders=artifact.get("encoders"), fit=False)
        scaler = artifact["scaler"]
        model = artifact["model"]
        X_scaled = scaler.transform(X)
        if hasattr(model, "predict_proba"):
            return model.predict_proba(X_scaled)[:, 1].astype(float)
    except Exception:
        return None
    return None


def _run_isolation_forest(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    n = len(df)
    if n < 5:
        return np.zeros(n, dtype=float), np.ones(n, dtype=int)
    X = _standardize_frame(df, IF_FEATURES)
    contamination = min(0.05, max(0.01, 1.0 / max(n, 1)))
    model = IsolationForest(
        n_estimators=120 if n > 20000 else 200,
        max_samples=min(n, 2048),
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)
    decision = model.decision_function(X)
    pred = model.predict(X)
    anomaly_raw = -decision
    return anomaly_raw.astype(float), pred.astype(int)


def _run_lof(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    n = len(df)
    if n < 5:
        return np.zeros(n, dtype=float), np.ones(n, dtype=int)
    X = _standardize_frame(df, LOF_FEATURES)
    n_neighbors = max(2, min(20, n - 1))

    if n > LARGE_LOF_THRESHOLD:
        rng = np.random.default_rng(42)
        sample_size = LARGE_LOF_THRESHOLD
        sample_idx = rng.choice(n, size=sample_size, replace=False)
        X_train = X[sample_idx]
        model = LocalOutlierFactor(
            n_neighbors=max(2, min(20, sample_size - 1)),
            contamination=min(0.05, 0.49),
            novelty=True,
            n_jobs=-1,
        )
        model.fit(X_train)
        raw = -model.score_samples(X)
        cutoff = np.quantile(raw, 0.95)
        pred = np.where(raw >= cutoff, -1, 1)
        return raw.astype(float), pred.astype(int)

    model = LocalOutlierFactor(
        n_neighbors=n_neighbors,
        contamination=min(0.05, 0.49),
        novelty=False,
        n_jobs=-1,
    )
    pred = model.fit_predict(X)
    raw = -model.negative_outlier_factor_
    return raw.astype(float), pred.astype(int)


def _run_dbscan(df: pd.DataFrame) -> np.ndarray:
    n = len(df)
    if n < 5:
        return np.zeros(n, dtype=float)
    X = _standardize_frame(df, DBSCAN_FEATURES)
    X_scaled = StandardScaler().fit_transform(X)

    # Auto-tune eps via k-distance elbow when dataset is manageable
    eps_value = 0.5
    if n <= LARGE_DBSCAN_THRESHOLD:
        try:
            k = min(5, n - 1)
            nn = NearestNeighbors(n_neighbors=k, n_jobs=-1)
            nn.fit(X_scaled)
            distances = nn.kneighbors(X_scaled, return_distance=True)[0][:, -1]
            distances = np.sort(distances)
            # Use the 90th-percentile knee as eps
            eps_value = max(0.3, float(np.percentile(distances, 90)))
        except Exception:
            eps_value = 0.5

    if n <= LARGE_DBSCAN_THRESHOLD:
        model = DBSCAN(eps=eps_value, min_samples=5, n_jobs=-1)
        labels = model.fit_predict(X_scaled)
        return (labels == -1).astype(float)

    rng = np.random.default_rng(42)
    sample_size = LARGE_DBSCAN_THRESHOLD
    sample_idx = rng.choice(n, size=sample_size, replace=False)
    X_sample = X_scaled[sample_idx]
    model = DBSCAN(eps=0.5, min_samples=5, n_jobs=-1)
    sample_labels = model.fit_predict(X_sample)

    noise = np.zeros(n, dtype=float)
    noise[sample_idx] = (sample_labels == -1).astype(float)

    core_mask = sample_labels != -1
    if core_mask.sum() < 5:
        center = X_sample.mean(axis=0, keepdims=True)
        dist = np.linalg.norm(X_scaled - center, axis=1)
        cutoff = np.quantile(dist, 0.95)
        return (dist >= cutoff).astype(float)

    core_points = X_sample[core_mask]
    nn = NearestNeighbors(n_neighbors=1, n_jobs=-1)
    nn.fit(core_points)
    dist_to_core = nn.kneighbors(X_scaled, return_distance=True)[0][:, 0]
    sample_dist = dist_to_core[sample_idx]
    cutoff = max(0.75, float(np.quantile(sample_dist, 0.95)))
    projected_noise = (dist_to_core > cutoff).astype(float)
    return np.maximum(noise, projected_noise)


def run_model_ensemble(df: pd.DataFrame, reviewer_model_path: str | Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    data = _compute_statistical_scores(df)

    if_raw, if_pred = _run_isolation_forest(data)
    lof_raw, lof_pred = _run_lof(data)
    dbscan_noise = _run_dbscan(data)

    data["if_raw_score"] = if_raw
    data["if_pred"] = if_pred
    data["lof_raw_score"] = lof_raw
    data["lof_pred"] = lof_pred
    data["dbscan_noise"] = dbscan_noise

    cache_path = Path(reviewer_model_path)
    cached_artifact = _load_reviewer_cache(cache_path)

    reviewer_proba: Optional[np.ndarray] = None
    reviewer_meta: Dict[str, Any] = {"used_cached": False, "model_kind": None}

    if cached_artifact is not None:
        reviewer_proba = _predict_with_reviewer_cache(data, cached_artifact)
        if reviewer_proba is not None:
            reviewer_meta["used_cached"] = True
            reviewer_meta["model_kind"] = cached_artifact.get("model_kind")

    if reviewer_proba is None:
        trained_proba, trained_artifact = _train_reviewer_model(data)
        if trained_proba is not None:
            reviewer_proba = trained_proba
            reviewer_meta["used_cached"] = False
            reviewer_meta["model_kind"] = trained_artifact.get("model_kind") if trained_artifact else None
            if trained_artifact is not None and cached_artifact is None:
                _save_reviewer_cache(cache_path, trained_artifact)

    data["if_norm"] = _robust_minmax(data["if_raw_score"].to_numpy())
    data["lof_norm"] = _robust_minmax(data["lof_raw_score"].to_numpy())
    data["dbscan_norm"] = data["dbscan_noise"].astype(float).clip(0.0, 1.0)
    data["zscore_norm"] = np.clip(data["zscore_max"].to_numpy(dtype=float) / 4.0, 0.0, 1.0)

    if reviewer_proba is None:
        data["reviewer_proba"] = np.nan
        data["reviewer_norm"] = 0.0
        weights = {
            "if_norm": 0.275,
            "lof_norm": 0.225,
            "dbscan_norm": 0.175,
            "zscore_norm": 0.325,
            "reviewer_norm": 0.0,
        }
        reviewer_meta["enabled"] = False
    else:
        reviewer = np.clip(reviewer_proba, 0.0, 1.0)
        data["reviewer_proba"] = reviewer
        data["reviewer_norm"] = reviewer
        weights = {
            "if_norm": 0.20,
            "lof_norm": 0.15,
            "dbscan_norm": 0.10,
            "zscore_norm": 0.25,
            "reviewer_norm": 0.30,
        }
        reviewer_meta["enabled"] = True

    data["composite_score"] = 100.0 * (
        weights["if_norm"] * data["if_norm"]
        + weights["lof_norm"] * data["lof_norm"]
        + weights["dbscan_norm"] * data["dbscan_norm"]
        + weights["zscore_norm"] * data["zscore_norm"]
        + weights["reviewer_norm"] * data["reviewer_norm"]
    )

    # ── Model agreement: how many independent signals flag this entry ──
    agreement = np.zeros(len(data), dtype=int)
    agreement += ((data["if_norm"] >= 0.75) | (data["if_pred"] == -1)).astype(int)
    agreement += ((data["lof_norm"] >= 0.75) | (data["lof_pred"] == -1)).astype(int)
    agreement += (data["dbscan_noise"] >= 1.0).astype(int)
    agreement += (data["zscore_max"] >= 2.0).astype(int)
    if reviewer_proba is not None:
        agreement += (data["reviewer_proba"] >= 0.60).astype(int)
    data["model_agreement"] = agreement

    metadata = {"weights": weights, "reviewer": reviewer_meta}
    return data, metadata
