# ============================================================
# Stage 1: Build the React frontend
# ============================================================
FROM node:20-alpine AS build-frontend

WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ============================================================
# Stage 2: Runtime — Python + Nginx + supervisord
# ============================================================
FROM python:3.11-slim

# Install system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends nginx supervisor curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Backend code
COPY backend/ ./backend/

# Frontend build output
COPY --from=build-frontend /build/dist ./frontend-dist/

# Data directory (will be mounted as volume in production)
RUN mkdir -p /app/data/uploads /app/data/exports

# Nginx config
COPY nginx.conf /etc/nginx/sites-available/default
RUN rm -f /etc/nginx/sites-enabled/default && \
    ln -s /etc/nginx/sites-available/default /etc/nginx/sites-enabled/default

# Supervisord config
COPY supervisord.conf /etc/supervisor/conf.d/timesheetiq.conf

EXPOSE 80

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -sf http://localhost/api/health || exit 1

CMD ["supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]
