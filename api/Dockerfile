# ── Base image ────────────────────────────────────────────────────────────────
# python:3.11-slim: Debian-based, no dev tools, ~50MB vs ~1GB for the full image.
# 3.11 gives us the `str | None` union syntax used in main.py (requires 3.10+).
FROM python:3.11-slim

# ── Working directory ─────────────────────────────────────────────────────────
# All subsequent COPY / RUN commands are relative to /app inside the container.
WORKDIR /app

# ── Install dependencies ──────────────────────────────────────────────────────
# Copy requirements BEFORE source code. Docker builds layers in order and caches
# each one. If only your .py files change (common during development), Docker
# reuses the cached pip install layer and skips reinstalling packages.
# If you copied everything first, every code change would re-run pip install.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# --no-cache-dir: don't store the pip download cache in the image layer,
#   keeps the image ~30MB smaller.

# ── Copy source ───────────────────────────────────────────────────────────────
# This Dockerfile lives inside api/, so the build context is api/.
# COPY . . copies retriever.py, llm.py, main.py into /app/.
COPY . .

# ── Port ──────────────────────────────────────────────────────────────────────
# Cloud Run injects a PORT environment variable (default 8080).
# EXPOSE is documentation only — it does not publish the port.
# The CMD below reads $PORT at runtime.
ENV PORT=8080
EXPOSE 8080

# ── Non-root user ─────────────────────────────────────────────────────────────
# Running as root inside a container is a security risk — if the app is
# compromised, the attacker has root inside the container.
# Cloud Run supports non-root containers; this follows the principle of
# least privilege.
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# ── Start command ─────────────────────────────────────────────────────────────
# Cloud Run sends HTTP traffic to $PORT. We bind uvicorn to 0.0.0.0 so it
# accepts connections from outside the container (not just localhost).
#
# main:app  →  the `app` FastAPI object in main.py
# --workers 1  →  Cloud Run scales by spinning up more container instances
#   (horizontal scaling), not by adding workers per instance. One worker per
#   container is the standard pattern; multiple workers would share the process
#   but complicate the singleton _conn pattern.
CMD uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1
