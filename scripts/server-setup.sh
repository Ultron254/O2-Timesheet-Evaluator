#!/usr/bin/env bash
# ============================================================
# TimesheetIQ — Ubuntu 22.04 Server Setup Script
# Run this ONCE on a fresh Ubuntu 22.04 VM as root or with sudo
#
# Usage:
#   chmod +x scripts/server-setup.sh
#   sudo ./scripts/server-setup.sh YOUR_DOMAIN
#
# Example:
#   sudo ./scripts/server-setup.sh timesheetiq.oxygene.co.ke
# ============================================================
set -euo pipefail

DOMAIN="${1:-}"
REPO_URL="https://github.com/Ultron254/O2-Timesheet-Evaluator.git"
APP_DIR="/opt/timesheetiq"

if [ -z "$DOMAIN" ]; then
    echo "⚠️  No domain provided. Will serve on IP only (no SSL)."
    echo "   To add SSL later: sudo certbot --nginx -d YOUR_DOMAIN"
fi

echo "============================================"
echo "  TimesheetIQ Server Setup"
echo "  Domain: ${DOMAIN:-[none — IP only]}"
echo "============================================"

# ── 1. System updates ──
echo ""
echo "📦 Updating system packages..."
apt-get update -qq
apt-get upgrade -y -qq

# ── 2. Install Docker ──
echo ""
echo "🐳 Installing Docker..."
if ! command -v docker &>/dev/null; then
    apt-get install -y -qq ca-certificates curl gnupg
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
        https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
        tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    systemctl enable docker
    systemctl start docker
    echo "✅ Docker installed"
else
    echo "✅ Docker already installed: $(docker --version)"
fi

# ── 3. Install Certbot for SSL (if domain provided) ──
if [ -n "$DOMAIN" ]; then
    echo ""
    echo "🔒 Installing Certbot for SSL..."
    apt-get install -y -qq certbot
fi

# ── 4. Configure firewall ──
echo ""
echo "🔥 Configuring firewall..."
if command -v ufw &>/dev/null; then
    ufw allow 22/tcp   # SSH
    ufw allow 80/tcp   # HTTP
    ufw allow 443/tcp  # HTTPS
    ufw --force enable
    echo "✅ Firewall configured (ports 22, 80, 443)"
else
    echo "⚠️  ufw not found — configure firewall manually"
fi

# ── 5. Clone the repo ──
echo ""
echo "📥 Cloning repository..."
if [ -d "$APP_DIR" ]; then
    echo "   Directory exists — pulling latest..."
    cd "$APP_DIR"
    git pull origin master
else
    git clone "$REPO_URL" "$APP_DIR"
    cd "$APP_DIR"
fi

# ── 6. Create data directories ──
mkdir -p "$APP_DIR/data/uploads" "$APP_DIR/data/exports"
chmod -R 755 "$APP_DIR/data"

# ── 7. Build and start the app ──
echo ""
echo "🚀 Building and starting TimesheetIQ..."
cd "$APP_DIR"
docker compose up -d --build --remove-orphans

# ── 8. Clean up old Docker images ──
docker image prune -f

# ── 9. Wait for health check ──
echo ""
echo "⏳ Waiting for app to start..."
sleep 8
if curl -sf http://localhost/api/health > /dev/null 2>&1; then
    echo "✅ Health check passed!"
else
    echo "⚠️  Health check failed — checking logs..."
    docker compose logs --tail=20
fi

# ── 10. Set up SSL with Let's Encrypt (if domain provided) ──
if [ -n "$DOMAIN" ]; then
    echo ""
    echo "🔒 Setting up SSL for $DOMAIN..."
    echo "   Note: Make sure your domain's DNS A record points to this server's IP."
    echo ""
    echo "   To obtain SSL certificate, run:"
    echo "   sudo certbot certonly --standalone --pre-hook 'docker compose -f $APP_DIR/docker-compose.yml down' --post-hook 'docker compose -f $APP_DIR/docker-compose.yml up -d' -d $DOMAIN"
    echo ""
    echo "   Or if you prefer to handle SSL via a host-level Nginx reverse proxy:"
    echo "   sudo apt install nginx"
    echo "   sudo certbot --nginx -d $DOMAIN"
fi

# ── 11. Create auto-deploy script ──
cat > "$APP_DIR/scripts/deploy.sh" << 'DEPLOY_EOF'
#!/usr/bin/env bash
set -euo pipefail
cd /opt/timesheetiq
echo "📥 Pulling latest code..."
git pull origin master
echo "🐳 Rebuilding containers..."
docker compose up -d --build --remove-orphans
echo "🧹 Cleaning up..."
docker image prune -f
sleep 5
if curl -sf http://localhost/api/health > /dev/null 2>&1; then
    echo "✅ Deploy complete — health check passed"
else
    echo "⚠️  Deploy complete — health check failed"
    docker compose logs --tail=20
fi
DEPLOY_EOF
chmod +x "$APP_DIR/scripts/deploy.sh"

# ── 12. Add cron job for auto-updates (every 5 minutes) ──
CRON_CMD="*/5 * * * * cd $APP_DIR && git fetch origin master --quiet && [ \$(git rev-parse HEAD) != \$(git rev-parse origin/master) ] && $APP_DIR/scripts/deploy.sh >> /var/log/timesheetiq-deploy.log 2>&1"
(crontab -l 2>/dev/null | grep -v "timesheetiq" ; echo "$CRON_CMD") | crontab -
echo "✅ Auto-deploy cron job installed (checks every 5 minutes)"

echo ""
echo "============================================"
echo "  ✅ TimesheetIQ Setup Complete!"
echo "============================================"
echo ""
echo "  App URL:     http://${DOMAIN:-$(curl -s ifconfig.me)}"
echo "  Health:      http://${DOMAIN:-$(curl -s ifconfig.me)}/api/health"
echo "  Data dir:    $APP_DIR/data/"
echo "  Logs:        docker compose -C $APP_DIR logs -f"
echo "  Redeploy:    $APP_DIR/scripts/deploy.sh"
echo "  Auto-deploy: Every 5 min via cron (git pull + rebuild)"
echo ""
if [ -n "$DOMAIN" ]; then
    echo "  ⚠️  SSL: Run the certbot command above to enable HTTPS"
fi
echo ""
