#!/bin/bash
# Run this on your DigitalOcean Droplet to set up both bots
# Usage: curl -sSL <this-url> | bash
#   or: ssh root@YOUR_DROPLET_IP < setup-droplet.sh

set -e

echo "=== Setting up Kalshi Trading Bots ==="

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi

# Install Docker Compose plugin if not present
if ! docker compose version &> /dev/null; then
    echo "Installing Docker Compose..."
    apt-get update && apt-get install -y docker-compose-plugin
fi

# Create project directory
mkdir -p /opt/kalshi
cd /opt/kalshi

# Clone both repos
if [ ! -d "kalshi-trading-bot" ]; then
    git clone https://github.com/usawrapco-spec/kalshi-trading-bot.git
else
    cd kalshi-trading-bot && git pull && cd ..
fi

if [ ! -d "kalshi-weather-bot" ]; then
    git clone https://github.com/usawrapco-spec/kalshi-weather-bot.git
else
    cd kalshi-weather-bot && git pull && cd ..
fi

# Create .env if it doesn't exist
if [ ! -f "kalshi-weather-bot/.env" ]; then
    cat > kalshi-weather-bot/.env << 'ENVEOF'
# === FILL THESE IN ===
KALSHI_API_KEY_ID=your-key-id-here
KALSHI_PRIVATE_KEY=your-private-key-here
ENABLE_TRADING=false
DB_PASSWORD=kalshi_secure_pw_change_me
ENVEOF
    echo ""
    echo "*** IMPORTANT: Edit /opt/kalshi/kalshi-weather-bot/.env with your API keys ***"
    echo "    nano /opt/kalshi/kalshi-weather-bot/.env"
    echo ""
fi

# Create the weather DB
cd kalshi-weather-bot

echo "Starting services..."
docker compose up -d db
sleep 5

# Create weather database
docker compose exec -T db psql -U kalshi -c "CREATE DATABASE kalshi_weather;" 2>/dev/null || true

# Start both bots
docker compose up -d --build

echo ""
echo "=== DONE ==="
echo "Crypto bot dashboard:  http://$(curl -s ifconfig.me):8080/dashboard"
echo "Weather bot dashboard: http://$(curl -s ifconfig.me):8081/dashboard"
echo ""
echo "Useful commands:"
echo "  cd /opt/kalshi/kalshi-weather-bot"
echo "  docker compose logs -f crypto-bot    # crypto bot logs"
echo "  docker compose logs -f weather-bot   # weather bot logs"
echo "  docker compose ps                    # status"
echo "  docker compose restart               # restart all"
echo "  docker compose down                  # stop all"
echo "  docker compose up -d --build         # rebuild & restart"
