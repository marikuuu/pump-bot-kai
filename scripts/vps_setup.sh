#!/bin/bash

# IZANAGI VPS Setup Script (Ubuntu 22.04+)
echo "🚀 Initializing IZANAGI VPS Environment Setup..."

# 1. Update & Install Basic Tools
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip python3-dev build-essential git curl libpq-dev

# 2. Install Node.js & PM2 (Process Manager for 24/7 run)
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm install -g pm2

# 3. Setup Project Directory
# Assumes you've already cloned/SFTP the repo to ~/izanagi
# cd ~/izanagi

# 4. Install Python Dependencies
pip3 install --upgrade pip
pip3 install -r requirements.txt

# 5. Enable PM2 to startup on boot
pm2 startup
# (You'll need to follow the printed instruction from the command above)

echo "✅ VPS Setup Complete!"
echo "👉 Next Step: Copy your .env to ~/izanagi/.env"
echo "👉 Start Backtester: pm2 start scripts/global_1month_validator.py --name izanagi-backtest"
echo "👉 Start Bot: pm2 start main.py --name izanagi-bot"
