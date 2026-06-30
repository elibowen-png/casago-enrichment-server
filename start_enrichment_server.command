#!/bin/bash
cd "$(dirname "$0")"
echo ""
echo "  Clearing port 5001..."
lsof -ti :5001 | xargs kill -9 2>/dev/null || true
sleep 1

echo "  Installing dependencies..."
pip3 install flask requests beautifulsoup4 gunicorn --quiet --break-system-packages 2>/dev/null || \
pip3 install flask requests beautifulsoup4 gunicorn --quiet
echo ""
echo "  Starting server on http://localhost:5001"
echo ""

while true; do
    gunicorn pm_enrichment_server:app \
        --bind 0.0.0.0:5001 \
        --workers 2 \
        --timeout 60 \
        --max-requests 20 \
        --max-requests-jitter 5 \
        --log-level warning
    echo ""
    echo "  Server stopped. Clearing port and restarting in 3 seconds..."
    lsof -ti :5001 | xargs kill -9 2>/dev/null || true
    sleep 3
done
