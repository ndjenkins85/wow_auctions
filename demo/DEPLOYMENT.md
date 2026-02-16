# Deployment Guide

## Railway Deployment

This demo is designed to deploy on Railway at `ndjenkins.com/projects/wow-auctions/`

### Steps:

1. **Connect Repository**: Link the GitHub repo to Railway
2. **Select Branch**: Use the `dev` branch
3. **Set Root Directory**: `/demo`
4. **Configure Port**: Set `PORT=3005` environment variable
5. **Deploy**: Railway will auto-detect the Dockerfile

### Environment Variables:
```
PORT=3005
```

### Build Command:
```bash
docker build -t wow-auctions-demo .
```

### Start Command:
```bash
docker run -p $PORT:$PORT -e PORT=$PORT wow-auctions-demo
```

## Local Testing

### Option 1: Simple HTTP Server
```bash
cd demo
python3 -m http.server 8000
# Visit http://localhost:8000
```

### Option 2: Docker
```bash
cd demo
docker build -t wow-auctions-demo .
docker run -p 3005:3005 -e PORT=3005 wow-auctions-demo
# Visit http://localhost:3005
```

## File Structure

```
demo/
├── index.html          # Main application
├── css/
│   └── style.css       # WoW-inspired dark theme
├── js/
│   └── app.js          # Charts and interactivity
├── data/
│   ├── price_history.json
│   ├── holdings.json
│   ├── inventory.json
│   ├── sell_policy.json
│   └── market_snapshot.json
├── Dockerfile          # Nginx deployment
├── README.md
└── DEPLOYMENT.md
```

## Notes

- All data is baked into JSON files
- No backend required
- No API keys needed
- Fully static and self-contained
- Mobile responsive
- Works offline after initial load
