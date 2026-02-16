# WoW Auctions - Portfolio Demo

A static, read-only showcase of the WoW Auctions trading automation system.

## What This Demo Shows

This is a portfolio-ready demonstration of the WoW Auctions project, featuring:

- **Holdings Tracking**: Visualize total wealth (gold + inventory) over time
- **Price Analysis**: Historical price trends for items and materials
- **Inventory Management**: Current stock across characters and locations
- **Market Intelligence**: Buy/sell recommendations with profit analysis
- **Live Market Data**: Auction house snapshot with competitor pricing

## Technology

- **Frontend**: Vanilla JavaScript with Chart.js
- **Design**: Dark gaming aesthetic with WoW-inspired colors (gold, purple)
- **Data**: Baked JSON sample data (no backend required)
- **Deployment**: Nginx static server via Docker

## Running Locally

```bash
# Serve with any static server
python -m http.server 8000
# Visit http://localhost:8000

# Or use Docker
docker build -t wow-auctions-demo .
docker run -p 3005:3005 -e PORT=3005 wow-auctions-demo
# Visit http://localhost:3005
```

## Deployment

Built for deployment on Railway at `ndjenkins.com/projects/wow-auctions/`

- Dockerfile configured to use PORT environment variable
- All assets are self-contained
- No external dependencies or API calls
- Mobile responsive

## Data Structure

Sample data files in `data/`:
- `price_history.json` - Historical item prices over time
- `holdings.json` - Total wealth progression
- `inventory.json` - Current inventory breakdown
- `sell_policy.json` - Recommended sell prices and strategies
- `market_snapshot.json` - Current auction house listings

## About the Parent Project

This demo showcases the WoW Auctions trading bot, which:
- Analyzes World of Warcraft auction house data
- Calculates optimal buy/sell prices for crafted goods
- Tracks inventory and wealth across multiple characters
- Generates automated trading policies for WoW addons

See the [main README](../README.md) and [project article](https://www.nickjenkins.com.au/articles/personal/2020/07/07/programming-and-analytics-in-games) for technical details.
