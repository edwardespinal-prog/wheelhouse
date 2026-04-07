# WheelHouse

### Real-time portfolio intelligence with built-in tax optimization.

**Built by [Ed Espinal](https://github.com/edwardespinal-prog)**

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/edwardespinal-prog/wheelhouse)

---

## Deploy Your Own

1. Fork this repo
2. Sign up at [render.com](https://render.com)
3. Click **Deploy to Render** above
4. Set `FINNHUB_API_KEY` (free at [finnhub.io](https://finnhub.io))
5. Done — live in ~2 minutes (~$7.30/mo)

## Run Locally

```bash
git clone https://github.com/edwardespinal-prog/wheelhouse
cd wheelhouse
pip install -r backend/requirements.txt
cp backend/.env.example backend/.env   # add your Finnhub key
python backend/server.py               # http://localhost:8000
```

Built with Python, React 18, yfinance
