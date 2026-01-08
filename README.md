# SM Insight Board (Sales Dash)

A Plotly Dash dashboard for sales analytics. This repository contains the Dash app (`dashboard.py`), Excel data used for development, and static assets.

---

## Quick start (development)

Prerequisites:
- Python 3.10+ (3.12 is used in development)
- Git

Run locally:

1. Create and activate a virtual environment (recommended):
   - Windows: `python -m venv venv && venv\Scripts\activate`
   - macOS/Linux: `python -m venv venv && source venv/bin/activate`
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the dev server (development mode):
   ```bash
   python dashboard.py
   ```

This will run the Dash built-in dev server and (if the `data_from_db.xlsx` file exists) run the transformation and background monitoring when run directly.

---

## Production / Deploy (Railway)

This project is prepared for a simple GitHub→Railway deploy. Key points:

- A `Procfile` has been added to run the app with Gunicorn:

  `web: gunicorn dashboard:server --bind 0.0.0.0:$PORT --workers 2 --timeout 120`

- `gunicorn` has been added to `requirements.txt`.

Railway deploy steps (summary):
1. Push this repo to GitHub.
2. Log into Railway and create a new project → "Deploy from GitHub" and connect your repo.
3. Railway will detect the `Procfile` and use it to start the web service (no manual start command needed).
4. (Optional) Set environment variables in Railway if needed.

Notes:
- The app exposes the Flask server as `server = app.server` (this is the WSGI entrypoint used by Gunicorn).
- `dashboard.py` has been made import-safe: heavy startup steps (transform + background monitor) are executed only when `python dashboard.py` is run directly (not on module import), so Gunicorn will not spawn monitor threads on worker import.

---

## Production considerations & tips

- Do **not** keep large or proprietary Excel files in the repo for production; instead store data in S3 or a database and fetch during runtime.
- Consider moving `data_from_db.xlsx` out of the repository and use environment variables for paths or connection strings.
- Add environment variable support (e.g. `python-dotenv`) if you need to configure behavior (monitoring toggle, debug flags).
- If you prefer Windows hosting, `waitress` is an alternative WSGI server. For Linux / containers, `gunicorn` is recommended.

---

## Files added/changed by the deployment prep
- `Procfile` — start command for Gunicorn (used by Railway)
- `.gitignore` — ignores Excel files, `.env`, pycache and runtime files
- `requirements.txt` — `gunicorn` added
- `dashboard.py` — import-safe init and monitoring moved to `if __name__ == "__main__"`

---

## Next steps (optional)
- Add a `runtime.txt` to pin the Python version (e.g., `python-3.12`).
- Add a `README` or short deploy checklist in GitHub with Railway screenshots/notes.
- Create a `Dockerfile` if you prefer container-based deploys (Fly.io or Railway Docker deploy).

If you want, I can also add a Dockerfile + sample `runtime.txt` and a short deploy checklist for Railway. Want me to add those now?