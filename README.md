SM Insight Board — Render deployment

Quick start (local):

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run locally (dev server):

```bash
python dashboard.py
# or with gunicorn (recommended for parity with Render):
PORT=8053 gunicorn dashboard:server --bind 0.0.0.0:$PORT
```

Windows PowerShell example:

```powershell
$env:PORT=8053; gunicorn dashboard:server --bind 0.0.0.0:$env:PORT
```

Deploying to Render.com

1. Push this repo to GitHub (or GitLab).
2. In Render, create a new **Web Service** and connect the repo/branch.
3. Render will detect `render.yaml` (or configure via UI):
   - Environment: `Python`
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn dashboard:server --bind 0.0.0.0:$PORT`
   - Health check path: `/health`
4. Set environment variables on Render as needed (e.g., `DEBUG=false`).

Data considerations

- The app expects these files by default:
  - `data from db.xlsx`
  - `excel_data_model_fixed.xlsx`
- For production, prefer storing data in cloud storage (S3, Azure Blob) and loading on startup rather than committing large binary files.

Notes

- `requirements.txt` already contains `gunicorn` and primary dependencies.
- The `/health` endpoint returns 200 OK for readiness checks.
- The app uses the `PORT` env var for listening (Render provides it).

If you want, I can:
- Add S3 load logic and an example `ENV` usage.
- Add a tiny CI step to build and upload an initial transformed dataset.
