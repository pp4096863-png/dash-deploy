FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Expose typical http port; Fly.io sets PORT in env
EXPOSE 8080

CMD ["gunicorn", "dashboard:server", "--bind", "0.0.0.0:$PORT", "--workers", "2", "--timeout", "120"]
