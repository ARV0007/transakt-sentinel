FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
RUN useradd --create-home sentinel && mkdir -p /data && chown sentinel /data
USER sentinel
# cases.db and checkpoints.db live on a volume, so paused cases survive restarts
ENV DATA_DIR=/data

EXPOSE 8003
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]
