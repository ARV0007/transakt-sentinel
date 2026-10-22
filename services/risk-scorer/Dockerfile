FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
# Train at build time: the image always ships a model matching its own feature
# code, and no binary model file lives in git. Takes a few seconds.
RUN python -m app.train --out model/model.joblib

RUN useradd --create-home sentinel && mkdir -p /data && chown sentinel /data
USER sentinel
ENV SCORES_DB=/data/scores.db MODEL_PATH=/app/model/model.joblib

EXPOSE 8002
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]
