FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The service runs as an unprivileged user, so it must OWN its code: COPY keeps
# the files' permissions from the build machine, and a file that is
# owner-read-only there (as some editors and sync tools leave them) would be
# unreadable to "sentinel" if root owned it. --chown makes that impossible.
RUN useradd --create-home sentinel && mkdir -p /data && chown sentinel /data
COPY --chown=sentinel:sentinel app ./app
COPY --chown=sentinel:sentinel feedback ./feedback
# Train at build time: the image always ships a model matching its own feature
# code, and no binary model file lives in git. Takes a few seconds. Analyst
# decisions in feedback/feedback.json (from ./sentinel.sh retrain) are folded in;
# with no file, it trains on synthetic data alone.
RUN python -m app.train --out model/model.skops --feedback feedback/feedback.json \
 && chown -R sentinel:sentinel model

USER sentinel
ENV SCORES_DB=/data/scores.db MODEL_PATH=/app/model/model.skops

EXPOSE 8002
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]
