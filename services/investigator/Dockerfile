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
USER sentinel
# cases.db and checkpoints.db live on a volume, so paused cases survive restarts
ENV DATA_DIR=/data

EXPOSE 8003
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]
