FROM python:3.12-slim

# Dependencies first, source second: a code edit then reuses the cached
# dependency layer instead of reinstalling everything (same idea as Transakt's
# `mvn dependency:go-offline` layer).
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The service runs as an unprivileged user, so it must OWN its code: COPY keeps
# the files' permissions from the build machine, and a file that is
# owner-read-only there (as some editors and sync tools leave them) would be
# unreadable to "sentinel" if root owned it. --chown makes that impossible.
RUN useradd --create-home sentinel
COPY --chown=sentinel:sentinel app ./app
USER sentinel

EXPOSE 8001
CMD ["python", "-m", "app.server"]
