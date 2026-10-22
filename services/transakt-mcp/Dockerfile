FROM python:3.12-slim

# Dependencies first, source second: a code edit then reuses the cached
# dependency layer instead of reinstalling everything (same idea as Transakt's
# `mvn dependency:go-offline` layer).
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
RUN useradd --create-home sentinel
USER sentinel

EXPOSE 8001
CMD ["python", "-m", "app.server"]
