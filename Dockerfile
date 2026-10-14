FROM python:3.12-slim

# ffmpeg for frame extraction; ca-certificates for https.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# DATA_DIR is the Fly volume mount; /data fallback for local runs.
# On Hugging Face Spaces the app listens on 7860.
ENV DATA_DIR=/data PORT=7860
EXPOSE 7860

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT} --workers 1"]
