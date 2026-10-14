---
title: FrameGrab
emoji: film_frames
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

# FrameGrab

YouTube → frame-by-frame stills for AI agents.

Joel's Arena AI agent cannot download `.mp4` files directly (128MB agent
storage limit), so this service does the heavy lifting: give it a YouTube
URL, it downloads the video (≤480p), extracts frames with ffmpeg, grabs
captions, and serves everything as plain URLs the agent fetches one by one.

## How it works

```
POST /api/extract {url, fps, maxFrames, width, startSec, endSec}
  -> {jobId, statusUrl}
GET /api/jobs/{id}            -> poll until status == "done"
GET /v/{id}/manifest.json     -> title, duration, frameCount, URLs
GET /v/{id}/frame/{n}.jpg     -> frame n (1-based), small JPEG
GET /v/{id}/contact.jpg       -> contact sheet of all frames
GET /v/{id}/subtitles.txt     -> captions as plain text (when available)
```

Defaults: 1 fps, up to 600 frames, 640px wide. One extraction at a time.
Jobs auto-delete after 12 hours. Set `SERVICE_KEY` and `PUBLIC_BASE` as
Space secrets; when `SERVICE_KEY` is set, `/api/*` needs `X-Service-Key`.

## Local run

```bash
pip install -r requirements.txt   # plus: apt install ffmpeg
DATA_DIR=/tmp/fgdata PUBLIC_BASE=http://127.0.0.1:7860 \
  uvicorn app:app --host 127.0.0.1 --port 7860
```

## RobloxFlow wiring

RobloxFlow proxies through `/api/video/extract` and `/api/video/jobs/[id]`
(owner or Arena agent token), so the agent never needs the Space URL or key.
Set on Vercel: `FRAMEGRAB_URL`, `FRAMEGRAB_KEY`.
