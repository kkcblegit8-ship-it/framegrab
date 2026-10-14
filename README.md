# FrameGrab

YouTube / video files → frame-by-frame stills for AI agents.

Joel's Arena AI agent cannot download `.mp4` files directly (128MB agent
storage limit), so this service does the heavy lifting: give it a video
(file upload or YouTube URL), it extracts frames with ffmpeg (up to 5,000),
grabs captions, and serves everything as plain URLs the agent fetches one
by one.

## How it works

```
POST /api/upload-token            -> one-time upload token (needs X-Service-Key)
POST /api/upload                  -> multipart: token, file, fps, maxFrames, width
POST /api/extract {url, ...}      -> YouTube URL (often blocked from datacenters)
GET  /api/jobs                    -> recent jobs, newest first
GET  /api/jobs/{id}               -> poll until status == "done"
GET  /v/{id}/manifest.json        -> title, duration, frameCount, URLs
GET  /v/{id}/frame/{n}.jpg        -> frame n (1-based), small JPEG
GET  /v/{id}/contact.jpg          -> contact sheet (first 144 frames sampled)
GET  /v/{id}/subtitles.txt        -> captions as plain text (when available)
```

Defaults: 1 fps, up to 600 frames (max 5,000), 640px wide. One extraction
at a time. Jobs auto-delete after 12 hours. Set `SERVICE_KEY` and
`PUBLIC_BASE` as env vars; when `SERVICE_KEY` is set, `/api/*` needs
`X-Service-Key`.

## Why uploads, not YouTube

YouTube 403-blocks datacenter IPs (tried: default, android/ios player
clients, TLS impersonation). Joel downloads the video on his own network
and uploads the file; the service never talks to YouTube for uploads.

## Deploy (Render, free tier)

`render.yaml` in the repo root. Create a free Render account (no card),
New → Blueprint, pick the repo, set `SERVICE_KEY` and `PUBLIC_BASE` when
prompted. Free tier sleeps after 15 min idle; callers handle the wake-up.

## Local run

```bash
pip install -r requirements.txt   # plus: apt install ffmpeg
DATA_DIR=/tmp/fgdata PUBLIC_BASE=http://127.0.0.1:8080 \
  uvicorn app:app --host 127.0.0.1 --port 8080
```

## RobloxFlow wiring

RobloxFlow proxies through `/api/video/upload-token`, `/api/video/jobs`,
and `/api/video/jobs/[id]` (owner or Arena agent token), so the agent never
needs the service URL or key. Set on Vercel: `FRAMEGRAB_URL`, `FRAMEGRAB_KEY`.
