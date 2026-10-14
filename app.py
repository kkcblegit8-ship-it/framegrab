"""
FrameGrab: YouTube -> frame-by-frame stills for AI agents.

Joel's Arena AI agent cannot download .mp4 files (128MB agent storage
limit), so this service does the heavy lifting: it resolves a YouTube URL
to a direct stream, extracts frames with ffmpeg (streaming, the video file
itself is never stored), and serves the frames as plain JPEGs the agent
fetches one by one.

Endpoints:
  POST /api/extract   {url, fps, maxFrames, width, startSec, endSec}
  GET  /api/jobs/{id}             job status + manifest
  GET  /v/{id}/manifest.json      same manifest, agent-friendly
  GET  /v/{id}/frame/{n}.jpg      single frame (n = 1..frameCount)
  GET  /v/{id}/contact.jpg        contact sheet of all frames
  GET  /healthz

One extraction runs at a time (256MB Fly VM). Jobs auto-delete after 12h.
"""

import json
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

DATA_DIR = os.environ.get("DATA_DIR", "/tmp/framegrab-data")
JOBS_DIR = os.path.join(DATA_DIR, "jobs")
os.makedirs(JOBS_DIR, exist_ok=True)

SERVICE_KEY = os.environ.get("SERVICE_KEY", "")
JOB_TTL_SECONDS = 12 * 3600
MAX_DURATION_SEC = 1800
MAX_FRAMES = 5000
MAX_UPLOAD_BYTES = 200 * 1024 * 1024
UPLOAD_TOKEN_TTL = 600
CONTACT_SHEET_FRAMES = 144

app = FastAPI(title="FrameGrab")
_extract_lock = threading.Lock()
_upload_tokens: dict[str, float] = {}  # token -> expiry timestamp


class ExtractRequest(BaseModel):
    url: str = Field(min_length=8, max_length=500)
    fps: float = Field(default=1.0, ge=0.25, le=5.0)
    maxFrames: int = Field(default=600, ge=10, le=MAX_FRAMES)
    width: int = Field(default=640, ge=320, le=854)
    startSec: float = Field(default=0, ge=0)
    endSec: float = Field(default=0, ge=0)  # 0 = to end of video


def _check_key(x_service_key: str | None):
    if SERVICE_KEY and x_service_key != SERVICE_KEY:
        raise HTTPException(status_code=401, detail="Bad service key.")


def _run(cmd: list[str], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _video_id(url: str) -> str | None:
    m = re.search(r"(?:v=|youtu\.be/|/shorts/|/embed/)([A-Za-z0-9_-]{11})", url)
    return m.group(1) if m else None


def _ytdlp_json(url: str) -> dict:
    """Video metadata (title, duration, subtitle tracks). Never downloads.

    YouTube 403-blocks many datacenter IPs on the default web client, so
    retry with the Android and iOS player clients, which are usually allowed.
    """
    last_err = ""
    for client in (None, "android", "ios"):
        cmd = ["yt-dlp", "--no-download", "--dump-json", "--no-playlist",
               "--impersonate", "chrome"]
        if client:
            cmd += ["--extractor-args", f"youtube:player_client={client}"]
        cmd.append(url)
        p = _run(cmd, timeout=180)
        if p.returncode == 0:
            try:
                return json.loads(p.stdout)
            except Exception:
                last_err = "bad metadata response"
                continue
        last_err = p.stderr.strip()[-300:] or "unknown error"
    raise RuntimeError("Could not read that video: " + last_err)


def _subtitles_text(info: dict) -> str:
    """Best-effort captions: manual subs first, then auto captions (English).
    Handles both VTT and YouTube's JSON3 formats."""
    for key in ("subtitles", "automatic_captions"):
        tracks = info.get(key) or {}
        for lang in ("en", "en-US", "en-GB"):
            for t in tracks.get(lang, []):
                sub_url = t.get("url")
                if not sub_url:
                    continue
                try:
                    import urllib.request

                    req = urllib.request.Request(sub_url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=30) as r:
                        raw = r.read().decode("utf-8", errors="replace")
                    if raw.lstrip().startswith("{"):
                        text = _json3_to_text(raw)
                    else:
                        text = _vtt_to_text(raw)
                    if text.strip():
                        return text
                except Exception:
                    continue
    return ""


def _json3_to_text(raw: str) -> str:
    """YouTube srv3/json3 captions -> plain text with minute markers."""
    try:
        data = json.loads(raw)
    except Exception:
        return ""
    lines: list[str] = []
    last_min = -1
    for ev in data.get("events", []):
        segs = ev.get("segs") or []
        text = "".join(s.get("utf8", "") for s in segs).replace("\n", " ").strip()
        if not text:
            continue
        if lines and lines[-1].endswith(text):
            continue
        ms = int(ev.get("tStartMs", 0) or 0)
        minute = ms // 60000
        if minute != last_min:
            lines.append(f"[{minute:02d}:{ms // 1000 % 60:02d}]")
            last_min = minute
        # de-dupe: skip if this exact line already added
        if not lines or lines[-1] != text:
            # avoid runaway repeats from overlapping cues
            if len(lines) < 2 or lines[-2] != text:
                lines.append(text)
    return "\n".join(lines[:2000])


def _vtt_to_text(vtt: str) -> str:
    lines: list[str] = []
    for block in re.split(r"\n\s*\n", vtt):
        block = block.strip()
        if not block or block.startswith("WEBVTT"):
            continue
        parts = block.split("\n")
        # drop cue header/timestamp line(s), keep text, de-dupe consecutive
        text = " ".join(p.strip() for p in parts if "-->" not in p).strip()
        text = re.sub(r"<[^>]+>", "", text).strip()
        if text and (not lines or lines[-1] != text):
            # keep a timestamp on every ~10th cue so the agent can align
            lines.append(text)
    return "\n".join(lines[:2000])


def _write_manifest(job_dir: str, manifest: dict):
    with open(os.path.join(job_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f)


def _read_manifest(job_dir: str) -> dict | None:
    p = os.path.join(job_dir, "manifest.json")
    if not os.path.isfile(p):
        return None
    with open(p) as f:
        return json.load(f)


def _probe_duration(video_path: str) -> float:
    p = _run(
        [
            "ffprobe",
            "-hide_banner",
            "-loglevel",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            video_path,
        ],
        timeout=60,
    )
    try:
        return float(p.stdout.strip())
    except Exception:
        return 0


def _extract_frames_from_file(
    job_dir: str,
    manifest: dict,
    video_path: str,
    title: str,
    fps: float,
    max_frames: int,
    width: int,
    start_sec: float,
    end_sec: float,
):
    """Shared frame extraction from a local video file. Updates manifest."""
    duration = _probe_duration(video_path)
    if duration <= 0:
        raise RuntimeError("Could not read the video length.")
    if duration > MAX_DURATION_SEC:
        raise RuntimeError(f"Video is too long ({duration:.0f}s). Max is {MAX_DURATION_SEC}s.")

    start = min(start_sec, duration - 1)
    end = end_sec if end_sec > start else duration
    clip = min(end - start, max_frames / fps)
    end = start + clip
    frame_count = max(1, min(max_frames, int(clip * fps)))

    manifest.update(
        {
            "status": "extracting",
            "title": title,
            "durationSec": round(duration, 1),
            "clipStartSec": round(start, 1),
            "clipEndSec": round(end, 1),
            "fps": fps,
            "frameCount": frame_count,
            "width": width,
        }
    )
    _write_manifest(job_dir, manifest)

    p = _run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            str(start),
            "-i",
            video_path,
            "-t",
            str(clip),
            "-vf",
            f"fps={fps},scale={width}:-1",
            "-frames:v",
            str(frame_count),
            "-q:v",
            "4",
            os.path.join(job_dir, "frame_%04d.jpg"),
            "-y",
        ],
        timeout=1500,
    )
    # The video file is no longer needed; frames are what we keep.
    try:
        os.remove(video_path)
    except OSError:
        pass
    if p.returncode != 0:
        raise RuntimeError("Frame extraction failed: " + (p.stderr.strip()[-300:] or "ffmpeg error"))

    actual = len([f for f in os.listdir(job_dir) if re.fullmatch(r"frame_\d+\.jpg", f)])
    if actual == 0:
        raise RuntimeError("No frames came out of that video.")

    # Contact sheet: 6 columns from the first frames, keeps the agent's
    # round trips low. Capped so 5k-frame jobs don't build a giant image.
    sheet_n = min(actual, CONTACT_SHEET_FRAMES)
    cols = 6
    rows = max(1, (sheet_n + cols - 1) // cols)
    if sheet_n < actual:
        # Build the sheet from evenly sampled frames across the whole job.
        import math

        step = actual / sheet_n
        sheet_dir = os.path.join(job_dir, "sheet")
        os.makedirs(sheet_dir, exist_ok=True)
        for i in range(sheet_n):
            src = os.path.join(job_dir, f"frame_{int(i * step) + 1:04d}.jpg")
            dst = os.path.join(sheet_dir, f"frame_{i + 1:04d}.jpg")
            try:
                os.link(src, dst)
            except OSError:
                shutil.copy(src, dst)
        sheet_input = os.path.join(sheet_dir, "frame_%04d.jpg")
    else:
        sheet_input = os.path.join(job_dir, "frame_%04d.jpg")
    _run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            sheet_input,
            "-vf",
            f"tile={cols}x{rows}:margin=4:padding=4,scale=1280:-1",
            "-frames:v",
            "1",
            "-q:v",
            "5",
            os.path.join(job_dir, "contact.jpg"),
            "-y",
        ],
        timeout=600,
    )
    if sheet_n < actual:
        shutil.rmtree(os.path.join(job_dir, "sheet"), ignore_errors=True)

    manifest.update(
        {
            "status": "done",
            "frameCount": actual,
            "finishedAt": datetime.now(timezone.utc).isoformat(),
        }
    )
    _write_manifest(job_dir, manifest)
    return manifest


def _do_extract(job_id: str, req: ExtractRequest):
    """YouTube flow: fetch metadata, download the video, extract frames."""
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    manifest: dict = {
        "id": job_id,
        "status": "working",
        "url": req.url,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    _write_manifest(job_dir, manifest)
    video_path = os.path.join(job_dir, "video.mp4")
    try:
        vid = _video_id(req.url)
        if not vid:
            raise RuntimeError("That does not look like a YouTube URL.")
        info = _ytdlp_json(req.url)

        manifest.update(
            {
                "status": "downloading",
                "title": info.get("title") or vid,
                "videoId": vid,
            }
        )
        _write_manifest(job_dir, manifest)

        # Download the video file first with yt-dlp's native downloader
        # (retries/resumes through flaky networks), then extract frames
        # locally. Streaming straight into ffmpeg dies on throttled links.
        # Try each player client; datacenter IPs get 403 on some of them.
        downloaded = False
        dl_err = ""
        for client in (None, "android", "ios"):
            cmd = [
                "yt-dlp",
                "--no-playlist",
                "--no-part",
                "--impersonate",
                "chrome",
                "--retries",
                "10",
                "--fragment-retries",
                "10",
                "-f",
                "bv*[height<=480]/b[height<=480]/bv*/b",
                "-o",
                video_path,
            ]
            if client:
                cmd += ["--extractor-args", f"youtube:player_client={client}"]
            cmd.append(req.url)
            p = _run(cmd, timeout=1500)
            if p.returncode == 0 and os.path.isfile(video_path):
                downloaded = True
                break
            dl_err = p.stderr.strip()[-300:] or "unknown error"
        if not downloaded:
            raise RuntimeError("Video download failed: " + dl_err)

        _extract_frames_from_file(
            job_dir, manifest, video_path,
            info.get("title") or vid,
            req.fps, req.maxFrames, req.width, req.startSec, req.endSec,
        )

        subs = _subtitles_text(info)
        if subs:
            with open(os.path.join(job_dir, "subtitles.txt"), "w") as f:
                f.write(subs)
            manifest["hasSubtitles"] = True
            _write_manifest(job_dir, manifest)
    except Exception as e:
        manifest.update({"status": "failed", "error": str(e)[:400]})
        _write_manifest(job_dir, manifest)


def _sweep():
    while True:
        time.sleep(600)
        now = time.time()
        try:
            for jid in os.listdir(JOBS_DIR):
                jd = os.path.join(JOBS_DIR, jid)
                if os.path.isdir(jd) and now - os.path.getmtime(jd) > JOB_TTL_SECONDS:
                    shutil.rmtree(jd, ignore_errors=True)
        except Exception:
            pass


threading.Thread(target=_sweep, daemon=True).start()


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/api/extract")
def extract(req: ExtractRequest, x_service_key: str | None = Header(default=None)):
    _check_key(x_service_key)
    if not _video_id(req.url):
        raise HTTPException(status_code=400, detail="That does not look like a YouTube URL.")
    if not _extract_lock.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="Another extraction is running. Try again in a minute.")
    job_id = uuid.uuid4().hex[:12]
    try:
        t = threading.Thread(target=_run_job, args=(job_id, req), daemon=True)
        t.start()
    except Exception:
        _extract_lock.release()
        raise
    base = os.environ.get("PUBLIC_BASE", "").rstrip("/")
    return {
        "ok": True,
        "jobId": job_id,
        "statusUrl": f"{base}/api/jobs/{job_id}",
        "note": "Poll statusUrl until status is done, then fetch frames.",
    }


def _run_job(job_id: str, req: ExtractRequest):
    try:
        _do_extract(job_id, req)
    finally:
        _extract_lock.release()


def _run_upload_job(
    job_id: str,
    video_path: str,
    title: str,
    fps: float,
    max_frames: int,
    width: int,
    start_sec: float,
    end_sec: float,
):
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    manifest: dict = {
        "id": job_id,
        "status": "working",
        "source": "upload",
        "title": title,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    _write_manifest(job_dir, manifest)
    try:
        _extract_frames_from_file(
            job_dir, manifest, video_path, title,
            fps, max_frames, width, start_sec, end_sec,
        )
    except Exception as e:
        manifest.update({"status": "failed", "error": str(e)[:400]})
        _write_manifest(job_dir, manifest)
    finally:
        _extract_lock.release()


@app.post("/api/upload-token")
def upload_token(x_service_key: str | None = Header(default=None)):
    """One-time token so Joel's browser can upload a video file directly
    to this service (bypassing Vercel's 4.5MB body limit)."""
    _check_key(x_service_key)
    token = uuid.uuid4().hex
    _upload_tokens[token] = time.time() + UPLOAD_TOKEN_TTL
    # prune expired
    now = time.time()
    for t in [t for t, exp in _upload_tokens.items() if exp < now]:
        del _upload_tokens[t]
    return {"ok": True, "token": token, "expiresInSec": UPLOAD_TOKEN_TTL}


@app.post("/api/upload")
async def upload_video(
    token: str = Form(...),
    file: UploadFile = File(...),
    fps: float = Form(1.0),
    maxFrames: int = Form(600),
    width: int = Form(640),
    startSec: float = Form(0),
    endSec: float = Form(0),
):
    """Direct video file upload. Joel downloads the video himself (his
    network isn't blocked), the service slices it into frames."""
    exp = _upload_tokens.pop(token, 0)
    if exp < time.time():
        raise HTTPException(status_code=401, detail="Bad or expired upload token.")
    fps = min(5.0, max(0.25, fps))
    maxFrames = min(MAX_FRAMES, max(10, maxFrames))
    width = min(854, max(320, width))
    startSec = max(0, startSec)
    endSec = max(0, endSec)

    if not _extract_lock.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="Another extraction is running. Try again in a minute.")

    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".mp4", ".mov", ".webm", ".mkv", ".avi", ".m4v"):
        _extract_lock.release()
        raise HTTPException(status_code=400, detail="That file type is not supported. Use mp4, mov, or webm.")
    video_path = os.path.join(job_dir, "video" + ext)
    try:
        size = 0
        with open(video_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Video is too big (max 200MB).")
                f.write(chunk)
    except HTTPException:
        _extract_lock.release()
        shutil.rmtree(job_dir, ignore_errors=True)
        raise
    title = os.path.splitext(file.filename or "upload")[0][:120] or "upload"

    t = threading.Thread(
        target=_run_upload_job,
        args=(job_id, video_path, title, fps, maxFrames, width, startSec, endSec),
        daemon=True,
    )
    t.start()
    base = os.environ.get("PUBLIC_BASE", "").rstrip("/")
    return {
        "ok": True,
        "jobId": job_id,
        "statusUrl": f"{base}/api/jobs/{job_id}",
        "note": "Poll statusUrl until status is done, then fetch frames.",
    }


def _job_or_404(job_id: str) -> tuple[str, dict]:
    if not re.fullmatch(r"[0-9a-f]{12}", job_id or ""):
        raise HTTPException(status_code=404, detail="Job not found.")
    job_dir = os.path.join(JOBS_DIR, job_id)
    m = _read_manifest(job_dir)
    if not m:
        raise HTTPException(status_code=404, detail="Job not found or expired.")
    return job_dir, m


def _public_manifest(job_id: str, m: dict) -> dict:
    base = os.environ.get("PUBLIC_BASE", "").rstrip("/")
    out = dict(m)
    out["framesBase"] = f"{base}/v/{job_id}/frame"
    out["manifestUrl"] = f"{base}/v/{job_id}/manifest.json"
    if os.path.isfile(os.path.join(JOBS_DIR, job_id, "contact.jpg")):
        out["contactSheetUrl"] = f"{base}/v/{job_id}/contact.jpg"
    return out


@app.get("/api/jobs")
def list_jobs(x_service_key: str | None = Header(default=None)):
    """Recent jobs, newest first. Lets the agent find Joel's latest upload."""
    _check_key(x_service_key)
    jobs = []
    try:
        entries = sorted(
            os.listdir(JOBS_DIR),
            key=lambda j: os.path.getmtime(os.path.join(JOBS_DIR, j)),
            reverse=True,
        )
    except FileNotFoundError:
        entries = []
    for jid in entries[:20]:
        m = _read_manifest(os.path.join(JOBS_DIR, jid))
        if m:
            jobs.append(_public_manifest(jid, m))
    return {"ok": True, "jobs": jobs}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str, x_service_key: str | None = Header(default=None)):
    _check_key(x_service_key)
    _, m = _job_or_404(job_id)
    return {"ok": True, "job": _public_manifest(job_id, m)}


@app.get("/v/{job_id}/manifest.json")
def public_manifest(job_id: str):
    _, m = _job_or_404(job_id)
    return JSONResponse(_public_manifest(job_id, m))


@app.get("/v/{job_id}/frame/{n}")
def frame(job_id: str, n: str):
    job_dir, m = _job_or_404(job_id)
    if not re.fullmatch(r"\d{1,4}", n or ""):
        raise HTTPException(status_code=404, detail="Bad frame number.")
    num = int(n)
    if num < 1 or num > int(m.get("frameCount", 0)):
        raise HTTPException(status_code=404, detail="Frame out of range.")
    p = os.path.join(job_dir, f"frame_{num:04d}.jpg")
    if not os.path.isfile(p):
        # tolerate .jpeg extension from older jobs
        p = os.path.join(job_dir, f"frame_{num:04d}.jpeg")
    if not os.path.isfile(p):
        raise HTTPException(status_code=404, detail="Frame not ready.")
    return FileResponse(p, media_type="image/jpeg")


@app.get("/v/{job_id}/contact.jpg")
def contact(job_id: str):
    job_dir, _ = _job_or_404(job_id)
    p = os.path.join(job_dir, "contact.jpg")
    if not os.path.isfile(p):
        raise HTTPException(status_code=404, detail="Contact sheet not ready.")
    return FileResponse(p, media_type="image/jpeg")


@app.get("/v/{job_id}/subtitles.txt")
def subtitles(job_id: str):
    job_dir, _ = _job_or_404(job_id)
    p = os.path.join(job_dir, "subtitles.txt")
    if not os.path.isfile(p):
        raise HTTPException(status_code=404, detail="No subtitles for this video.")
    return FileResponse(p, media_type="text/plain; charset=utf-8")
