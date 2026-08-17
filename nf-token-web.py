import argparse
import importlib.util
import io
import json
import os
import queue
import re
import threading
import time
import uuid
from datetime import datetime

from flask import Flask, Response, jsonify, request, send_file, stream_with_context

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GEN_PATH = os.path.join(BASE_DIR, "nf-token-generator.py")
MY_COOKIE_FILE = os.path.join(BASE_DIR, "t_single.json")

spec = importlib.util.spec_from_file_location("nfgen", GEN_PATH)
nfgen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nfgen)

REQUIRED_COOKIE = nfgen.REQUIRED_COOKIE
COOKIE_KEYS = nfgen.COOKIE_KEYS

HEADER_RE = re.compile(r"^\[\s*(\d+)\]\s*(\S*)\s*$")
COMMENT_META_RE = re.compile(r"^#\s*\[\s*(\d+)\]\s*(\S*)\s*$")
SEP_RE = re.compile(r"^-{5,}")
MAX_JOBS = 100

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024

JOBS = {}
JOBS_LOCK = threading.RLock()
BULK_DELAY = 0.5
FETCH_TIMEOUT = 10


def fetch_account_info(cookie_dict):
    """Fetch region & plan metadata if available, fallback to Unknown."""
    region = "Unknown"
    plan = "Unknown"
    
    # Parse OptanonConsent for geolocation hint
    consent = cookie_dict.get("OptanonConsent", "")
    if consent:
        import urllib.parse
        try:
            parsed = urllib.parse.unquote(consent)
            # geolocation=US%3BNY or similar
            if "geolocation=" in parsed:
                for part in parsed.split("&"):
                    if part.startswith("geolocation="):
                        geo = part.split("=", 1)[1]
                        region = geo.replace("%3B", " ").replace(";", " ").replace("&", " ")
                        break
        except Exception:
            pass
    
    # NetflixId sometimes contains region hint in pg parameter
    netflix_id = cookie_dict.get("NetflixId", "")
    if netflix_id and "pg%3D" in netflix_id:
        # Extract pg value as potential region hint
        try:
            decoded = urllib.parse.unquote(netflix_id)
            if "pg=" in decoded:
                pg_val = decoded.split("pg=")[1].split("&")[0]
                region = pg_val[-2:] if len(pg_val) >= 2 else region
        except Exception:
            pass
    
    return region, plan


def extract_json_balanced(text):
    start = None
    for pos, char in enumerate(text):
        if char in ("{", "["):
            start = pos
            break
    if start is None:
        return None
    depth = 0
    for pos in range(start, len(text)):
        char = text[pos]
        if char in ("{", "["):
            depth += 1
        elif char in ("}", "]"):
            depth -= 1
            if depth == 0:
                return text[start : pos + 1]
    return None


def parse_block(text):
    cookie_dict = nfgen.extract_cookie_dict(text)
    if cookie_dict and REQUIRED_COOKIE in cookie_dict:
        return cookie_dict, None

    json_text = extract_json_balanced(text)
    if json_text:
        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, list):
            for cookie in data:
                name = cookie.get("name")
                value = cookie.get("value")
                if name in COOKIE_KEYS and isinstance(value, str):
                    cookie_dict[name] = nfgen._decode_cookie_value(value)
        elif isinstance(data, dict):
            for key in COOKIE_KEYS:
                value = data.get(key)
                if isinstance(value, str):
                    cookie_dict[key] = nfgen._decode_cookie_value(value)

    if cookie_dict and REQUIRED_COOKIE in cookie_dict:
        return cookie_dict, None
    return None, "Missing required cookie: " + REQUIRED_COOKIE


def split_cookie_blocks(text):
    lines = [line.rstrip("\r\n") for line in text.splitlines()]
    blocks = []
    index = 0
    total = len(lines)
    pending_meta = None

    def take_meta():
        meta = pending_meta
        return meta if meta is not None else (None, "")

    def reset_meta():
        nonlocal pending_meta
        pending_meta = None

    while index < total:
        stripped = lines[index].strip()
        if not stripped or SEP_RE.match(stripped):
            index += 1
            continue

        if stripped.startswith("#"):
            match = COMMENT_META_RE.match(stripped)
            pending_meta = (match.group(1), match.group(2)) if match else None
            index += 1
            continue

        match = HEADER_RE.match(stripped)
        if match:
            number, email = match.group(1), match.group(2)
            reset_meta()
            index += 1
            collected = []
            while index < total:
                inner = lines[index].strip()
                if (
                    not inner
                    or inner.startswith("#")
                    or SEP_RE.match(inner)
                    or HEADER_RE.match(inner)
                ):
                    break
                collected.append(lines[index])
                index += 1
            blocks.append((number, email, "\n".join(collected)))
            continue

        if stripped.startswith(("{", "[")):
            number, email = take_meta()
            reset_meta()
            depth = 0
            collected = []
            started = False
            while index < total:
                inner = lines[index].strip()
                if not started:
                    if (
                        not inner
                        or inner.startswith("#")
                        or SEP_RE.match(inner)
                        or HEADER_RE.match(inner)
                    ):
                        break
                    started = True
                else:
                    if (
                        not inner
                        or inner.startswith("#")
                        or SEP_RE.match(inner)
                        or HEADER_RE.match(inner)
                    ):
                        break
                depth += inner.count("{") + inner.count("[") - inner.count("}") - inner.count("]")
                collected.append(lines[index])
                index += 1
                if depth == 0:
                    break
            blocks.append((number, email, "\n".join(collected)))
            continue

        number, email = take_meta()
        reset_meta()
        collected = []
        while index < total:
            inner = lines[index].strip()
            if (
                not inner
                or inner.startswith("#")
                or SEP_RE.match(inner)
                or HEADER_RE.match(inner)
                or inner.startswith(("{", "["))
            ):
                break
            collected.append(lines[index])
            index += 1
        if collected:
            blocks.append((number, email, "\n".join(collected)))

    return blocks


class BulkJob:
    def __init__(self, job_id, blocks):
        self.job_id = job_id
        self.blocks = blocks
        self.total = len(blocks)
        self.done = 0
        self.success = 0
        self.failed = 0
        self.finished = False
        self.cancelled = False
        self.fatal = None
        self.results = []
        self.events = queue.Queue()
        self.stop_event = threading.Event()

    def publish(self, payload):
        self.events.put(payload)


def process_job(job_id):
    job = JOBS[job_id]
    try:
        for position, (number, email, block_text) in enumerate(job.blocks, 1):
            if job.stop_event.is_set():
                job.cancelled = True
                break
            cookie_dict, error = parse_block(block_text)
            label = email or ("#" + str(number) if number else str(position))
            if error:
                line = "[%s] %s | FAILED: %s" % (number or position, label, error)
                ok = False
            else:
                try:
                    token, expires = nfgen.fetch_nftoken(cookie_dict, timeout=FETCH_TIMEOUT)
                    region = "Unknown"
                    plan = "Unknown"
                    links = " | ".join(
                        "%s %s" % (label_v, url)
                        for label_v, url in nfgen.build_nftoken_links(token)
                    )
                    line = "[%s] %s | %s | %s | Region: %s | Plan: %s" % (
                        number or position,
                        label,
                        links,
                        nfgen.format_expiry(expires),
                        region or "Unknown",
                        plan or "Unknown",
                    )
                    ok = True
                except Exception as exc:
                    line = "[%s] %s | FAILED: %s" % (number or position, label, exc)
                    ok = False

            job.results.append((number or position, label, line, ok))
            job.done = position
            if ok:
                job.success += 1
            else:
                job.failed += 1
            job.publish({"position": position, "total": job.total, "line": line, "ok": ok})
            if position < job.total and not job.stop_event.is_set():
                time.sleep(BULK_DELAY)
    except Exception as exc:
        job.fatal = str(exc)
    finally:
        job.finished = True
        job.publish(
            {
                "position": job.done,
                "total": job.total,
                "finished": True,
                "cancelled": job.cancelled,
                "fatal": job.fatal,
            }
        )


@app.after_request
def no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


@app.route("/api/single", methods=["POST"])
def api_single():
    body = request.get_json(silent=True) or {}
    text = body.get("text") or ""
    if not isinstance(text, str):
        return jsonify({"ok": False, "error": "Payload tidak valid."})
    text = text.strip()
    if not text:
        return jsonify({"ok": False, "error": "Input kosong. Tempel cookie dulu."})

    blocks = split_cookie_blocks(text)
    if not blocks:
        return jsonify({"ok": False, "error": "Tidak ada blok cookie yang dikenali."})
    if len(blocks) > 1:
        return jsonify({"ok": False, "error": "Terdeteksi %d akun. Gunakan mode Bulk." % len(blocks)})

    number, email, block_text = blocks[0]
    cookie_dict, error = parse_block(block_text)
    if error:
        return jsonify({"ok": False, "error": error})

    try:
        token, expires = nfgen.fetch_nftoken(cookie_dict, timeout=FETCH_TIMEOUT)
        region, plan = fetch_account_info(cookie_dict)
        return jsonify(
            {
                "ok": True,
                "urls": [
                    {"label": label, "url": url}
                    for label, url in nfgen.build_nftoken_links(token)
                ],
                "expires": nfgen.format_expiry(expires),
                "region": region,
                "plan": plan,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


def _is_local_request():
    ip = request.remote_addr or ""
    return ip in ("127.0.0.1", "::1", "localhost")


@app.route("/api/single/file", methods=["POST"])
def api_single_file():
    """Local convenience: generate from your own MY_COOKIE_FILE, no paste needed.
    Loopback-only so it can never become a public account-distribution endpoint."""
    if not _is_local_request():
        return jsonify({"ok": False, "error": "Hanya bisa dari mesin lokal (localhost)."}), 403
    if not os.path.exists(MY_COOKIE_FILE):
        return jsonify({"ok": False, "error": "File tidak ditemukan: %s" % os.path.basename(MY_COOKIE_FILE)})
    try:
        with open(MY_COOKIE_FILE, "r", encoding="utf-8") as fh:
            text = fh.read()
    except Exception as exc:
        return jsonify({"ok": False, "error": "Gagal baca file: %s" % exc})

    cookie_dict = nfgen.extract_cookie_dict(text)
    if not cookie_dict or REQUIRED_COOKIE not in cookie_dict:
        return jsonify({"ok": False, "error": "Cookie di file tidak valid (NetflixId tak ada)."})

    try:
        token, expires = nfgen.fetch_nftoken(cookie_dict, timeout=FETCH_TIMEOUT)
        region, plan = fetch_account_info(cookie_dict)
        return jsonify(
            {
                "ok": True,
                "urls": [
                    {"label": label, "url": url}
                    for label, url in nfgen.build_nftoken_links(token)
                ],
                "expires": nfgen.format_expiry(expires),
                "region": region,
                "plan": plan,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


@app.route("/api/bulk/stop/<job_id>", methods=["POST"])
def api_bulk_stop(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "Unknown job"}), 404
    job.stop_event.set()
    return jsonify({"ok": True, "cancelled": job.cancelled})


@app.route("/api/bulk/start", methods=["POST"])
def api_bulk_start():
    body = request.get_json(silent=True) or {}
    text = body.get("text") or ""
    if not isinstance(text, str):
        return jsonify({"ok": False, "error": "Payload tidak valid."})
    text = text.strip()
    if not text:
        return jsonify({"ok": False, "error": "Input kosong."})

    blocks = split_cookie_blocks(text)
    if not blocks:
        return jsonify({"ok": False, "error": "Tidak ada blok akun yang dikenali."})

    with JOBS_LOCK:
        job_id = uuid.uuid4().hex[:12]
        job = BulkJob(job_id, blocks)
        JOBS[job_id] = job
        while len(JOBS) > MAX_JOBS:
            for old_id in list(JOBS):
                if JOBS[old_id].finished:
                    del JOBS[old_id]
                    break

    thread = threading.Thread(target=process_job, args=(job_id,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "job_id": job_id, "total": job.total})


@app.route("/api/bulk/events/<job_id>")
def api_bulk_events(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "Unknown job"}), 404

    def generate():
        while True:
            try:
                event = job.events.get(timeout=3)
                yield "data: %s\n\n" % json.dumps(event)
                if event.get("finished"):
                    return
            except queue.Empty:
                if job.finished:
                    return
                continue

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


@app.route("/api/bulk/download/<job_id>")
def api_bulk_download(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "Unknown job"}), 404

    lines = [
        "BATCH NFToken Results",
        "Generated: %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "=" * 80,
    ]
    for number, label, line, ok in job.results:
        lines.append(line)
    payload = "\n".join(lines) + "\n"
    return send_file(
        io.BytesIO(payload.encode("utf-8")),
        mimetype="text/plain",
        as_attachment=True,
        download_name="nftoken_results_%s.txt" % job_id,
    )


PAGE = r"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Netflix NFToken Generator</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{
  --primary:#e50914;--primary-active:#b80710;--primary-glow:rgba(229,9,20,0.35);--link:#6f7bff;
  --canvas:#0a0a0c;--canvas-deep:#050507;
  --card:rgba(20,20,24,0.75);--card-elev:rgba(30,30,36,0.85);--strong:rgba(42,42,50,0.9);
  --hair:rgba(255,255,255,0.08);--hair-soft:rgba(255,255,255,0.04);--hair-strong:rgba(255,255,255,0.15);
  --ink:#ffffff;--body:#a0a0ab;--muted:#70707c;--muted-soft:#50505a;
  --ok:#2ecc71;--fail:#ff4757;--nf:#e50914;
  --head:'Space Grotesk',ui-sans-serif,system-ui,sans-serif;
  --sans:'Inter',ui-sans-serif,system-ui,sans-serif;
  --mono:'JetBrains Mono','Fira Code',Consolas,monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
*{scrollbar-width:thin;scrollbar-color:var(--hair-strong) transparent}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--hair-strong);border-radius:9999px;border:2px solid var(--canvas);background-clip:padding-box}
::-webkit-scrollbar-thumb:hover{background:var(--muted-soft);border:2px solid var(--canvas);background-clip:padding-box}
::-webkit-scrollbar-corner{background:transparent}
.list::-webkit-scrollbar-thumb{border-color:var(--canvas-deep)}
.list::-webkit-scrollbar-thumb:hover{border-color:var(--canvas-deep)}
body{
  background:var(--canvas);color:var(--body);
  font-family:var(--sans);letter-spacing:-0.01em;
  min-height:100vh;padding:40px 24px;
  background-image:
    radial-gradient(80% 50% at 50% 0%, rgba(229,9,20,0.12), transparent 70%),
    radial-gradient(50% 30% at 80% 10%, rgba(0,7,205,0.1), transparent 60%);
  background-repeat:no-repeat;background-attachment:fixed;
}
.wrap{max-width:920px;margin:0 auto}
.header{display:flex;align-items:center;gap:14px;margin-bottom:8px}
.logo-icon{
  width:40px;height:40px;background:var(--primary);border-radius:12px;
  display:flex;align-items:center;justify-content:center;flex-shrink:0;
  box-shadow:0 0 24px var(--primary-glow);
}
.logo-icon svg{width:24px;height:24px;fill:#fff}
h1{font-family:var(--head);font-size:30px;font-weight:700;letter-spacing:-0.02em;color:var(--ink);line-height:1.15}
h1 b{color:var(--primary);font-weight:700}
.sub{color:var(--muted);font-size:14px;margin:4px 0 26px;line-height:1.5}
.tabs{display:flex;gap:8px;margin-bottom:20px;background:rgba(0,0,0,0.3);padding:4px;border-radius:12px;border:1px solid var(--hair)}
.tab{
  flex:1;background:transparent;border:none;color:var(--muted);
  padding:10px 16px;border-radius:8px;cursor:pointer;font-size:13.5px;font-weight:600;
  font-family:var(--sans);transition:all .2s ease;text-align:center;
}
.tab:hover{color:var(--ink)}
.tab.on{background:var(--primary);color:#fff;box-shadow:0 4px 12px var(--primary-glow)}
.panel{display:none}
.panel.on{display:block}
.card-box{
  background:var(--card);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
  border:1px solid var(--hair-strong);border-radius:16px;padding:20px;
  box-shadow:0 8px 32px rgba(0,0,0,0.4);
}
textarea{
  width:100%;min-height:200px;background:rgba(10,10,14,0.7);color:var(--ink);
  border:1px solid var(--hair);border-radius:10px;padding:14px;
  font-family:var(--mono);font-size:12.5px;line-height:1.6;resize:vertical;white-space:pre;
}
textarea:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px var(--primary-glow)}
.optional-meta{display:flex;gap:12px;margin-top:14px;flex-wrap:wrap}
.optional-meta .meta-field{flex:1;min-width:180px;display:flex;flex-direction:column;gap:6px}
.optional-meta input{width:100%;background:rgba(10,10,14,0.7);color:var(--ink);border:1px solid var(--hair);border-radius:8px;padding:10px 12px;font-family:var(--sans);font-size:13px;transition:all .2s}
.optional-meta input:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px var(--primary-glow)}
.optional-meta input::placeholder{color:var(--muted-soft)}
.optional-meta .field-label{font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.05em}
.row{display:flex;gap:10px;flex-wrap:wrap;margin:16px 0 0} 
button{
  background:var(--primary);border:none;color:#fff;padding:11px 22px;border-radius:8px;
  cursor:pointer;font-size:13.5px;font-weight:600;font-family:var(--sans);
  transition:all .2s ease;box-shadow:0 4px 12px var(--primary-glow);
}
button:hover{background:var(--primary-active);transform:translateY(-1px)}
button.ghost{background:var(--card-elev);border:1px solid var(--hair-strong);color:var(--ink);box-shadow:none}
button.ghost:hover{background:var(--strong);transform:translateY(-1px)}
button:disabled{opacity:.4;cursor:not-allowed;transform:none!important}
.hint{color:var(--muted);font-size:13px;margin-top:10px}
.hidden{display:none!important}
.single-result{margin-top:24px}
.result-head{font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:12px}
.link-card{
  background:var(--card-elev);border:1px solid var(--hair-strong);border-radius:12px;
  padding:16px;margin-bottom:12px;box-shadow:0 4px 16px rgba(0,0,0,0.2);
}
.link-card .tag{
  display:inline-block;background:rgba(229,9,20,0.15);color:var(--primary-light);border:1px solid rgba(229,9,20,0.3);
  font-size:10.5px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;
  padding:3px 10px;border-radius:9999px;margin-bottom:10px;
}
.link-card .url-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.link-card .url-text{flex:1;min-width:0;color:var(--link);font-family:var(--mono);font-size:12px;line-height:1.5;word-break:break-all;text-decoration:none}
.link-card .url-text:hover{text-decoration:underline}
.link-card .meta-row{display:flex;gap:16px;margin-top:12px;padding-top:10px;border-top:1px solid var(--hair);font-size:12px;color:var(--muted);flex-wrap:wrap}
.link-card .meta-row span{color:var(--ink);font-weight:500}
.open-btn, .copy-btn{
  background:rgba(255,255,255,0.05);border:1px solid var(--hair-strong);color:var(--ink);
  font-size:11px;font-weight:600;padding:6px 14px;border-radius:6px;cursor:pointer;flex-shrink:0;font-family:var(--sans);transition:all .15s;box-shadow:none;
}
.open-btn:hover, .copy-btn:hover{background:rgba(255,255,255,0.15);border-color:rgba(255,255,255,0.3)}
.single-result .meta{color:var(--muted);font-size:13px;margin-top:8px}
.single-result .meta span{color:var(--ink);font-weight:600}
.bar{height:8px;background:var(--card);border:1px solid var(--hair-strong);border-radius:9999px;margin:12px 0;overflow:hidden}
.bar>i{display:block;height:100%;width:0;background:var(--primary);transition:width .3s}
.sum{font-size:13px;color:var(--muted);margin-bottom:10px}
.list{
  background:var(--canvas-deep);border:1px solid var(--hair-strong);border-radius:12px;
  padding:12px;max-height:440px;overflow:auto;font-family:var(--mono);font-size:12.5px;line-height:1.8;
}
.row-line{display:flex;gap:8px;align-items:flex-start;padding:6px 8px;border-radius:6px;flex-wrap:wrap}
.row-line:hover{background:var(--card)}
.copy-btn{
  background:var(--card-elev);border:1px solid var(--hair-strong);color:var(--body);
  font-size:11px;font-weight:500;padding:5px 12px;border-radius:6px;cursor:pointer;
  flex-shrink:0;font-family:var(--sans);transition:background .15s,color .15s;
}
.copy-btn:hover{color:var(--ink);background:var(--strong)}
.err{
  color:var(--fail);font-size:13px;margin-top:12px;
  background:rgba(255,77,77,.08);border:1px solid rgba(255,77,77,.3);
  border-radius:8px;padding:10px 14px;
}
.wm{
  padding:12px 0;
  border-bottom:1px solid var(--hair);
  display:flex;gap:18px;flex-wrap:wrap;align-items:center;
  color:var(--muted);font-size:13px;
  margin-bottom:28px;
}
.wm .who{color:var(--body);font-weight:600}
.wm a{color:var(--muted);text-decoration:none}
.wm a:hover{color:var(--primary-glow)}
@media (max-width:640px){
  body{padding:24px 16px}
  h1{font-size:24px}
  .sub{font-size:13px;margin-bottom:20px}
  .tabs{gap:6px}
  .tab{flex:1;padding:10px 12px;text-align:center}
  .row{gap:6px}
  .row button{flex:1 1 auto;min-width:calc(50% - 3px)}
  textarea{min-height:200px;font-size:12.5px}
  .link-card a{font-size:12px}
  .list{font-size:11.5px}
  .wm{gap:12px;font-size:12px}
}
</style>
</head>
<body>
<div class="wrap">
  <div class="wm">
    <span class="who">Yuuashura</span>
    <a href="https://github.com/Yuuashura" target="_blank" rel="noopener">GitHub: Yuuashura</a>
    <a href="https://yuuashura.my.id" target="_blank" rel="noopener">Web: yuuashura.my.id</a>
    <a href="https://instagram.com/yudis.ashura" target="_blank" rel="noopener">Instagram: yudis.ashura</a>
  </div>

  <div class="header">
    <div class="logo-icon">
      <svg viewBox="0 0 24 24"><path d="M4 2v20l7-5 7 5V2H4zm12 14.5l-5-3.57-5 3.57V4h10v12.5z"/></svg>
    </div>
    <h1>Netflix <b>NFToken</b> Generator</h1>
  </div>
  <div class="sub">Tempel cookie (Netscape / raw / JSON) &rarr; dapatkan link login nftoken. Mode Single &amp; Bulk.</div>

  <div class="tabs">
    <button class="tab on" id="tabSingle">Single Mode</button>
    <button class="tab" id="tabBulk">Bulk Mode</button>
  </div>

  <div class="panel on" id="panelSingle">
    <div class="card-box">
    <div class="input-label">Tempelkan Token disini dan Generate Link Login</div>
    <textarea id="taSingle" spellcheck="false" placeholder="Contoh JSON:
{
    &quot;NetflixId&quot;: &quot;v%3D3%26ct%3D...&quot;,
    &quot;SecureNetflixId&quot;: &quot;v%3D3%26mac%3D...&quot;,
    &quot;nfvdid&quot;: &quot;...&quot;
}

atau raw:
NetflixId=v%3D3%26ct%3D...; SecureNetflixId=...; nfvdid=..."></textarea>
    <div class="row">
      <button id="btnSingle">Generate Link Login</button>
      <button class="ghost" id="fileSingle">Generate dari file-ku</button>
      <button class="ghost" id="exSingle">Contoh</button>
      <button class="ghost" id="clearSingle">Bersihkan</button>
    </div>
    <div class="err hidden" id="singleErr"></div>
    <div class="single-result hidden" id="singleResult">
      <div class="result-head">Login URL</div>
      <div id="singleLinks"></div>
      <div class="meta">Token Expires: <span id="singleExp"></span></div>
    </div>

    <div class="optional-meta">
      <div class="meta-field">
        <span class="field-label">Plan (isi manual)</span>
        <input type="text" id="inputPlan" placeholder="Contoh: Basic, Standard, Premium">
      </div>
      <div class="meta-field">
        <span class="field-label">Billing / Plan Expires</span>
        <input type="date" id="inputBilling">
      </div>
    </div>
  </div>

  <div class="panel" id="panelBulk">
    <textarea id="taBulk" spellcheck="false" placeholder="Tempel banyak akun. Bisa campur format, pisahkan tiap akun dengan baris kosong / komentar / header:

# [1] akun.satu@gmail.com
.netflix.com  TRUE  /  TRUE  1779826982  NetflixId  v%3D3%26ct%3Dxxx..
.netflix.com  TRUE  /  TRUE  1779826982  SecureNetflixId  v%3D3%26mac%3Dxxx.
.netflix.com  TRUE  /  TRUE  1779826982  nfvdid  BQFm..

# [2] akun.dua@gmail.com
NetflixId=v%3D3%26ct%3Dyyy..; SecureNetflixId=...; nfvdid=..

[3] akun.tiga@gmail.com
{ &quot;NetflixId&quot;: &quot;...&quot; }"></textarea>
    <div class="row">
      <button id="startBulk">Generate Semua</button>
      <button class="ghost" id="exBulk">Contoh</button>
      <button class="ghost" id="clearBulk">Bersihkan</button>
      <button class="ghost hidden" id="dlBulk">Download .txt</button>
    </div>
    <div class="err hidden" id="bulkErr"></div>
    <div class="hint hidden" id="bulkHint"></div>
    <div class="bar hidden" id="barWrap"><i id="barFill"></i></div>
    <div class="sum hidden" id="bulkSum"></div>
    <button class="ghost hidden" id="stopBulk">Stop</button>
    <div class="list hidden" id="bulkList"></div>
  </div>

</div>

<script>
var $ = function (id) { return document.getElementById(id); };

var exSingleText = JSON.stringify({
  "NetflixId": "v%3D3%26ct%3D...",
  "SecureNetflixId": "v%3D3%26mac%3D...",
  "nfvdid": "..."
}, null, 2);

var exBulkText = '# [1] akun.contoh@gmail.com\n.netflix.com\tTRUE\t/\tTRUE\t1779826982\tNetflixId\tv%3D3%26ct%3Dxxx..\n.netflix.com\tTRUE\t/\tTRUE\t1779826982\tSecureNetflixId\tv%3D3%26mac%3Dxxx.\n.netflix.com\tTRUE\t/\tTRUE\t1779826982\tnfvdid\tBQFm..\n\n# [2] akun.raw@gmail.com\nNetflixId=v%3D3%26ct%3Dyyy..; SecureNetflixId=v%3D3%26mac%3Dyyy.; nfvdid=BQFm..';

function switchTab(name) {
  var single = name === 'single';
  $('tabSingle').classList.toggle('on', single);
  $('tabBulk').classList.toggle('on', !single);
  $('panelSingle').classList.toggle('on', single);
  $('panelBulk').classList.toggle('on', !single);
}
$('tabSingle').onclick = function () { switchTab('single'); };
$('tabBulk').onclick = function () { switchTab('bulk'); };

$('exSingle').onclick = function () { $('taSingle').value = exSingleText; };
$('clearSingle').onclick = function () {
  $('taSingle').value = '';
  $('singleErr').classList.add('hidden');
  $('singleResult').classList.add('hidden');
};
$('exBulk').onclick = function () { $('taBulk').value = exBulkText; };
$('clearBulk').onclick = function () {
  $('taBulk').value = '';
  $('bulkErr').classList.add('hidden');
  $('bulkHint').classList.add('hidden');
  $('barWrap').classList.add('hidden');
  $('bulkSum').classList.add('hidden');
  $('bulkList').classList.add('hidden');
  $('bulkList').textContent = '';
  $('dlBulk').classList.add('hidden');
};

function copyText(t, btn) {
  if (navigator.clipboard) {
    navigator.clipboard.writeText(t).then(function () {
      btn.textContent = 'ok';
      setTimeout(function () { btn.textContent = 'copy'; }, 1000);
    });
  }
}

function buildRow(line, ok) {
  var d = document.createElement('div');
  d.className = 'row-line';
  var s = document.createElement('span');
  s.textContent = (ok ? '\u25cf ' : '\u2717 ') + line;
  s.style.color = ok ? 'var(--ok)' : 'var(--fail)';
  d.appendChild(s);
  var re = /https:\/\/netflix\.com\/(?:tv2\?|unsupported\?|\?)nftoken=[A-Za-z0-9_\-\+\.\=\~\%]+/g;
  var match = line.match(re);
  if (match) {
    match.forEach(function (url) {
      var c = document.createElement('button');
      c.className = 'copy-btn';
      c.textContent = 'copy';
      c.onclick = function () { copyText(url, c); };
      d.appendChild(c);
      var o = document.createElement('button');
      o.className = 'open-btn';
      o.textContent = 'open';
      o.onclick = function () { window.open(url, '_blank', 'noopener'); };
      d.appendChild(o);
    });
  }
  return d;
}

function buildLinkRow(label, url, region, plan) {
  var d = document.createElement('div');
  d.className = 'link-card';
  var tag = document.createElement('span');
  tag.className = 'tag';
  tag.textContent = label;
  d.appendChild(tag);

  var urlRow = document.createElement('div');
  urlRow.className = 'url-row';
  var urlSpan = document.createElement('span');
  urlSpan.className = 'url-text';

  var shortUrl = url;
  var idx = url.indexOf('nftoken=');
  if (idx !== -1) {
    var tok = url.substring(idx + 8);
    shortUrl = 'nftoken=...' + tok.slice(-12);
  }
  urlSpan.textContent = shortUrl;
  urlSpan.title = url;
  urlRow.appendChild(urlSpan);

  var copyBtn = document.createElement('button');
  copyBtn.className = 'copy-btn';
  copyBtn.textContent = 'copy';
  copyBtn.onclick = function () { copyText(url, copyBtn); };
  urlRow.appendChild(copyBtn);

  var openBtn = document.createElement('button');
  openBtn.className = 'open-btn';
  openBtn.textContent = 'open';
  openBtn.onclick = function () { window.open(url, '_blank', 'noopener'); };
  urlRow.appendChild(openBtn);

  d.appendChild(urlRow);

  var metaRow = document.createElement('div');
  metaRow.className = 'meta-row';

  var rDiv = document.createElement('div');
  rDiv.textContent = 'Region: ';
  var rSpan = document.createElement('span');
  rSpan.textContent = region || 'Unknown (estimate)';
  rDiv.appendChild(rSpan);

  var pDiv = document.createElement('div');
  pDiv.textContent = 'Plan: ';
  var pSpan = document.createElement('span');
  pSpan.textContent = plan || 'Unknown (API limited)';
  pSpan.style.fontStyle = 'italic';
  pDiv.appendChild(pSpan);

  var nDiv = document.createElement('div');
  nDiv.textContent = 'Note: ';
  var nSpan = document.createElement('span');
  nSpan.textContent = 'Plan/billing date not available via current API';
  nSpan.style.fontSize = '11px';
  nSpan.style.color = 'var(--muted-soft)';
  nDiv.appendChild(nSpan);

  metaRow.appendChild(rDiv);
  metaRow.appendChild(pDiv);
  metaRow.appendChild(nDiv);
  d.appendChild(metaRow);

  return d;
}

function renderSingle(data) {
  if (data.ok) {
    var box = $('singleLinks');
    box.textContent = '';
    (data.urls || []).forEach(function (item) {
      box.appendChild(buildLinkRow(item.label, item.url, data.region, data.plan));
    });
    $('singleExp').textContent = data.expires;
    $('singleResult').classList.remove('hidden');
  } else {
    $('singleErr').textContent = 'Gagal: ' + data.error;
    $('singleErr').classList.remove('hidden');
  }
}

$('btnSingle').onclick = function () {
  var text = $('taSingle').value;
  if (!text.trim()) {
    $('singleErr').textContent = 'Input kosong. Tempel cookie dulu (klik Contoh untuk lihat format).';
    $('singleErr').classList.remove('hidden');
    return;
  }
  var btn = $('btnSingle');
  btn.disabled = true;
  btn.textContent = 'Memproses...';
  $('singleErr').classList.add('hidden');
  $('singleResult').classList.add('hidden');
  fetch('/api/single', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text: text })
  }).then(function (r) { return r.json(); }).then(function (data) {
    btn.disabled = false;
    btn.textContent = 'Generate Link Login';
    renderSingle(data);
  }).catch(function (e) {
    btn.disabled = false;
    btn.textContent = 'Generate Link Login';
    $('singleErr').textContent = 'Request error: ' + e;
    $('singleErr').classList.remove('hidden');
  });
};

$('fileSingle').onclick = function () {
  var btn = $('fileSingle');
  btn.disabled = true;
  btn.textContent = 'Membaca file...';
  $('singleErr').classList.add('hidden');
  $('singleResult').classList.add('hidden');
  fetch('/api/single/file', { method: 'POST' })
    .then(function (r) { return r.json(); }).then(function (data) {
      btn.disabled = false;
      btn.textContent = 'Generate dari file-ku';
      renderSingle(data);
    }).catch(function (e) {
      btn.disabled = false;
      btn.textContent = 'Generate dari file-ku';
      $('singleErr').textContent = 'Request error: ' + e;
      $('singleErr').classList.remove('hidden');
    });
};

var sse = null;
var jobId = null;
var startedAt = null;

function fmtEta(secs) {
  secs = Math.max(0, Math.round(secs));
  if (secs < 60) return secs + ' detik';
  if (secs < 3600) return Math.round(secs / 60) + ' menit';
  return (secs / 3600).toFixed(1) + ' jam';
}

$('stopBulk').onclick = function () {
  if (jobId && sse) {
    fetch('/api/bulk/stop/' + jobId, { method: 'POST' });
    $('stopBulk').disabled = true;
    $('stopBulk').textContent = 'Menghentikan...';
  }
};

$('startBulk').onclick = function () {
  var text = $('taBulk').value;
  if (!text.trim()) {
    $('bulkErr').textContent = 'Input kosong. Tempel akun dulu (klik Contoh untuk lihat format).';
    $('bulkErr').classList.remove('hidden');
    return;
  }
  var btn = $('startBulk');
  btn.disabled = true;
  btn.textContent = 'Memulai...';
  $('bulkErr').classList.add('hidden');
  $('bulkHint').classList.remove('hidden');
  $('barWrap').classList.remove('hidden');
  $('bulkSum').classList.remove('hidden');
  $('bulkList').classList.remove('hidden');
  $('bulkList').textContent = '';
  $('barFill').style.width = '0%';
  $('bulkSum').textContent = 'Memulai...';
  $('dlBulk').classList.add('hidden');
  $('stopBulk').classList.add('hidden');

  fetch('/api/bulk/start', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text: text })
  }).then(function (r) { return r.json(); }).then(function (data) {
    if (!data.ok) {
      btn.disabled = false;
      $('bulkErr').textContent = 'Gagal: ' + data.error;
      $('bulkErr').classList.remove('hidden');
      return;
    }
    jobId = data.job_id;
    startedAt = Date.now();
    if (data.total > 100 && !window.confirm('Terdeteksi ' + data.total + ' akun. Proses ini kira-kira ' +
        fmtEta(data.total * 12) + ' (tergantung koneksi). Lanjutkan?')) {
      fetch('/api/bulk/stop/' + data.job_id, { method: 'POST' });
      btn.disabled = false;
      $('bulkHint').textContent = 'Dibatalkan.';
      $('bulkSum').textContent = '';
      return;
    }
    $('bulkHint').textContent = 'Terdeteksi ' + data.total + ' akun. Memproses...';
    $('stopBulk').classList.remove('hidden');
    $('stopBulk').disabled = false;
    $('stopBulk').textContent = 'Stop';
    if (sse) { sse.close(); }
    sse = new EventSource('/api/bulk/events/' + data.job_id);
    sse.onmessage = function (e) {
      var ev = JSON.parse(e.data);
      if (ev.line) {
        $('bulkList').appendChild(buildRow(ev.line, ev.ok));
        $('bulkList').scrollTop = $('bulkList').scrollHeight;
      }
      if (ev.total && ev.position) {
        var pct = Math.round((ev.position / ev.total) * 100);
        $('barFill').style.width = pct + '%';
        var elapsed = (Date.now() - startedAt) / 1000;
        var eta = (elapsed / ev.position) * (ev.total - ev.position);
        if (ev.cancelled) {
          $('bulkSum').textContent = 'Dihentikan di ' + ev.position + ' / ' + ev.total;
        } else {
          $('bulkSum').textContent = 'Proses ' + ev.position + ' / ' + ev.total + ' \u00b7 sisa \u2248 ' + fmtEta(eta);
        }
      }
      if (ev.finished) {
        sse.close();
        btn.disabled = false;
        $('stopBulk').classList.add('hidden');
        var okLines = 0;
        $('bulkList').querySelectorAll('.row-line').forEach(function (row) {
          if (row.querySelector('span').style.color === 'var(--ok)') { okLines++; }
        });
        $('bulkHint').textContent = ev.cancelled ? 'Batch dihentikan oleh pengguna.' :
          ('Selesai' + (ev.fatal ? ' (fatal: ' + ev.fatal + ')' : '') + '.');
        var doneCount = ev.position || ev.total;
        $('bulkSum').textContent = 'Diproses ' + doneCount + ' | Sukses ' + okLines + ' | Gagal ' + (doneCount - okLines);
        if (jobId) {
          $('dlBulk').classList.remove('hidden');
          $('dlBulk').onclick = function () { window.location.href = '/api/bulk/download/' + jobId; };
        }
      }
    };
    sse.onerror = function () {
      if (sse) { sse.close(); }
      btn.disabled = false;
      $('bulkErr').textContent = 'Koneksi SSE terputus. Coba lagi.';
      $('bulkErr').classList.remove('hidden');
    };
  }).catch(function (e) {
    btn.disabled = false;
    $('bulkErr').textContent = 'Request error: ' + e;
    $('bulkErr').classList.remove('hidden');
  });
};
</script>
</body>
</html>"""


@app.route("/api/ping")
def api_ping():
    return jsonify({"ok": True})


def main():
    parser = argparse.ArgumentParser(description="Netflix NFToken Generator - Web")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--delay", type=float, default=0.5, help="Delay antar akun di mode bulk (detik)")
    parser.add_argument("--timeout", type=float, default=10, help="Timeout tiap request ke Netflix (detik)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    global BULK_DELAY, FETCH_TIMEOUT
    BULK_DELAY = args.delay
    FETCH_TIMEOUT = args.timeout

    print("Netflix NFToken Generator - Web")
    print("Open: http://%s:%d" % (args.host, args.port))
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()
