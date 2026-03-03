import os
import sys
import json
import time
import base64
import traceback
import subprocess
import threading
from flask import Flask, request, jsonify
from playwright.sync_api import sync_playwright, Error as PWError

# ============================================================
# CONFIG
# ============================================================

API_KEY = os.getenv("API_KEY")
if not API_KEY:
    print("FATAL: API_KEY not set", flush=True)
    sys.exit(1)

PORT = int(os.getenv("PORT", "10000"))
BROWSERS_PATH = "/tmp/ms-playwright"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH

DEFAULT_TIMEOUT = 30000

APP = Flask(__name__)

# Lock global: apenas 1 execução por vez
_run_lock = threading.Lock()
_in_use = False


# ============================================================
# UTILS
# ============================================================

def log(msg, **data):
    try:
        print(f"[{time.time():.3f}] {msg} " + " ".join(f"{k}={repr(v)}" for k, v in data.items()), flush=True)
    except:
        pass


def install_chromium():
    log("Installing chromium if needed...")
    os.makedirs(BROWSERS_PATH, exist_ok=True)

    cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )

    log("Install finished", returncode=proc.returncode)

    if proc.returncode != 0:
        log("Install failed", stderr=proc.stderr[-500:])
        raise RuntimeError("Chromium install failed")


def ensure_api_key(req):
    key = req.headers.get("x-api-key")
    return key == API_KEY


# ============================================================
# ROUTES
# ============================================================

@APP.get("/")
def root():
    return jsonify({"ok": True, "service": "single-shot-browser"})


@APP.get("/health")
def health():
    return jsonify({
        "ok": True,
        "in_use": _in_use,
        "python": sys.version,
        "pid": os.getpid()
    })


@APP.post("/run")
def run_browser():
    global _in_use

    if not ensure_api_key(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    if not _run_lock.acquire(blocking=False):
        return jsonify({"ok": False, "error": "busy"}), 409

    _in_use = True
    start_time = time.time()
    log("Execution started")

    try:
        body = request.get_json(force=True)

        url = body.get("url")
        if not url:
            return jsonify({"ok": False, "error": "missing url"}), 400

        viewport = body.get("viewport", {"width": 1280, "height": 800})
        wait_until = body.get("wait_until", "load")
        storage_state_b64 = body.get("storage_state_b64")

        storage_path = None

        # Se enviou profile/cookies
        if storage_state_b64:
            raw = base64.b64decode(storage_state_b64.encode())
            storage_path = f"/tmp/state_{int(time.time())}.json"
            with open(storage_path, "wb") as f:
                f.write(raw)
            log("Storage state saved", path=storage_path)

        # ====================================================
        # PLAYWRIGHT EXECUTION
        # ====================================================

        try:
            playwright = sync_playwright().start()
            log("Playwright started")

            try:
                browser = playwright.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu"
                    ]
                )
            except PWError:
                install_chromium()
                browser = playwright.chromium.launch(headless=True)

            log("Browser launched")

            context_args = {
                "viewport": viewport,
                "ignore_https_errors": True
            }

            if storage_path:
                context_args["storage_state"] = storage_path

            context = browser.new_context(**context_args)
            context.set_default_timeout(DEFAULT_TIMEOUT)

            page = context.new_page()

            log("Navigating", url=url)
            page.goto(url, wait_until=wait_until)

            title = page.title()
            final_url = page.url

            screenshot_bytes = page.screenshot(full_page=True)
            screenshot_b64 = base64.b64encode(screenshot_bytes).decode()

            log("Screenshot captured", size=len(screenshot_bytes))

            context.close()
            browser.close()
            playwright.stop()

            if storage_path and os.path.exists(storage_path):
                os.remove(storage_path)

            duration = round(time.time() - start_time, 3)
            log("Execution finished", duration=duration)

            return jsonify({
                "ok": True,
                "title": title,
                "final_url": final_url,
                "duration_s": duration,
                "screenshot_b64": screenshot_b64
            })

        except Exception as e:
            log("Execution error", error=str(e))
            return jsonify({
                "ok": False,
                "error": str(e),
                "traceback": traceback.format_exc(limit=10)
            }), 500

    finally:
        _in_use = False
        _run_lock.release()
        log("Lock released")


# ============================================================
# STARTUP LOG
# ============================================================

log("Service starting...")
log("API_KEY loaded", length=len(API_KEY))
log("Browsers path", path=BROWSERS_PATH)
log("Ready.")
