import os
import sys
import json
import time
import base64
import traceback
import subprocess
import threading
from typing import Any, Dict, List, Optional

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

DEFAULT_TIMEOUT_MS = 30_000
MAX_TIMEOUT_MS = 180_000
MAX_EXTRA_WAIT_MS = 120_000
MAX_CAPTURE_FOR_MS = 120_000
MAX_CAPTURE_EVERY_MS = 60_000
MAX_FRAMES = 60

APP = Flask(__name__)

_run_lock = threading.Lock()
_in_use = False


# ============================================================
# UTILS
# ============================================================

def log(msg: str, **data: Any) -> None:
    try:
        suffix = " ".join(f"{k}={repr(v)}" for k, v in data.items())
        print(f"[{time.time():.3f}] {msg} {suffix}".rstrip(), flush=True)
    except Exception:
        pass


def clamp_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(value)
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def ensure_api_key(req) -> bool:
    return req.headers.get("x-api-key") == API_KEY


def install_chromium() -> None:
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
        log("Install failed", stderr=proc.stderr[-1000:])
        raise RuntimeError("Chromium install failed")


def b64_to_file(data_b64: str, path: str) -> str:
    raw = base64.b64decode(data_b64.encode("utf-8"))
    with open(path, "wb") as f:
        f.write(raw)
    return path


def to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")


def clean_file(path: Optional[str]) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
            log("Temp file removed", path=path)
    except Exception as e:
        log("Temp file remove failed", path=path, error=str(e))


def read_json_body() -> Dict[str, Any]:
    try:
        body = request.get_json(force=True)
        return body if isinstance(body, dict) else {}
    except Exception:
        return {}


def normalize_wait_until(value: Any) -> str:
    allowed = {"load", "domcontentloaded", "networkidle", "commit"}
    v = str(value or "load").strip().lower()
    return v if v in allowed else "load"


def normalize_viewport(value: Any) -> Dict[str, int]:
    if not isinstance(value, dict):
        return {"width": 1280, "height": 800}
    width = clamp_int(value.get("width"), 1280, 320, 4000)
    height = clamp_int(value.get("height"), 800, 240, 4000)
    return {"width": width, "height": height}


def normalize_screenshot_type(value: Any) -> str:
    v = str(value or "png").strip().lower()
    return "jpeg" if v == "jpeg" else "png"


def normalize_headers(value: Any) -> Optional[Dict[str, str]]:
    if not isinstance(value, dict):
        return None
    out: Dict[str, str] = {}
    for k, v in value.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out or None


def normalize_browser_args(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def normalize_init_scripts(value: Any) -> List[str]:
    if isinstance(value, str) and value.strip():
        return [value]
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, str) and item.strip():
                out.append(item)
        return out
    return []


def jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


# ============================================================
# STEP ENGINE
# ============================================================

ALLOWED_STEP_OPS = {
    "goto",
    "wait_for_selector",
    "wait_for_timeout",
    "click",
    "fill",
    "press",
    "type",
    "hover",
    "evaluate",
    "set_content",
}

def execute_steps(page, steps: Any) -> List[Dict[str, Any]]:
    if not isinstance(steps, list):
        return []

    results: List[Dict[str, Any]] = []

    for idx, step in enumerate(steps):
        if not isinstance(step, dict):
            raise ValueError(f"step {idx} must be object")

        op = str(step.get("op", "")).strip()
        if op not in ALLOWED_STEP_OPS:
            raise ValueError(f"step {idx} invalid op: {op}")

        t0 = time.time()

        if op == "goto":
            url = step.get("url")
            if not isinstance(url, str) or not url.strip():
                raise ValueError(f"step {idx} goto missing url")
            wait_until = normalize_wait_until(step.get("wait_until", "load"))
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            result = {"ok": True, "op": op, "url": page.url}

        elif op == "wait_for_selector":
            selector = step.get("selector")
            if not isinstance(selector, str) or not selector.strip():
                raise ValueError(f"step {idx} wait_for_selector missing selector")
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            state = str(step.get("state", "visible")).strip()
            page.wait_for_selector(selector, timeout=timeout_ms, state=state)
            result = {"ok": True, "op": op, "selector": selector, "state": state}

        elif op == "wait_for_timeout":
            timeout_ms = clamp_int(step.get("timeout_ms"), 1_000, 1, MAX_EXTRA_WAIT_MS)
            page.wait_for_timeout(timeout_ms)
            result = {"ok": True, "op": op, "timeout_ms": timeout_ms}

        elif op == "click":
            selector = step.get("selector")
            if not isinstance(selector, str) or not selector.strip():
                raise ValueError(f"step {idx} click missing selector")
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            button = str(step.get("button", "left")).strip()
            page.click(selector, timeout=timeout_ms, button=button)
            result = {"ok": True, "op": op, "selector": selector}

        elif op == "fill":
            selector = step.get("selector")
            value = step.get("value")
            if not isinstance(selector, str) or not selector.strip():
                raise ValueError(f"step {idx} fill missing selector")
            if not isinstance(value, str):
                raise ValueError(f"step {idx} fill missing value")
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            page.fill(selector, value, timeout=timeout_ms)
            result = {"ok": True, "op": op, "selector": selector}

        elif op == "press":
            selector = step.get("selector")
            key = step.get("key")
            if not isinstance(selector, str) or not selector.strip():
                raise ValueError(f"step {idx} press missing selector")
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"step {idx} press missing key")
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            page.press(selector, key, timeout=timeout_ms)
            result = {"ok": True, "op": op, "selector": selector, "key": key}

        elif op == "type":
            selector = step.get("selector")
            value = step.get("value")
            if not isinstance(selector, str) or not selector.strip():
                raise ValueError(f"step {idx} type missing selector")
            if not isinstance(value, str):
                raise ValueError(f"step {idx} type missing value")
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            delay_ms = clamp_int(step.get("delay_ms"), 0, 0, 500)
            page.type(selector, value, timeout=timeout_ms, delay=delay_ms)
            result = {"ok": True, "op": op, "selector": selector, "len": len(value)}

        elif op == "hover":
            selector = step.get("selector")
            if not isinstance(selector, str) or not selector.strip():
                raise ValueError(f"step {idx} hover missing selector")
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            page.hover(selector, timeout=timeout_ms)
            result = {"ok": True, "op": op, "selector": selector}

        elif op == "evaluate":
            expression = step.get("expression")
            arg = step.get("arg")
            if not isinstance(expression, str) or not expression.strip():
                raise ValueError(f"step {idx} evaluate missing expression")
            value = page.evaluate(expression, arg)
            result = {"ok": True, "op": op, "result": jsonable(value)}

        elif op == "set_content":
            html = step.get("html")
            if not isinstance(html, str):
                raise ValueError(f"step {idx} set_content missing html")
            wait_until = normalize_wait_until(step.get("wait_until", "load"))
            timeout_ms = clamp_int(step.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
            page.set_content(html, wait_until=wait_until, timeout=timeout_ms)
            result = {"ok": True, "op": op}

        else:
            raise ValueError(f"step {idx} unsupported op: {op}")

        result["dt_ms"] = round((time.time() - t0) * 1000, 3)
        results.append(result)
        log("Step executed", index=idx, op=op, dt_ms=result["dt_ms"])

    return results


# ============================================================
# SCREENSHOT
# ============================================================

def capture_one(page, screenshot_type: str, full_page: bool, jpeg_quality: int) -> str:
    kwargs: Dict[str, Any] = {
        "type": screenshot_type,
        "full_page": full_page,
    }
    if screenshot_type == "jpeg":
        kwargs["quality"] = jpeg_quality
    data = page.screenshot(**kwargs)
    return to_b64(data)


def capture_sequence(page, screenshot_type: str, full_page: bool, jpeg_quality: int,
                     capture_every_ms: int, capture_for_ms: int, max_frames: int) -> List[str]:
    frames: List[str] = []
    started = time.time()
    next_at = started
    idx = 0

    while True:
        now = time.time()
        elapsed_ms = int((now - started) * 1000)

        if elapsed_ms > capture_for_ms:
            break
        if idx >= max_frames:
            break

        if now < next_at:
            sleep_ms = int((next_at - now) * 1000)
            if sleep_ms > 0:
                page.wait_for_timeout(sleep_ms)

        idx += 1
        frame = capture_one(page, screenshot_type, full_page, jpeg_quality)
        frames.append(frame)
        log("Frame captured", index=idx, elapsed_ms=elapsed_ms, bytes_b64=len(frame))
        next_at += capture_every_ms / 1000.0

    return frames


# ============================================================
# ROUTES
# ============================================================

@APP.get("/")
def root():
    return jsonify({"ok": True, "service": "single-shot-browser-flex"})


@APP.get("/health")
def health():
    return jsonify({
        "ok": True,
        "in_use": _in_use,
        "python": sys.version,
        "pid": os.getpid(),
        "browsers_path": BROWSERS_PATH,
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
    playwright = None
    browser = None
    context = None
    storage_path = None

    log("Execution started")

    try:
        body = read_json_body()

        url = body.get("url")
        if not isinstance(url, str) or not url.strip():
            return jsonify({"ok": False, "error": "missing url"}), 400

        viewport = normalize_viewport(body.get("viewport"))
        wait_until = normalize_wait_until(body.get("wait_until", "load"))
        timeout_ms = clamp_int(body.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1_000, MAX_TIMEOUT_MS)
        full_page = bool(body.get("full_page", True))
        screenshot_type = normalize_screenshot_type(body.get("screenshot_type", "png"))
        jpeg_quality = clamp_int(body.get("jpeg_quality"), 85, 1, 100)

        wait_for_selector = body.get("wait_for_selector")
        wait_selector_timeout_ms = clamp_int(
            body.get("wait_selector_timeout_ms"), timeout_ms, 1_000, MAX_TIMEOUT_MS
        )
        extra_wait_ms = clamp_int(body.get("extra_wait_ms"), 0, 0, MAX_EXTRA_WAIT_MS)

        capture_sequence_enabled = bool(body.get("capture_sequence", False))
        capture_every_ms = clamp_int(body.get("capture_every_ms"), 5_000, 500, MAX_CAPTURE_EVERY_MS)
        capture_for_ms = clamp_int(body.get("capture_for_ms"), 10_000, 500, MAX_CAPTURE_FOR_MS)
        max_frames = clamp_int(body.get("max_frames"), MAX_FRAMES, 1, MAX_FRAMES)

        storage_state_b64 = body.get("storage_state_b64")
        user_agent = body.get("user_agent")
        locale = body.get("locale")
        timezone_id = body.get("timezone_id")
        ignore_https_errors = bool(body.get("ignore_https_errors", True))
        extra_http_headers = normalize_headers(body.get("extra_http_headers"))
        browser_args = normalize_browser_args(body.get("browser_args"))
        init_scripts = normalize_init_scripts(body.get("init_script"))
        steps = body.get("steps")

        if isinstance(storage_state_b64, str) and storage_state_b64.strip():
            storage_path = f"/tmp/state_{int(time.time() * 1000)}.json"
            b64_to_file(storage_state_b64, storage_path)
            log("Storage state saved", path=storage_path)

        playwright = sync_playwright().start()
        log("Playwright started")

        launch_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ]
        launch_args.extend(browser_args)

        try:
            browser = playwright.chromium.launch(
                headless=True,
                args=launch_args,
            )
        except PWError:
            install_chromium()
            browser = playwright.chromium.launch(
                headless=True,
                args=launch_args,
            )

        log("Browser launched", args=launch_args)

        context_args: Dict[str, Any] = {
            "viewport": viewport,
            "ignore_https_errors": ignore_https_errors,
        }

        if storage_path:
            context_args["storage_state"] = storage_path
        if isinstance(user_agent, str) and user_agent.strip():
            context_args["user_agent"] = user_agent.strip()
        if isinstance(locale, str) and locale.strip():
            context_args["locale"] = locale.strip()
        if isinstance(timezone_id, str) and timezone_id.strip():
            context_args["timezone_id"] = timezone_id.strip()
        if extra_http_headers:
            context_args["extra_http_headers"] = extra_http_headers

        context = browser.new_context(**context_args)
        context.set_default_timeout(timeout_ms)
        context.set_default_navigation_timeout(timeout_ms)

        for script in init_scripts:
            context.add_init_script(script)
        if init_scripts:
            log("Init scripts added", count=len(init_scripts))

        page = context.new_page()
        log("Page created")

        log("Navigating", url=url, wait_until=wait_until)
        page.goto(url, wait_until=wait_until, timeout=timeout_ms)

        step_results = execute_steps(page, steps)

        if isinstance(wait_for_selector, str) and wait_for_selector.strip():
            selector = wait_for_selector.strip()
            log("Waiting for selector", selector=selector, timeout_ms=wait_selector_timeout_ms)
            page.wait_for_selector(selector, timeout=wait_selector_timeout_ms)
            log("Selector ready", selector=selector)

        if extra_wait_ms > 0:
            log("Extra wait", extra_wait_ms=extra_wait_ms)
            page.wait_for_timeout(extra_wait_ms)

        title = page.title()
        final_url = page.url
        content_length = len(page.content())

        response: Dict[str, Any] = {
            "ok": True,
            "title": title,
            "final_url": final_url,
            "step_results": step_results,
            "content_length": content_length,
        }

        if capture_sequence_enabled:
            log(
                "Capturing sequence",
                every_ms=capture_every_ms,
                for_ms=capture_for_ms,
                max_frames=max_frames,
            )
            frames = capture_sequence(
                page=page,
                screenshot_type=screenshot_type,
                full_page=full_page,
                jpeg_quality=jpeg_quality,
                capture_every_ms=capture_every_ms,
                capture_for_ms=capture_for_ms,
                max_frames=max_frames,
            )
            response["screenshots_b64"] = frames
            response["frames"] = len(frames)
        else:
            screenshot_b64 = capture_one(page, screenshot_type, full_page, jpeg_quality)
            response["screenshot_b64"] = screenshot_b64
            response["frames"] = 1
            log("Screenshot captured", bytes_b64=len(screenshot_b64))

        duration = round(time.time() - start_time, 3)
        response["duration_s"] = duration

        log("Execution finished", duration=duration, final_url=final_url, title=title)
        return jsonify(response)

    except Exception as e:
        log("Execution error", error=str(e))
        return jsonify({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc(limit=20),
        }), 500

    finally:
        try:
            if context is not None:
                context.close()
                log("Context closed")
        except Exception as e:
            log("Context close failed", error=str(e))

        try:
            if browser is not None:
                browser.close()
                log("Browser closed")
        except Exception as e:
            log("Browser close failed", error=str(e))

        try:
            if playwright is not None:
                playwright.stop()
                log("Playwright stopped")
        except Exception as e:
            log("Playwright stop failed", error=str(e))

        clean_file(storage_path)

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
