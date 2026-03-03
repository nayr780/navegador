import os
import sys
import json
import time
import glob
import shutil
import base64
import traceback
import threading
from typing import Any, Dict, List, Optional

from flask import Flask, request, jsonify
from playwright.sync_api import sync_playwright


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


def normalize_env(value: Any) -> Optional[Dict[str, str]]:
    if not isinstance(value, dict):
        return None
    out: Dict[str, str] = {}
    for k, v in value.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out or None


def jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def is_executable_file(path: str) -> bool:
    try:
        return isinstance(path, str) and os.path.isfile(path) and os.access(path, os.X_OK)
    except Exception:
        return False


# ============================================================
# BROWSER DISCOVERY
# ============================================================

def _append_candidate(candidates: List[Dict[str, str]], seen: set, path: Optional[str], source: str) -> None:
    if not isinstance(path, str) or not path.strip():
        return
    path = os.path.abspath(path.strip())
    if path in seen:
        return
    if is_executable_file(path):
        seen.add(path)
        candidates.append({"path": path, "source": source})


def discover_chromium_candidates(playwright=None) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []
    seen = set()

    # 1) Caminho que o próprio Playwright conhece, se existir no filesystem
    if playwright is not None:
        try:
            pw_path = playwright.chromium.executable_path
            _append_candidate(candidates, seen, pw_path, "playwright.chromium.executable_path")
        except Exception:
            pass

    # 2) PATH do sistema
    bin_names = [
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
        "google-chrome-beta",
        "google-chrome-unstable",
        "chrome",
    ]
    for name in bin_names:
        _append_candidate(candidates, seen, shutil.which(name), f"which:{name}")

    # 3) Caminhos comuns Linux/Render
    common_paths = [
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/opt/google/chrome/chrome",
    ]
    for p in common_paths:
        _append_candidate(candidates, seen, p, "common-path")

    # 4) Estruturas comuns do Playwright já baixado
    glob_patterns = [
        f"{BROWSERS_PATH}/chromium-*/chrome-linux/chrome",
        "/opt/render/.cache/ms-playwright/chromium-*/chrome-linux/chrome",
        "/opt/render/project/.cache/ms-playwright/chromium-*/chrome-linux/chrome",
        "/root/.cache/ms-playwright/chromium-*/chrome-linux/chrome",
    ]
    for pattern in glob_patterns:
        for match in sorted(glob.glob(pattern)):
            _append_candidate(candidates, seen, match, f"glob:{pattern}")

    return candidates


def resolve_chromium_path(playwright, explicit_path: Optional[str]) -> Dict[str, Any]:
    checked: List[str] = []

    if isinstance(explicit_path, str) and explicit_path.strip():
        explicit_path = os.path.abspath(explicit_path.strip())
        checked.append(explicit_path)
        if is_executable_file(explicit_path):
            return {
                "ok": True,
                "path": explicit_path,
                "source": "request.executable_path",
                "checked": checked,
                "candidates": [{"path": explicit_path, "source": "request.executable_path"}],
            }
        return {
            "ok": False,
            "error": "explicit_executable_path_not_found_or_not_executable",
            "checked": checked,
            "candidates": [],
        }

    candidates = discover_chromium_candidates(playwright)
    checked.extend([c["path"] for c in candidates])

    if candidates:
        chosen = candidates[0]
        return {
            "ok": True,
            "path": chosen["path"],
            "source": chosen["source"],
            "checked": checked,
            "candidates": candidates,
        }

    return {
        "ok": False,
        "error": "chromium_not_found",
        "checked": checked,
        "candidates": [],
    }


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
    return jsonify({"ok": True, "service": "single-shot-browser-flex-no-install"})


@APP.get("/health")
def health():
    candidates = discover_chromium_candidates()
    return jsonify({
        "ok": True,
        "in_use": _in_use,
        "python": sys.version,
        "pid": os.getpid(),
        "browsers_path": BROWSERS_PATH,
        "chromium_found": bool(candidates),
        "chromium_candidates": candidates,
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

        # opções mais flexíveis de launch
        explicit_executable_path = body.get("executable_path")
        headless = bool(body.get("headless", True))
        channel = body.get("channel")
        ignore_default_args = body.get("ignore_default_args")
        chromium_sandbox = body.get("chromium_sandbox")
        proxy = body.get("proxy")
        slow_mo = body.get("slow_mo")
        launch_env = normalize_env(body.get("env"))
        launch_options = body.get("launch_options") if isinstance(body.get("launch_options"), dict) else {}
        context_options = body.get("context_options") if isinstance(body.get("context_options"), dict) else {}

        if isinstance(storage_state_b64, str) and storage_state_b64.strip():
            storage_path = f"/tmp/state_{int(time.time() * 1000)}.json"
            b64_to_file(storage_state_b64, storage_path)
            log("Storage state saved", path=storage_path)

        playwright = sync_playwright().start()
        log("Playwright started")

        resolved = resolve_chromium_path(playwright, explicit_executable_path)
        if not resolved.get("ok"):
            log("Chromium not found", checked=resolved.get("checked", []))
            return jsonify({
                "ok": False,
                "error": resolved.get("error", "chromium_not_found"),
                "checked": resolved.get("checked", []),
                "chromium_candidates": resolved.get("candidates", []),
            }), 500

        executable_path = resolved["path"]
        log("Chromium resolved", path=executable_path, source=resolved.get("source"))

        launch_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ]
        launch_args.extend(browser_args)

        launch_kwargs: Dict[str, Any] = {
            "headless": headless,
            "args": launch_args,
            "executable_path": executable_path,
        }

        if isinstance(channel, str) and channel.strip():
            # só usa channel se você quiser forçar; com executable_path explícito geralmente não precisa
            launch_kwargs["channel"] = channel.strip()

        if isinstance(ignore_default_args, bool) or isinstance(ignore_default_args, list):
            launch_kwargs["ignore_default_args"] = ignore_default_args

        if isinstance(chromium_sandbox, bool):
            launch_kwargs["chromium_sandbox"] = chromium_sandbox

        if isinstance(proxy, dict):
            launch_kwargs["proxy"] = proxy

        if slow_mo is not None:
            launch_kwargs["slow_mo"] = clamp_int(slow_mo, 0, 0, 5000)

        if launch_env:
            merged_env = os.environ.copy()
            merged_env.update(launch_env)
            launch_kwargs["env"] = merged_env

        # merge final de launch_options, mas sem deixar sobrescrever executable_path sem querer
        for k, v in launch_options.items():
            if k == "executable_path":
                continue
            launch_kwargs[k] = v

        browser = playwright.chromium.launch(**launch_kwargs)
        log("Browser launched", executable_path=executable_path, args=launch_args)

        merged_context_options: Dict[str, Any] = {}
        merged_context_options.update(context_options)
        merged_context_options["viewport"] = viewport
        merged_context_options["ignore_https_errors"] = ignore_https_errors

        if storage_path:
            merged_context_options["storage_state"] = storage_path
        if isinstance(user_agent, str) and user_agent.strip():
            merged_context_options["user_agent"] = user_agent.strip()
        if isinstance(locale, str) and locale.strip():
            merged_context_options["locale"] = locale.strip()
        if isinstance(timezone_id, str) and timezone_id.strip():
            merged_context_options["timezone_id"] = timezone_id.strip()
        if extra_http_headers:
            merged_context_options["extra_http_headers"] = extra_http_headers

        context = browser.new_context(**merged_context_options)
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
            "chromium_path": executable_path,
            "chromium_source": resolved.get("source"),
            "chromium_candidates": resolved.get("candidates", []),
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

_boot_candidates = discover_chromium_candidates()
if _boot_candidates:
    log("Chromium candidates found at boot", count=len(_boot_candidates), first=_boot_candidates[0])
else:
    log("Chromium not found at boot")

log("Ready.")
