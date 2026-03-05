import os
import time
import threading
import glob
from flask import Flask, request, jsonify, send_file
from playwright.sync_api import sync_playwright

app = Flask(__name__)
API_KEY = os.getenv("API_KEY", "sua_key_super_secreta")
LAST_IMAGE_PATH = "/tmp/claude_snapshot.png"
_run_lock = threading.Lock()

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

@app.route("/download")
def download():
    if os.path.exists(LAST_IMAGE_PATH):
        return send_file(LAST_IMAGE_PATH, mimetype='image/png', as_attachment=True)
    return "Nenhuma imagem gerada ainda.", 404

@app.route("/run", methods=["POST"])
def run_browser():
    if request.headers.get("x-api-key") != API_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    if not _run_lock.acquire(blocking=False):
        return jsonify({"ok": False, "error": "busy"}), 409

    try:
        body = request.get_json(force=True)
        storage_state = body.get("storage_state")
        proxy_config = body.get("proxy")

        with sync_playwright() as p:
            cands = glob.glob("/opt/render/project/.cache/ms-playwright/chromium-*/chrome-linux/chrome")
            executable = cands[0] if cands else None
            
            log(f"Iniciando Browser. Proxy: {proxy_config.get('server') if proxy_config else 'Nenhum'}")
            
            browser = p.chromium.launch(
                executable_path=executable,
                headless=True,
                proxy=proxy_config,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            
            # Aumentamos o timeout global do contexto para 60s
            context = browser.new_context(
                storage_state=storage_state,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            context.set_default_timeout(60000) 
            
            page = context.new_page()
            log("Acessando Claude.ai...")
            
            # Tentativa de navegação com timeout estendido
            try:
                page.goto("https://claude.ai/", wait_until="domcontentloaded", timeout=60000)
                time.sleep(7) # Espera o Cloudflare/Proxy estabilizar
                
                page.screenshot(path=LAST_IMAGE_PATH, full_page=False)
                log("Snapshot tirado.")
                
                res = {
                    "ok": True, 
                    "title": page.title(),
                    "download_url": f"{request.host_url.rstrip('/')}/download"
                }
            except Exception as e:
                log(f"Erro durante navegação: {str(e)}")
                res = {"ok": False, "error": f"Navegação falhou: {str(e)}"}
            
            browser.close()
            return jsonify(res)

    except Exception as e:
        log(f"Erro geral: {str(e)}")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if _run_lock.locked():
            _run_lock.release()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
