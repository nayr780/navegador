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

    browser = None
    try:
        body = request.get_json(force=True)
        storage_state = body.get("storage_state")
        proxy_config = body.get("proxy") # Recebe o proxy do cliente

        with sync_playwright() as p:
            # Busca o executável do Chromium
            cands = glob.glob("/opt/render/project/.cache/ms-playwright/chromium-*/chrome-linux/chrome")
            executable = cands[0] if cands else None
            
            log(f"Iniciando Browser. Proxy ativo: {bool(proxy_config)}")
            
            # Launch com Proxy Dinâmico
            browser = p.chromium.launch(
                executable_path=executable,
                headless=True,
                proxy=proxy_config, # Aqui a mágica acontece
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            
            context = browser.new_context(
                storage_state=storage_state,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )
            
            # Stealth básico
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page = context.new_page()
            log("Acessando Claude.ai...")
            
            # Aumentamos o timeout pois proxies podem ser lentos
            page.goto("https://claude.ai/", wait_until="domcontentloaded", timeout=90000)
            
            # Espera 5 segundos para o Cloudflare processar o IP do proxy
            time.sleep(5)
            
            page.screenshot(path=LAST_IMAGE_PATH, full_page=True)
            log("Snapshot tirado com sucesso.")

            res = {
                "ok": True, 
                "title": page.title(),
                "final_url": page.url,
                "download_url": f"{request.host_url.rstrip('/')}/download"
            }
            return jsonify(res)

    except Exception as e:
        log(f"Erro na execução: {str(e)}")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if browser:
            browser.close()
        _run_lock.release()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
