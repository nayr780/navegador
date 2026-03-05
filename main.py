import os
import time
import base64
import threading
from flask import Flask, request, jsonify, send_file
from playwright.sync_api import sync_playwright

app = Flask(__name__)
API_KEY = os.getenv("API_KEY", "sua_key_super_secreta")
LAST_IMAGE_PATH = "/tmp/claude_snapshot.png"
_run_lock = threading.Lock()

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# Rota para baixar a última foto gerada
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
        storage_state = body.get("storage_state") # O JSON dos cookies convertido

        with sync_playwright() as p:
            # Tenta achar o Chromium no cache do Render
            executable = "/opt/render/project/.cache/ms-playwright/chromium-1105/chrome-linux/chrome"
            if not os.path.exists(executable):
                # Fallback caso a versão mude
                import glob
                cands = glob.glob("/opt/render/project/.cache/ms-playwright/chromium-*/chrome-linux/chrome")
                executable = cands[0] if cands else None

            log(f"Usando browser em: {executable}")
            
            browser = p.chromium.launch(
                executable_path=executable,
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            
            # Cria contexto com cookies se houver
            context = browser.new_context(
                storage_state=storage_state,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            
            # Script anti-detecção
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page = context.new_page()
            log("Navegando para Claude...")
            
            page.goto("https://claude.ai/", wait_until="networkidle", timeout=60000)
            
            # Tira a foto e salva no caminho de download
            page.screenshot(path=LAST_IMAGE_PATH, full_page=True)
            log("Foto salva com sucesso!")

            res = {
                "ok": True, 
                "title": page.title(),
                "download_url": f"{request.host_url}download"
            }
            browser.close()
            return jsonify(res)

    except Exception as e:
        log(f"Erro: {str(e)}")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _run_lock.release()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
