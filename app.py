from flask import Flask, request, jsonify, Blueprint
import base64
import os
import pandas as pd
import subprocess, threading, time

app = Flask(__name__)

# URL prefix for Apache reverse proxy
api_bp = Blueprint('api', __name__)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

print("Files in uploads:", os.listdir(UPLOAD_DIR))

preprocess_timer = None
lock = threading.Lock()

def delayed_run():
    """Run the preprocessing script after cooldown expires"""
    global preprocess_timer
    time.sleep(60)
    with lock:
        preprocess_timer = None
    try:
        venv_python = "/usr/bin/python3"  # adjust if using virtualenv
        result = subprocess.run(
            [venv_python, "Command_Centre_Final_v1.py"],
            capture_output=True,
            text=True,
            cwd="/opt/command_centre_final"
        )
        print("Preprocessing script executed")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
    except Exception as e:
        print("Error running preprocessing script:", str(e))

@api_bp.route("/api/command_centre", methods=["POST"])
def command_centre():
    try:
        data = request.get_json()
        filename = data.get("filename", "unknown.xlsx")
        content_base64 = data.get("content")

        if not content_base64:
            return jsonify({"status": "error", "message": "No file content received"}), 400

        file_bytes = base64.b64decode(content_base64)
        file_path = os.path.join(UPLOAD_DIR, filename)

        with open(file_path, "wb") as f:
            f.write(file_bytes)

        df = pd.read_excel(file_path)
        rows, cols = df.shape
        preview = df.head(3).astype(str).to_dict(orient="records")

        return jsonify({
            "status": "success",
            "api": "command_centre",
            "filename": filename,
            "rows": rows,
            "cols": cols,
            "preview": preview
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@api_bp.route("/api/run_preprocessing", methods=["POST"])
def run_preprocessing():
    global preprocess_timer
    with lock:
        if preprocess_timer is not None:
            preprocess_timer.cancel()
        preprocess_timer = threading.Timer(60, delayed_run)
        preprocess_timer.start()
    return jsonify({
        "status": "scheduled",
        "message": "Preprocessing will run in 1 minute if no new files arrive"
    }), 200

# Register blueprint
app.register_blueprint(api_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


