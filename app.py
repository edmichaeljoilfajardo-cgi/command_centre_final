from flask import Flask, request, jsonify
import base64
import os
import pandas as pd
import subprocess, threading, time

app = Flask(__name__)

# Directory
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

print("Files in uploads:", os.listdir(UPLOAD_DIR))

# Timer for delayed preprocessing execution
preprocess_timer = None
lock = threading.Lock()

def delayed_run():
    """Run the preprocessing script after cooldown expires"""
    global preprocess_timer
    time.sleep(60)
    with lock:
        preprocess_timer = None
    try:
        # venv Python path
        venv_python = r"C:\Users\edmichaeljoil.fajard\PythonVSCode\cc-dashboard\.venv\Scripts\python.exe"

        result = subprocess.run(
            [venv_python, "Command_Centre_Final_v1.py"],
            capture_output=True,
            text=True,
            cwd=r"C:\Users\edmichaeljoil.fajard\PythonVSCode\Command_Centre"  # script folder
        )

        print("Preprocessing script executed")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

    except Exception as e:
        print("Error running preprocessing script:", str(e))


@app.route("/api/command_centre", methods=["POST"])
def command_centre():
    try:
        # Get JSON payload
        data = request.get_json()
        filename = data.get("filename", "unknown.xlsx")
        content_base64 = data.get("content")

        if not content_base64:
            return jsonify({"status": "error", "message": "No file content received"}), 400

        # Decode Base64 Excel
        file_bytes = base64.b64decode(content_base64)
        file_path = os.path.join(UPLOAD_DIR, filename)

        # Save file local
        with open(file_path, "wb") as f:
            f.write(file_bytes)

        # Load Excel with pandas just for validation/preview
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


@app.route("/api/run_preprocessing", methods=["POST"])
def run_preprocessing():
    """Schedules the preprocessing script with a cooldown"""
    global preprocess_timer
    with lock:
        if preprocess_timer is not None:
            preprocess_timer.cancel()  # cancel previous timer
        preprocess_timer = threading.Timer(60, delayed_run)  # reset 1-min countdown
        preprocess_timer.start()
    return jsonify({
        "status": "scheduled",
        "message": "Preprocessing will run in 1 minutes if no new files arrive"
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
