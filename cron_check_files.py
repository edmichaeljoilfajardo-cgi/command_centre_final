#!/opt/command_centre/venv/bin/python
import requests, os, time, subprocess

# === CONFIGURATION ===
FUNCTION_KEY = os.getenv("GET_FILES_KEY")
GET_FILES_URL = f"https://forward-pass-hweec7daafebhqfy.canadacentral-01.azurewebsites.net/api/getNewFiles?code={FUNCTION_KEY}"
UPLOADS_DIR = "/opt/command_centre/uploads"
FLASK_API = "http://127.0.0.1:5000/command_centre/api/run_preprocessing"
PREPROCESS_SCRIPT = "/opt/command_centre/Command_Centre_Final_v1.py"

os.makedirs(UPLOADS_DIR, exist_ok=True)

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def download_files():
    try:
        log("Checking for new files from Azure Function...")
        resp = requests.get(GET_FILES_URL, timeout=60)
        if resp.status_code != 200:
            log(f"Failed to reach Azure Function: {resp.status_code}")
            return False

        files = resp.json().get("files", [])
        if not files:
            log("No files found in Azure container.")
            return False

        for f in files:
            name = f["filename"]
            url = f["url"]
            log(f"Downloading {name}...")
            dl = requests.get(url, timeout=60)
            if dl.status_code == 200:
                local_path = os.path.join(UPLOADS_DIR, name)
                with open(local_path, "wb") as fh:
                    fh.write(dl.content)
                log(f"Downloaded {name} successfully.")
            else:
                log(f"Failed to download {name}: {dl.status_code}")

        return True

    except Exception as e:
        log(f"Error in download_files: {e}")
        return False

def run_preprocessing():
    try:
        log("Starting preprocessing...")
        result = subprocess.run(
            ["/opt/command_centre/venv/bin/python", PREPROCESS_SCRIPT],

            capture_output=True,
            text=True,
            cwd="/opt/command_centre"
        )
        log("Preprocessing completed.")
        log("STDOUT:")
        log(result.stdout)
        log("STDERR:")
        log(result.stderr)
        return True
    except Exception as e:
        log(f"Error in run_preprocessing: {e}")
        return False

if __name__ == "__main__":
    log("===== Cron Job Started =====")
    if download_files():
        log("All files downloaded. Proceeding to preprocessing...")
        run_preprocessing()
    else:
        log("No files downloaded. Skipping preprocessing.")
    log("===== Cron Job Finished =====\n")

