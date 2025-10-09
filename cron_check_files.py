#!/usr/bin/python3
import requests, os, time

FUNCTION_KEY = os.getenv("GET_FILES_KEY")
GET_FILES_URL = f"https://forward-pass-hweec7daafebhqfy.canadacentral-01.azurewebsites.net/api/getNewFiles?code={FUNCTION_KEY}"
UPLOADS_DIR = "/opt/command_centre/uploads"
FLASK_API = "http://127.0.0.1:5000/command_centre/api/run_preprocessing"

os.makedirs(UPLOADS_DIR, exist_ok=True)

def download_and_trigger():
    try:
        print("Checking for new files...")
        resp = requests.get(GET_FILES_URL, timeout=30)
        if resp.status_code != 200:
            print("Failed to reach Azure Function:", resp.status_code)
            return

        files = resp.json().get("files", [])
        if not files:
            print("No new files found.")
            return

        for f in files:
            name = f["filename"]
            url = f["url"]
            print(f"Downloading {name}...")
            print("Download URL:", url)
            dl = requests.get(url, timeout=60)
            if dl.status_code == 200:
                local_path = os.path.join(UPLOADS_DIR, name)
                with open(local_path, "wb") as fh:
                    fh.write(dl.content)
                print(f"Downloaded {name} successfully.")
            else:
                print(f"Failed to download {name}:", dl.status_code)

            # Trigger Flask preprocessing
            payload = {"filename": name}
            resp2 = requests.post(FLASK_API, json=payload)
            print(f"Triggered preprocessing for {name}:", resp2.status_code)

    except Exception as e:
        print("Error in cron_check_files:", e)

if __name__ == "__main__":
    download_and_trigger()



