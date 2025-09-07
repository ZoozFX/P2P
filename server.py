# server.py
import os
import threading
from flask import Flask
from logging import getLogger
logger = getLogger(__name__)

# استورد الدالة اللي بدأت البوت
from arbitrage_bot import start_worker

app = Flask(__name__)

@app.route("/health")
def health():
    return "OK", 200

# Start the bot in a background thread inside the worker process (once)
if os.getenv("DISABLE_BACKGROUND", "0") != "1":
    try:
        t = threading.Thread(target=start_worker, daemon=True)
        t.start()
        logger.info("Background bot thread started.")
    except Exception:
        logger.exception("Failed to start background thread")

if __name__ == "__main__":
    # for local debug only
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
