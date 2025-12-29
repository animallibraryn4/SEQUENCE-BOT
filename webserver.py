import os
import threading
import subprocess
import sys
from flask import Flask

app = Flask(__name__)

@app.route("/")
def index():
    return "Bot is running with merging feature!"

def run_bot():
    """Run the bot as a separate process"""
    try:
        print("Starting bot...")
        subprocess.run([sys.executable, "bot.py"])
    except Exception as e:
        print(f"Bot error: {e}")

def run_server():
    """Run Flask server"""
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

if __name__ == "__main__":
    # Start bot in separate thread
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.daemon = True
    bot_thread.start()
    
    # Start web server
    run_server()
