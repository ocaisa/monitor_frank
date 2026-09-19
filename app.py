"""Flask frontend for the node monitor.

Run with the venv:
    venv/bin/python app.py
then open http://localhost:8080
"""

from __future__ import annotations

import os

from flask import Flask, jsonify, render_template

from collector import MONITOR

app = Flask(__name__, static_folder="images", static_url_path="/images")

# Begin background polling as soon as the app module is imported so it works
# under both `python app.py` and a WSGI/flask runner.
MONITOR.start()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    return jsonify(MONITOR.snapshot())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    host = os.environ.get("HOST", "0.0.0.0")
    app.run(host=host, port=port, debug=False)
