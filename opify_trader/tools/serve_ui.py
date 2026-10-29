"""Serve ONLY tools/opify_ui.js on http://127.0.0.1:8765 with CORS for the Opify page.

Chrome treats 127.0.0.1 as a secure origin, so the https Opify page can fetch it. Inject with:
    eval(await (await fetch('http://127.0.0.1:8765/opify_ui.js')).text())

    python serve_ui.py          (run in background; Ctrl+C to stop)
"""
import http.server
import os

HERE = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(HERE, "opify_ui.js")


class H(http.server.BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "https://www.opify.in")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Cache-Control", "no-store")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.send_header("Access-Control-Allow-Methods", "GET")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_GET(self):
        if self.path.split("?")[0] != "/opify_ui.js":
            self.send_response(404)
            self.end_headers()
            return
        body = open(FILE, "rb").read()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "application/javascript; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    http.server.ThreadingHTTPServer(("127.0.0.1", 8765), H).serve_forever()
