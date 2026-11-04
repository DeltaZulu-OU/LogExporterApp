"""Disposable HTTP/blocklist and RFC 5424 UDP/TCP collector for the E2E suite."""

import json
import re
import socketserver
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

BLOCKLIST = Path(__file__).parent / "fixtures" / "blocklist.txt"
LOCK = threading.RLock()
STATE = {"http": [], "attempts": [], "syslog_udp": [], "syslog_tcp": [], "blocklist_hits": 0}
CONTROL = {"failure_count": 0, "status": 503}


def snapshot():
    with LOCK:
        return json.loads(json.dumps(STATE))


class WebHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return  # Keep event bodies and credentials out of CI console logs.

    def reply(self, code, data, content_type="application/json"):
        if isinstance(data, (dict, list)):
            data = json.dumps(data).encode()
        if isinstance(data, str):
            data = data.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/health":
            return self.reply(200, {"status": "ok"})
        if path == "/state":
            return self.reply(200, snapshot())
        if path == "/blocklist.txt":
            with LOCK:
                STATE["blocklist_hits"] += 1
            return self.reply(200, BLOCKLIST.read_bytes(), "text/plain")
        return self.reply(404, {"error": "unknown path"})

    def do_POST(self):
        path = urlsplit(self.path).path
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        if path == "/reset":
            with LOCK:
                for key in STATE:
                    STATE[key] = [] if isinstance(STATE[key], list) else 0
                CONTROL.update(failure_count=0, status=503)
            return self.reply(200, {"status": "ok"})
        if path == "/control":
            options = json.loads(body)
            count, status = int(options.get("failure_count", 0)), int(options.get("status", 503))
            if count < 0 or status < 400 or status > 599:
                return self.reply(400, {"error": "invalid failure policy"})
            with LOCK:
                CONTROL.update(failure_count=count, status=status)
            return self.reply(200, {"status": "ok"})
        if path.startswith("/events"):
            with LOCK:
                status = 200
                if CONTROL["failure_count"] > 0:
                    CONTROL["failure_count"] -= 1
                    status = CONTROL["status"]
                event = {"path": path, "body": body.decode("utf-8"),
                         "content_type": self.headers.get("Content-Type", "")}
                STATE["attempts"].append({**event, "status": status})
                if status == 200:
                    STATE["http"].append(event)
            return self.reply(status, {"status": "ok" if status == 200 else "error"})
        return self.reply(404, {"error": "unknown path"})


class UDPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data, _ = self.request
        with LOCK:
            STATE["syslog_udp"].append(data.decode("utf-8", errors="replace"))


class TCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        # Forwarder uses RFC 6587 octet-counting, not newline delimiting.
        while True:
            length = bytearray()
            while True:
                char = self.rfile.read(1)
                if not char:
                    return
                if char == b" ":
                    break
                if not re.fullmatch(rb"[0-9]", char) or len(length) >= 9:
                    return
                length.extend(char)
            if not length:
                return
            payload = self.rfile.read(int(length))
            if len(payload) != int(length):
                return
            with LOCK:
                STATE["syslog_tcp"].append(payload.decode("utf-8", errors="replace"))


class ThreadedUDP(socketserver.ThreadingUDPServer):
    allow_reuse_address = True
    daemon_threads = True


class ThreadedTCP(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    udp = ThreadedUDP(("0.0.0.0", 5514), UDPHandler)
    tcp = ThreadedTCP(("0.0.0.0", 5515), TCPHandler)
    threading.Thread(target=udp.serve_forever, daemon=True).start()
    threading.Thread(target=tcp.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), WebHandler).serve_forever()


if __name__ == "__main__":
    main()
