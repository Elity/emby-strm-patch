#!/usr/bin/env python3
"""A stand-in origin for probe-origin.sh, with a rate cap you choose the shape of.

probe-origin.sh exists to tell two origins apart: one that caps each connection, and one
that caps the pipe. That distinction is the whole product, and until this fixture existed the
only way to check the script got it right was to find a real throttled origin. Now:

  MODE=perconn RATE=4194304  python3 origin-sim.py 8099   each connection gets RATE B/s
  MODE=total   RATE=16777216 python3 origin-sim.py 8099   all connections share RATE B/s
  MODE=403                   python3 origin-sim.py 8099   every request is rejected
  MODE=norange               python3 origin-sim.py 8099   Range ignored, 200 + whole body

Bytes are synthetic, so there is no fixture file to keep around and SIZE can be any size.

One trap worth keeping in mind if you touch the limiter: the bucket must start EMPTY and its
burst must be capped well under one second of traffic. A bucket that starts full lets any
request smaller than the burst through at line rate, which silently turns the cap off - the
"per-connection" and "shared" shapes then look identical and the fixture proves nothing.
"""
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIZE = int(os.environ.get("SIZE", 512 * 1024 * 1024))
RATE = float(os.environ.get("RATE", 4 * 1024 * 1024))
MODE = os.environ.get("MODE", "perconn")
BLOCK = 64 * 1024
PAYLOAD = bytes(BLOCK)


class Bucket:
    """Token bucket. Starts empty, holds at most BURST seconds of tokens."""

    BURST = 0.05

    def __init__(self, rate):
        self.rate = rate
        self.cap = rate * self.BURST
        self.tokens = 0.0
        self.t = time.monotonic()
        self.lock = threading.Lock()

    def take(self, n):
        while n > 0:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.cap, self.tokens + (now - self.t) * self.rate)
                self.t = now
                got = min(n, self.tokens)
                self.tokens -= got
                n -= got
            if n > 0:
                time.sleep(0.005)


SHARED = Bucket(RATE)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _empty(self, code):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        # Readiness probe, answered identically in every mode - the test harness needs a
        # cheap, deterministic "are you listening yet", and in norange mode any real request
        # streams the entire body.
        if self.path == "/ping":
            return self._empty(204)

        if MODE == "403":
            return self._empty(403)

        rng = self.headers.get("Range", "")
        if MODE == "norange" or not rng.startswith("bytes="):
            if MODE != "norange":
                return self._empty(400)
            start, end = 0, SIZE - 1
            code = 200
        else:
            lo, _, hi = rng[len("bytes="):].partition("-")
            start = int(lo)
            end = min(int(hi), SIZE - 1) if hi else SIZE - 1
            if start > end or start >= SIZE:
                return self._empty(416)
            code = 206

        length = end - start + 1
        self.send_response(code)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        if code == 206:
            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, end, SIZE))
        self.end_headers()

        bucket = SHARED if MODE == "total" else Bucket(RATE)
        left = length
        try:
            while left > 0:
                chunk = min(BLOCK, left)
                bucket.take(chunk)
                self.wfile.write(PAYLOAD[:chunk])
                left -= chunk
        except (BrokenPipeError, ConnectionResetError):
            pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
