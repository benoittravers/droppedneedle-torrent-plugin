"""Tests run against the real ``droppedneedle_plugin`` helper from a
DroppedNeedle v3 checkout (``DROPPEDNEEDLE_SOURCE``), with fake qBittorrent,
Prowlarr and Torznab services on a local port."""

import hashlib
import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
SDK = Path(os.environ["DROPPEDNEEDLE_SOURCE"]).resolve() / "sdk" / "python"
sys.path.insert(0, str(SDK))
sys.path.insert(0, str(ROOT))

QBT_KEY = "qbt-key-0123456789"
PROWLARR_KEY = "prowlarr-key-0123456789"
TORZNAB_KEY = "torznab-key-0123456789"
SAVE_PATH = "/data/torrents/droppedneedle"


class Services:
    """State behind the fake services; tests change it directly."""

    def __init__(self) -> None:
        self.torrents: list[dict] = []
        self.files: dict[str, list[dict]] = {}
        self.next_files: list[dict] = []
        self.adds: list[dict] = []
        self.deletes: list[dict] = []
        self.qbt_down = False
        self.add_answer = "Ok."
        self.add_status = 200
        self.add_registers = True  # False: accepted but never appears
        self.prowlarr_results: list[dict] = []
        self.torznab_body = b""
        self.base = ""

    def torrent(self, tag: str) -> dict:
        return next(t for t in self.torrents if tag in t["tags"].split(","))


class Handler(BaseHTTPRequestHandler):
    services: Services

    def log_message(self, *args):  # quiet
        pass

    def _send(self, status, body=b"", content_type="text/plain", headers=None):
        if isinstance(body, (dict, list)):
            body, content_type = json.dumps(body).encode(), "application/json"
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _form(self):
        length = int(self.headers.get("Content-Length") or 0)
        return {k: v[0] for k, v in parse_qs(self.rfile.read(length).decode()).items()}

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def _route(self, method):
        s = self.services
        url = urlsplit(self.path)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path.startswith("/api/v2/"):
            return self._qbt(method, url.path[len("/api/v2/") :], query)
        if url.path.startswith("/api/v1/"):
            if self.headers.get("X-Api-Key") != PROWLARR_KEY:
                return self._send(401)
            if url.path == "/api/v1/search":
                return self._send(200, s.prowlarr_results)
            if url.path == "/api/v1/system/status":
                return self._send(200, {"version": "2.0"})
        if url.path == "/torznab/api":
            if query.get("apikey") != TORZNAB_KEY:
                return self._send(200, b'<error code="100" description="bad key"/>', "text/xml")
            if query.get("t") == "caps":
                return self._send(200, b"<caps/>", "text/xml")
            return self._send(200, s.torznab_body, "application/rss+xml")
        if url.path == "/redirect/api":
            return self._send(302, headers={"Location": f"{s.base}/torznab/api?{url.query}"})
        return self._send(404)

    def _qbt(self, method, path, query):
        s = self.services
        if s.qbt_down:
            return self._send(503)
        if self.headers.get("Authorization") != f"Bearer {QBT_KEY}":
            return self._send(403)
        if path == "app/version":
            return self._send(200, "v5.2.1")
        if path == "torrents/add" and method == "POST":
            form = self._form()
            s.adds.append(form)
            if s.add_registers and s.add_answer.lower().startswith("ok"):
                digest = hashlib.sha1(form["urls"].encode()).hexdigest()
                s.torrents.append(
                    {
                        "hash": digest,
                        "state": "downloading",
                        "progress": 0.4,
                        "size": 1000,
                        "downloaded": 400,
                        "category": form.get("category", ""),
                        "tags": form.get("tags", ""),
                        "save_path": SAVE_PATH,
                        "content_path": f"{SAVE_PATH}/Album",
                    }
                )
                s.files[digest] = list(s.next_files)
            return self._send(s.add_status, s.add_answer)
        if path == "torrents/info":
            rows = [
                t
                for t in s.torrents
                if (not query.get("tag") or query["tag"] in t["tags"].split(","))
                and (not query.get("category") or t["category"] == query["category"])
            ]
            return self._send(200, rows)
        if path == "torrents/files":
            if query.get("hash") not in s.files:
                return self._send(404)
            return self._send(200, s.files[query["hash"]])
        if path == "torrents/delete" and method == "POST":
            form = self._form()
            s.deletes.append(form)
            s.torrents = [t for t in s.torrents if t["hash"] != form["hashes"]]
            return self._send(200)
        return self._send(404)


@pytest.fixture
def services():
    state = Services()
    handler = type("BoundHandler", (Handler,), {"services": state})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    state.base = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def folders(tmp_path):
    mount = tmp_path / "torrents"
    mount.mkdir()
    data = tmp_path / "data"
    data.mkdir()
    return SimpleNamespace(mount=mount, data=data, stage=tmp_path / "stage")


@pytest.fixture
def settings(services, folders):
    return {
        "qbittorrent_url": services.base,
        "qbittorrent_api_key": QBT_KEY,
        "downloads_path": str(folders.mount),
        "downloads_dir": str(folders.stage),
        "category": "droppedneedle",
        "search_backend": "prowlarr",
        "prowlarr_url": services.base,
        "prowlarr_api_key": PROWLARR_KEY,
        "torznab_url": f"{services.base}/torznab",
        "torznab_api_key": TORZNAB_KEY,
        "categories": "3000",
    }


@pytest.fixture
def make_plugin(settings, folders, monkeypatch):
    import droppedneedle_plugin

    import plugin

    monkeypatch.setattr(plugin, "_ADD_CORRELATE_DELAY", 0.01)
    monkeypatch.setattr(plugin, "_ADD_CORRELATE_ATTEMPTS", 5)
    monkeypatch.setattr(plugin, "_RETRY_BACKOFF", 0.0)

    def build(**overrides):
        ctx = SimpleNamespace(
            name="prowlarr-qbittorrent",
            settings={**settings, **overrides},
            data_dir=folders.data,
            plugin_dir=ROOT,
            logger=logging.getLogger("prowlarr-qbittorrent"),
            scoring=droppedneedle_plugin.ScoringHelper(),
        )
        return plugin.TorrentPlugin(ctx)

    return build
