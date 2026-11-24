# SPDX-License-Identifier: AGPL-3.0-only
"""Prowlarr or Torznab search with qBittorrent downloads, for DroppedNeedle v3.

Plugin API 2 (DroppedNeedle v3): the plugin runs as its own process through
the ``droppedneedle_plugin`` helper and uses only the standard library.

A finished torrent keeps seeding. Its audio files are copied into a staging
folder (``downloads_dir``, or ``staging/`` in the plugin's data folder), and
only those copies are reported to DroppedNeedle, which imports them and then
removes them. The torrent's own files are never reported, so nothing the host
does after an import can touch them.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import errno
import hashlib
import http.client
import json
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlencode, urljoin, urlsplit
from xml.etree import ElementTree

from droppedneedle_plugin import (
    DownloadFailure,
    DownloadMaterialization,
    DownloadTaskStatus,
    EnqueueRequest,
    PluginSearchResult,
    ServiceStatus,
    TaskHandle,
)

NAME = "prowlarr-qbittorrent"
SOURCE = f"plugin:{NAME}"
USER_AGENT = f"DroppedNeedle-Plugin/{NAME}"

# Indexer API keys embedded in grab links are replaced by this marker in the
# search payload (search results reach browsers) and restored at enqueue.
KEY_PLACEHOLDER = "DROPPEDNEEDLE_INDEXER_KEY"

# DroppedNeedle hands out job names like ``droppedneedle-<task id>-<index>``.
# They become qBittorrent tags, so commas (the tag separator) are refused.
_TAG = re.compile(r"droppedneedle-[A-Za-z0-9._:-]{1,200}")

_MAX_JSON_BYTES = 16 * 1024 * 1024
_MAX_TORZNAB_BYTES = 8 * 1024 * 1024
_MAX_RESULTS = 100
_MAX_FILES = 10_000

_AUDIO_SUFFIXES = {".flac", ".mp3", ".m4a", ".m4b", ".mp4", ".ogg", ".oga", ".opus", ".wav"}

_FAILED_STATES = {"error", "missingfiles"}
_ACTIVE_STATES = {"downloading", "forceddl"}
_CHECKING_STATES = {"checkingdl", "checkingup", "checkingresumedata", "moving"}
_SEEDING_STATES = {"uploading", "stalledup", "queuedup", "pausedup", "stoppedup", "forcedup"}

# A just-added torrent can take a moment to appear (qBittorrent fetches a
# .torrent link itself). Enqueue has 120 s; this waits up to about 60.
_ADD_CORRELATE_ATTEMPTS = 30
_ADD_CORRELATE_DELAY = 2.0
_RETRY_BACKOFF = 0.5


class ConfigError(ValueError):
    """A setting is missing or invalid."""


# --------------------------------------------------------------------------
# HTTP: standard library, no redirects (an Authorization header must never
# follow a redirect to another host), bounded response size.
# --------------------------------------------------------------------------


class TransportError(Exception):
    """The service could not be reached. Never carries the URL, which can
    hold an API key."""


@dataclasses.dataclass
class Reply:
    status: int
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", "replace")

    def json(self) -> Any:
        return json.loads(self.body or b"null")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def _fetch_sync(method, url, params, headers, form, timeout, max_bytes) -> Reply:
    if params:
        url += ("&" if urlsplit(url).query else "?") + urlencode(params, doseq=True)
    sent = {"User-Agent": USER_AGENT, **(headers or {})}
    body = None
    if form is not None:
        body = urlencode(form).encode()
        sent["Content-Type"] = "application/x-www-form-urlencoded"
    request = urllib.request.Request(url, data=body, headers=sent, method=method)
    try:
        response = _OPENER.open(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        if getattr(error, "fp", None) is None:
            return Reply(error.code, b"")
        response = error
    except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
        raise TransportError(_reason(exc)) from None
    try:
        with response:
            data = response.read(max_bytes + 1)
    except (http.client.HTTPException, OSError) as exc:
        raise TransportError(_reason(exc)) from None
    if len(data) > max_bytes:
        raise TransportError(f"response larger than {max_bytes // (1024 * 1024)} MiB")
    return Reply(response.status, data)


def _reason(exc: Exception) -> str:
    reason = getattr(exc, "reason", None)
    if isinstance(reason, Exception):
        exc = reason
    if isinstance(exc, TimeoutError):
        return "timed out"
    if isinstance(exc, ConnectionRefusedError):
        return "connection refused"
    return type(exc).__name__


async def fetch(
    method: str,
    url: str,
    *,
    params=None,
    headers=None,
    form=None,
    timeout: float = 30.0,
    max_bytes: int = _MAX_JSON_BYTES,
) -> Reply:
    return await asyncio.to_thread(
        _fetch_sync, method, url, params, headers, form, timeout, max_bytes
    )


# --------------------------------------------------------------------------
# qBittorrent (5.2+, Web API bearer key)
# --------------------------------------------------------------------------


class QbtError(Exception):
    def __init__(self, message: str, *, auth: bool = False, outage: bool = False) -> None:
        super().__init__(message)
        self.auth = auth
        self.outage = outage


@dataclasses.dataclass
class Torrent:
    hash: str = ""
    state: str = ""
    progress: float = 0.0
    size: int = 0
    downloaded: int = 0
    category: str = ""
    tags: str = ""
    save_path: str = ""

    @classmethod
    def of(cls, row: dict) -> Torrent:
        def number(key, kind):
            try:
                return kind(row.get(key) or 0)
            except (TypeError, ValueError):
                return kind(0)

        return cls(
            hash=str(row.get("hash") or ""),
            state=str(row.get("state") or "").lower(),
            progress=number("progress", float),
            size=number("size", int),
            downloaded=number("downloaded", int),
            category=str(row.get("category") or ""),
            tags=str(row.get("tags") or ""),
            save_path=str(row.get("save_path") or ""),
        )

    @property
    def complete(self) -> bool:
        return (
            self.progress >= 1.0 or self.state in _SEEDING_STATES
        ) and self.state not in _CHECKING_STATES | _FAILED_STATES


class Qbittorrent:
    def __init__(self, base_url: str, api_key: str) -> None:
        self._base = base_url.rstrip("/")
        self._key = api_key

    async def _call(self, method, path, *, params=None, form=None, timeout=30.0, retry=True):
        attempts = 3 if retry else 1
        failure = QbtError("qBittorrent request failed", outage=True)
        for attempt in range(attempts):
            if attempt:
                await asyncio.sleep(_RETRY_BACKOFF * 2 ** (attempt - 1))
            try:
                reply = await fetch(
                    method,
                    f"{self._base}/api/v2/{path}",
                    params=params,
                    form=form,
                    headers={"Authorization": f"Bearer {self._key}"},
                    timeout=timeout,
                )
            except TransportError as exc:
                failure = QbtError(f"qBittorrent is unreachable: {exc}", outage=True)
                continue
            if reply.status in (401, 403):
                raise QbtError("qBittorrent rejected the API key", auth=True)
            if reply.status >= 500:
                failure = QbtError(f"qBittorrent returned HTTP {reply.status}", outage=True)
                continue
            return reply
        raise failure

    async def version(self, *, timeout: float = 4.0) -> str:
        reply = await self._call("GET", "app/version", timeout=timeout, retry=False)
        if reply.status >= 400:
            raise QbtError(f"qBittorrent returned HTTP {reply.status}")
        return reply.text.strip()

    async def add(self, url: str, *, category: str, tag: str) -> None:
        """Not retried: a re-send can race the first add. The endpoint does
        not name the torrent, so callers correlate by ``tag``."""
        reply = await self._call(
            "POST",
            "torrents/add",
            form={"urls": url, "category": category, "tags": tag},
            timeout=60.0,
            retry=False,
        )
        if reply.status == 415 or reply.text.strip().lower().startswith("fails"):
            raise QbtError("qBittorrent rejected the torrent")
        if reply.status >= 400:
            raise QbtError(f"qBittorrent add returned HTTP {reply.status}")

    async def torrents(self, *, tag: str, category: str) -> list[Torrent]:
        reply = await self._call("GET", "torrents/info", params={"tag": tag, "category": category})
        if reply.status >= 400:
            raise QbtError(f"qBittorrent info returned HTTP {reply.status}")
        try:
            rows = reply.json()
        except ValueError:
            raise QbtError("qBittorrent returned something that is not JSON") from None
        return (
            [Torrent.of(row) for row in rows if isinstance(row, dict)]
            if isinstance(rows, list)
            else []
        )

    async def files(self, torrent_hash: str) -> list[dict]:
        reply = await self._call("GET", "torrents/files", params={"hash": torrent_hash})
        if reply.status == 404:
            return []
        if reply.status >= 400:
            raise QbtError(f"qBittorrent files returned HTTP {reply.status}")
        try:
            rows = reply.json()
        except ValueError:
            raise QbtError("qBittorrent returned something that is not JSON") from None
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    async def delete(self, torrent_hash: str) -> bool:
        reply = await self._call(
            "POST",
            "torrents/delete",
            form={"hashes": torrent_hash, "deleteFiles": "true"},
            retry=False,
        )
        return reply.status < 400


def supports_api_key(version: str) -> bool:
    """qBittorrent added bearer API-key authentication in 5.2.0."""
    match = re.search(r"(\d+)\.(\d+)", version or "")
    return bool(match and (int(match.group(1)), int(match.group(2))) >= (5, 2))


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Config:
    qbt_url: str
    qbt_key: str
    mount: Path
    stage: Path
    category: str
    backend: str
    index_url: str
    index_key: str
    categories: tuple[int, ...]


_LABELS = {
    "qbittorrent_url": "qBittorrent URL",
    "qbittorrent_api_key": "qBittorrent API key",
    "downloads_path": "Torrent downloads folder",
    "prowlarr_url": "Prowlarr URL",
    "prowlarr_api_key": "Prowlarr API key",
    "torznab_url": "Torznab URL",
    "torznab_api_key": "Torznab API key",
}


def http_url(value: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.netloc
        or parsed.username
        or parsed.password
    ):
        raise ConfigError("Expected an http(s) address without a user name or password in it")


def origin(url: str) -> tuple:
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError:
        port = None
    default = {"http": 80, "https": 443}.get(parsed.scheme)
    return parsed.scheme, (parsed.hostname or "").lower(), port or default


def _looks_sealed(value: str) -> bool:
    """A Fernet token: what the old fork's migration helper stored. v3
    cannot open it for the plugin, so the admin has to type the key again."""
    return value.startswith("gAAAA") and len(value) >= 100 and bool(re.fullmatch(r"[\w=-]+", value))


def _folder(value: str, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ConfigError(f"{label} must be an absolute path without '..'")
    return path


def build_config(settings: dict, data_dir: Path) -> Config:
    def text(key: str) -> str:
        return str(settings.get(key) or "").strip()

    def required(key: str) -> str:
        value = text(key)
        if not value:
            raise ConfigError(f"Set the {_LABELS[key]}")
        if key.endswith("_api_key") and _looks_sealed(value):
            raise ConfigError(
                f"The {_LABELS[key]} is still encrypted from the old fork; type the key again"
            )
        return value

    qbt_url = required("qbittorrent_url").rstrip("/")
    http_url(qbt_url)
    qbt_key = required("qbittorrent_api_key")
    mount = _folder(required("downloads_path"), "The torrent downloads folder")
    stage_text = text("downloads_dir")
    stage = (
        _folder(stage_text, "The staging folder")
        if stage_text
        else Path(data_dir).absolute() / "staging"
    )
    real_mount, real_stage = mount.resolve(), stage.resolve()
    if real_stage.is_relative_to(real_mount) or real_mount.is_relative_to(real_stage):
        raise ConfigError("The staging folder and the torrent downloads folder must not overlap")
    backend = text("search_backend").lower() or "prowlarr"
    if backend not in ("prowlarr", "torznab"):
        raise ConfigError("Search backend must be prowlarr or torznab")
    index_url = required(f"{backend}_url").rstrip("/")
    http_url(index_url)
    index_key = required(f"{backend}_api_key")
    raw = text("categories") or "3000"
    try:
        categories = tuple(int(part.strip()) for part in raw.split(","))
    except ValueError:
        raise ConfigError("Audio category IDs must be comma-separated numbers") from None
    if any(value <= 0 for value in categories):
        raise ConfigError("Audio category IDs must be positive numbers")
    if not re.fullmatch(r"[^\\/,]{1,100}", text("category") or "droppedneedle"):
        raise ConfigError("The qBittorrent category may not contain slashes or commas")
    return Config(
        qbt_url=qbt_url,
        qbt_key=qbt_key,
        mount=mount,
        stage=stage,
        category=text("category") or "droppedneedle",
        backend=backend,
        index_url=index_url,
        index_key=index_key,
        categories=categories,
    )


# --------------------------------------------------------------------------
# Search
# --------------------------------------------------------------------------


def quality_tier(title: str, categories: list[int]) -> str:
    if re.search(r"\b(flac|alac|lossless)\b", title, re.IGNORECASE) or 3040 in categories:
        return "lossless"
    if 3020 in categories:
        return ""
    for bitrate in (320, 256, 192):
        if re.search(rf"\b{bitrate}\b", title):
            return f"mp3_{bitrate}"
    return ""


def _number(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


async def prowlarr_search(cfg: Config, query: str, timeout: float) -> list[dict]:
    params = [("query", query), ("type", "search"), ("limit", str(_MAX_RESULTS))]
    params += [("categories", str(c)) for c in cfg.categories]
    reply = await _prowlarr_get(cfg, "search", params, timeout)
    rows = []
    for hit in reply if isinstance(reply, list) else []:
        if not isinstance(hit, dict) or str(hit.get("protocol", "")).lower() != "torrent":
            continue
        link = str(hit.get("downloadUrl") or "")
        rows.append(
            {
                "title": str(hit.get("title") or ""),
                "size": _number(hit.get("size"), 0),
                "seeders": _number(hit.get("seeders")),
                "download_url": urljoin(f"{cfg.index_url}/", link) if link else "",
                "magnet_url": str(hit.get("magnetUrl") or ""),
                "info_hash": str(hit.get("infoHash") or ""),
                "categories": [
                    _number(c.get("id"), 0)
                    for c in hit.get("categories") or []
                    if isinstance(c, dict)
                ],
            }
        )
    return rows


async def _prowlarr_get(cfg: Config, path: str, params, timeout: float) -> Any:
    try:
        reply = await fetch(
            "GET",
            f"{cfg.index_url}/api/v1/{path}",
            params=params,
            headers={"X-Api-Key": cfg.index_key},
            timeout=timeout,
        )
    except TransportError as exc:
        raise RuntimeError(f"Prowlarr is unreachable: {exc}") from None
    if reply.status in (401, 403):
        raise RuntimeError("Prowlarr rejected the API key")
    if reply.status >= 400:
        raise RuntimeError(f"Prowlarr returned HTTP {reply.status}")
    try:
        return reply.json()
    except ValueError:
        raise RuntimeError("Prowlarr returned something that is not JSON") from None


def torznab_api_url(cfg: Config) -> str:
    return cfg.index_url if cfg.index_url.endswith("/api") else cfg.index_url + "/api"


async def torznab_search(cfg: Config, query: str, timeout: float, *, caps: bool = False):
    url = torznab_api_url(cfg)
    params = {"apikey": cfg.index_key, "t": "caps" if caps else "search"}
    if not caps:
        params.update(
            q=query,
            cat=",".join(map(str, cfg.categories)),
            extended="1",
            limit=str(_MAX_RESULTS),
        )
    try:
        reply = await fetch(
            "GET", url, params=params, timeout=timeout, max_bytes=_MAX_TORZNAB_BYTES
        )
    except TransportError as exc:
        raise RuntimeError(f"Torznab feed is unreachable: {exc}") from None
    if reply.status != 200:
        raise RuntimeError(f"Torznab feed returned HTTP {reply.status}")
    return parse_torznab(reply.body, url, caps=caps)


def parse_torznab(raw: bytes, base_url: str, *, caps: bool = False) -> list[dict]:
    # Only UTF-8, and no DTDs or entities (entity expansion attacks).
    xml = raw.decode("utf-8-sig")
    if "\x00" in xml or re.search(r"<!\s*(DOCTYPE|ENTITY)", xml, re.IGNORECASE):
        raise ValueError("Torznab XML contains a forbidden declaration")
    root = ElementTree.fromstring(xml)
    if root.tag.rsplit("}", 1)[-1] == "error":
        raise RuntimeError("Torznab feed rejected the request")
    if caps:
        return []
    rows = []
    for item in root.iter("item"):
        if len(rows) >= _MAX_RESULTS:
            break
        attrs: dict[str, list[str]] = {}
        for child in item:
            if child.tag.rsplit("}", 1)[-1] == "attr":
                attrs.setdefault(child.get("name", "").lower(), []).append(child.get("value", ""))

        def first(name, attrs=attrs):
            return attrs.get(name, [""])[0]

        enclosure = item.find("enclosure")
        link = (enclosure.get("url", "") if enclosure is not None else "") or (
            item.findtext("link") or ""
        ).strip()
        magnet = first("magneturl") or (link if link.startswith("magnet:") else "")
        size = first("size") or (enclosure.get("length", "") if enclosure is not None else "")
        rows.append(
            {
                "title": (item.findtext("title") or "").strip(),
                "size": _number(size, 0),
                "seeders": _number(first("seeders")),
                "magnet_url": magnet,
                "download_url": urljoin(base_url, link)
                if link and not link.startswith("magnet:")
                else "",
                "info_hash": first("infohash"),
                "categories": [_number(c, 0) for c in attrs.get("category", [])],
            }
        )
    return rows


# --------------------------------------------------------------------------
# Staging
# --------------------------------------------------------------------------

_MANIFEST = ".ready.json"


def read_manifest(job: Path) -> list[Path] | None:
    """The staged copies of a finished job, or None while it is not staged."""
    if job.is_symlink():
        raise ValueError("A staging job folder cannot be a link")
    marker = job / _MANIFEST
    if not marker.is_file():
        return None
    names = json.loads(marker.read_text())
    if not isinstance(names, list):
        raise TypeError("Invalid staging manifest")
    found = []
    for name in names:
        rel = PurePosixPath(str(name))
        if rel.is_absolute() or ".." in rel.parts or not rel.parts:
            raise ValueError("Invalid staging manifest path")
        path = job.joinpath(*rel.parts)
        if path.is_file() and not path.is_symlink():
            found.append(path)
    return found


def copy_file(source, output) -> None:
    """Kernel-side copy (a reflink on btrfs/XFS), else a userspace copy."""
    copied = 0
    try:
        while chunk := os.copy_file_range(source.fileno(), output.fileno(), 1 << 30):
            copied += chunk
        return
    except (AttributeError, OSError) as exc:
        unsupported = {errno.EXDEV, errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP, errno.EPERM}
        if copied or (isinstance(exc, OSError) and exc.errno not in unsupported):
            raise
    shutil.copyfileobj(source, output, 1 << 20)


def stage_files(mount: Path, job: Path, relative_names: list[str]) -> list[Path]:
    """Copy finished torrent files (paths relative to the downloads folder)
    into ``job`` and write the manifest last. Blocking."""
    if job.is_symlink():
        raise ValueError("A staging job folder cannot be a link")
    # Anything here without a manifest is an interrupted copy: start again.
    shutil.rmtree(job, ignore_errors=True)
    job.mkdir(parents=True)
    real_mount = mount.resolve()
    names = []
    for name in relative_names:
        rel = PurePosixPath(name)
        if rel.is_absolute() or ".." in rel.parts or not rel.parts:
            raise ValueError("A torrent file name points outside the downloads folder")
        source = mount.joinpath(*rel.parts)
        if source.is_symlink() or not source.resolve().is_relative_to(real_mount):
            raise ValueError("A torrent file escapes the downloads folder")
        target = job.joinpath(*rel.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.parent.resolve().is_relative_to(job.resolve()):
            raise ValueError("A staged file escapes its job folder")
        fd, tmp = tempfile.mkstemp(prefix=".copy-", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as output, source.open("rb") as source_file:
                copy_file(source_file, output)
                output.flush()
                os.fsync(output.fileno())
            os.replace(tmp, target)
        finally:
            Path(tmp).unlink(missing_ok=True)
        names.append(rel.as_posix())
    tmp_marker = job / ".ready.tmp"
    with tmp_marker.open("w") as output:
        json.dump(names, output)
        output.flush()
        os.fsync(output.fileno())
    os.replace(tmp_marker, job / _MANIFEST)
    return [job.joinpath(*PurePosixPath(n).parts) for n in names]


def _folder_readable(path: Path) -> bool:
    try:
        if not path.is_dir():
            return False
        next(path.iterdir(), None)
        return True
    except OSError:
        return False


def _check_writable(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(dir=path):
        pass


# --------------------------------------------------------------------------
# The plugin
# --------------------------------------------------------------------------


class TorrentPlugin:
    """indexer + download_client for ``plugin:prowlarr-qbittorrent``."""

    def __init__(self, context) -> None:
        self.ctx = context
        self.log = context.logger
        self._cached: tuple | None = None
        self._enqueue_locks: dict[str, asyncio.Lock] = {}
        self._staging: dict[str, asyncio.Task] = {}
        # Last good status per job, replayed while qBittorrent is briefly down.
        self._last_status: dict[str, DownloadTaskStatus] = {}
        try:
            # Each worker thread can reserve a glibc malloc arena; a small pool
            # keeps the process well inside the host's 1 GiB address-space cap.
            asyncio.get_running_loop().set_default_executor(
                concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix=NAME)
            )
        except RuntimeError:
            pass

    # -- settings ---------------------------------------------------------

    def _config(self) -> Config:
        snapshot = (
            tuple(sorted((str(k), str(v)) for k, v in self.ctx.settings.items())),
            str(self.ctx.data_dir),
        )
        if self._cached is None or self._cached[0] != snapshot:
            try:
                value: Config | ConfigError = build_config(self.ctx.settings, self.ctx.data_dir)
            except ConfigError as exc:
                value = exc
            self._cached = (snapshot, value)
        value = self._cached[1]
        if isinstance(value, ConfigError):
            raise value
        return value

    def _qbt(self, cfg: Config) -> Qbittorrent:
        return Qbittorrent(cfg.qbt_url, cfg.qbt_key)

    def is_configured(self) -> bool:
        try:
            self._config()
            return True
        except ConfigError:
            return False

    async def health_check(self) -> ServiceStatus:
        try:
            cfg = self._config()
        except ConfigError as exc:
            return ServiceStatus(status="error", message=str(exc))
        try:
            version = await self._qbt(cfg).version()
        except QbtError as exc:
            return ServiceStatus(status="error", message=str(exc))
        if not supports_api_key(version):
            return ServiceStatus(
                status="error",
                version=version or None,
                message="qBittorrent 5.2 or newer is required for API-key authentication",
            )
        if not await asyncio.to_thread(_folder_readable, cfg.mount):
            return ServiceStatus(
                status="error", message="The torrent downloads folder is not readable"
            )
        try:
            await asyncio.to_thread(_check_writable, cfg.stage)
        except OSError:
            return ServiceStatus(status="error", message="The staging folder is not writable")
        try:
            if cfg.backend == "torznab":
                await torznab_search(cfg, "", 5.0, caps=True)
            else:
                await _prowlarr_get(cfg, "system/status", None, 5.0)
        except (RuntimeError, ValueError, ElementTree.ParseError) as exc:
            return ServiceStatus(status="error", message=str(exc))
        return ServiceStatus(
            status="ok",
            version=version,
            message=f"qBittorrent {version} and {cfg.backend.capitalize()} are ready",
        )

    # -- indexer ----------------------------------------------------------

    async def search_album(
        self, artist_name, album_title, year=None, track_count=None, *, timeout=30.0
    ):
        return await self._search(artist_name, album_title, timeout)

    async def search_track(
        self, artist_name, track_title, album_title=None, duration_seconds=None, *, timeout=30.0
    ):
        return await self._search(artist_name, album_title or track_title, timeout)

    async def _search(self, artist: str, title: str, timeout: float) -> list[PluginSearchResult]:
        cfg = self._config()
        query = f"{artist} {title}".strip()
        async with asyncio.timeout(timeout):
            if cfg.backend == "torznab":
                rows = await torznab_search(cfg, query, timeout)
            else:
                rows = await prowlarr_search(cfg, query, timeout)
        results, seen = [], set()
        for row in rows[:_MAX_RESULTS]:
            if row["seeders"] == 0 or 3020 in row["categories"]:
                continue
            download, magnet = row["download_url"], row["magnet_url"]
            if magnet and not magnet.startswith("magnet:"):
                # Prowlarr and Jackett proxy magnets as HTTP links that redirect.
                download, magnet = download or magnet, ""
            if not download and not magnet:
                continue
            if len(cfg.index_key) >= 8:
                download = download.replace(cfg.index_key, KEY_PLACEHOLDER)
            identity = row["info_hash"].lower() or magnet or download
            if identity in seen:
                continue
            seen.add(identity)
            score = self.ctx.scoring.album_match(artist, title, row["title"])
            # Unknown seeders: below the host's default 0.70 automatic threshold.
            if row["seeders"] is None:
                score = min(score, 0.69)
            payload = json.dumps(
                {"download_url": download, "magnet_url": magnet, "info_hash": row["info_hash"]},
                sort_keys=True,
                separators=(",", ":"),
            )
            results.append(
                PluginSearchResult(
                    title=row["title"],
                    size_bytes=max(0, row["size"]),
                    score=score,
                    quality_tier=quality_tier(row["title"], row["categories"]),
                    payload=payload,
                )
            )
        return results

    # -- download client --------------------------------------------------

    def _indexer_key(self, url: str) -> str:
        """The configured key of the indexer that issued ``url``; never another host's."""
        for backend in ("prowlarr", "torznab"):
            base = str(self.ctx.settings.get(f"{backend}_url") or "").strip()
            key = str(self.ctx.settings.get(f"{backend}_api_key") or "").strip()
            if base and key and origin(base) == origin(url):
                return key
        raise ValueError("The grab link does not belong to a configured indexer")

    async def enqueue(self, request: EnqueueRequest) -> TaskHandle:
        try:
            cfg = self._config()
        except ConfigError as exc:
            raise DownloadFailure(str(exc), fault="local") from None
        try:
            payload = json.loads(request.payload or "")
        except ValueError:
            raise DownloadFailure("Invalid torrent payload", fault="source") from None
        if not isinstance(payload, dict):
            raise DownloadFailure("Invalid torrent payload", fault="source")
        download = payload.get("download_url") or ""
        magnet = payload.get("magnet_url") or ""
        for value in (download, magnet):
            if not isinstance(value, str) or "\r" in value or "\n" in value:
                raise DownloadFailure("Invalid torrent link", fault="source")
        if download:
            try:
                http_url(download)
            except ConfigError:
                raise DownloadFailure("Invalid torrent download link", fault="source") from None
        if magnet and not magnet.startswith("magnet:?"):
            raise DownloadFailure("Invalid magnet link", fault="source")
        if not download and not magnet:
            raise DownloadFailure("The release has no magnet or torrent link", fault="source")
        if KEY_PLACEHOLDER in download:
            try:
                download = download.replace(KEY_PLACEHOLDER, self._indexer_key(download))
            except ValueError as exc:
                raise DownloadFailure(str(exc), fault="source") from None
        tag = request.job_name or f"droppedneedle-{request.task_id}"
        if not _TAG.fullmatch(tag):
            raise DownloadFailure("Invalid task correlation tag", fault="unknown")
        if len(self._enqueue_locks) > 1000:
            for key in [k for k, v in self._enqueue_locks.items() if not v.locked()]:
                del self._enqueue_locks[key]
        async with self._enqueue_locks.setdefault(tag, asyncio.Lock()):
            return await self._add(cfg, tag, magnet or download)

    async def _add(self, cfg: Config, tag: str, url: str) -> TaskHandle:
        qbt = self._qbt(cfg)
        handle = TaskHandle(source=SOURCE, job_name=tag)
        try:
            if await self._find(cfg, tag) is not None:
                return handle  # already added (a retry, or an earlier ambiguous answer)
            try:
                await qbt.add(url, category=cfg.category, tag=tag)
            except QbtError as exc:
                if exc.outage and await self._find(cfg, tag) is not None:
                    return handle  # a timeout after qBittorrent had accepted it
                raise
            for _ in range(_ADD_CORRELATE_ATTEMPTS):
                if await self._find(cfg, tag) is not None:
                    return handle
                await asyncio.sleep(_ADD_CORRELATE_DELAY)
        except QbtError as exc:
            # Down, or a rejected key: our side. A rejected torrent: the release.
            fault = "local" if exc.auth or exc.outage else "source"
            raise DownloadFailure(str(exc), fault=fault) from None
        raise DownloadFailure(
            "qBittorrent accepted the link but no torrent appeared (dead link or tracker?)",
            fault="source",
        )

    async def _find(self, cfg: Config, tag: str) -> Torrent | None:
        """The one torrent carrying exactly this task tag in our category.
        An untagged or foreign torrent is never adopted."""
        rows = await self._qbt(cfg).torrents(tag=tag, category=cfg.category)
        owned = [
            row
            for row in rows
            if tag in {part.strip() for part in row.tags.split(",")}
            and row.category == cfg.category
        ]
        return owned[0] if len(owned) == 1 else None

    def _job_dir(self, cfg: Config, handle: TaskHandle) -> Path:
        if not _TAG.fullmatch(handle.job_name or ""):
            raise ValueError("Missing torrent correlation tag")
        return cfg.stage / hashlib.sha256(handle.job_name.encode()).hexdigest()

    async def get_status(self, handle: TaskHandle) -> DownloadTaskStatus:
        try:
            cfg = self._config()
        except ConfigError as exc:
            raise DownloadFailure(str(exc), fault="local") from None
        job = self._job_dir(cfg, handle)
        staged = await asyncio.to_thread(read_manifest, job)
        if staged is not None:
            # Staged copies outlive the torrent being removed or rechecked.
            self._last_status.pop(handle.job_name, None)
            return _completed(staged)
        try:
            torrent = await self._find(cfg, handle.job_name)
        except QbtError as exc:
            if exc.auth:
                raise DownloadFailure(str(exc), fault="local") from None
            # A brief outage must not fail the task: report no active transfer
            # and let the host's own timeout be the backstop.
            self.log.warning("qBittorrent unavailable while polling %s: %s", handle.job_name, exc)
            last = self._last_status.get(handle.job_name)
            if last is None:
                return DownloadTaskStatus(status="queued", matched_transfers=1)
            return dataclasses.replace(last, has_active_transfer=False)
        if torrent is None:
            self._last_status.pop(handle.job_name, None)
            return DownloadTaskStatus(status="queued", matched_transfers=0)
        if torrent.state in _FAILED_STATES:
            self._last_status.pop(handle.job_name, None)
            # qBittorrent's error/missingFiles are disk problems on this machine.
            raise DownloadFailure(
                f"qBittorrent reports the torrent as {torrent.state}", fault="local"
            )
        if torrent.complete:
            status = await self._staging_status(cfg, handle.job_name, job, torrent)
        else:
            status = _transfer_status(torrent)
        self._last_status.pop(handle.job_name, None)
        self._last_status[handle.job_name] = status
        while len(self._last_status) > 2000:
            self._last_status.pop(next(iter(self._last_status)))
        return status

    async def _staging_status(self, cfg, tag, job, torrent) -> DownloadTaskStatus:
        key = str(job)
        task = self._staging.get(key)
        if task is None:
            task = asyncio.create_task(self._stage(cfg, job, torrent))
            self._staging[key] = task
        if task.done():
            self._staging.pop(key, None)
            exc = task.exception()
            if exc is None:
                return _completed(task.result())
            if isinstance(exc, DownloadFailure):
                raise exc
            if isinstance(exc, QbtError) and not exc.auth:
                # qBittorrent blinked while listing files: the next poll restarts the copy.
                self.log.warning("staging %s waits for qBittorrent: %s", tag, exc)
                return _copying(torrent)
            if isinstance(exc, QbtError):
                raise DownloadFailure(str(exc), fault="local")
            if isinstance(exc, OSError):
                raise DownloadFailure(
                    f"Could not copy into the staging folder: {exc}", fault="local"
                )
            raise DownloadFailure(f"Could not stage the torrent: {exc}", fault="unknown")
        return _copying(torrent)

    async def _stage(self, cfg: Config, job: Path, torrent: Torrent) -> list[Path]:
        files = await self._qbt(cfg).files(torrent.hash)
        names = [
            str(row.get("name") or "").replace("\\", "/")
            for row in files[:_MAX_FILES]
            if float(row.get("progress") or 0) >= 1.0
            and PurePosixPath(str(row.get("name") or "")).suffix.lower() in _AUDIO_SUFFIXES
        ]
        if not names:
            raise DownloadFailure("The finished torrent has no audio files", fault="source")
        staged = await asyncio.to_thread(stage_files, cfg.mount, job, names)
        self.log.info("staged %d files for %s", len(staged), torrent.hash)
        return staged

    async def inspect_materialization(self, handle: TaskHandle) -> DownloadMaterialization:
        cfg = self._config()
        job = self._job_dir(cfg, handle)
        staged = await asyncio.to_thread(read_manifest, job)
        if staged is not None:
            return DownloadMaterialization(
                state="completed",
                mount_root=str(cfg.stage),
                workspace_path=str(job),
                file_paths=[str(path) for path in staged],
                mount_healthy=await asyncio.to_thread(_folder_readable, cfg.stage),
            )
        torrent = await self._find(cfg, handle.job_name)
        if torrent is None:
            state = "missing"
        elif torrent.state in _FAILED_STATES:
            state = "failed"
        else:
            state = "active"
        # Never a workspace or file path into the torrent's own files.
        return DownloadMaterialization(
            state=state,
            mount_root=str(cfg.stage),
            mount_healthy=await asyncio.to_thread(_folder_readable, cfg.mount),
        )

    async def abort(self, handle: TaskHandle) -> bool:
        return await self._remove_unless_complete(self._config(), handle, "abort")

    async def discard_client_artifacts(self, handle: TaskHandle) -> bool:
        cfg = self._config()
        removed = await self._remove_unless_complete(cfg, handle, "discard")
        if removed:
            job = self._job_dir(cfg, handle)
            task = self._staging.get(str(job))
            if task is not None and not task.done():
                # Never delete underneath a running copy. asyncio.wait does not
                # cancel the copy if the host stops waiting for this call.
                await asyncio.wait({task})
            self._staging.pop(str(job), None)
            await asyncio.to_thread(shutil.rmtree, job, True)
        return removed

    async def _remove_unless_complete(self, cfg: Config, handle: TaskHandle, action: str) -> bool:
        """Remove an incomplete, task-tagged torrent with its partial data.
        A completed torrent is never removed: it keeps seeding. Already gone
        counts as success."""
        torrent = await self._find(cfg, handle.job_name)
        if torrent is None:
            return True
        if torrent.progress >= 1.0:
            self.log.info("%s left completed torrent %s seeding", action, torrent.hash)
            return True
        return await self._qbt(cfg).delete(torrent.hash)


def _completed(staged: list[Path]) -> DownloadTaskStatus:
    return DownloadTaskStatus(
        status="completed",
        files_total=len(staged),
        files_completed=len(staged),
        progress_percent=100.0,
        succeeded_filenames=[path.name for path in staged],
        matched_transfers=1,
    )


def _copying(torrent: Torrent) -> DownloadTaskStatus:
    """The transfer is done and the staging copy is still running."""
    return DownloadTaskStatus(
        status="downloading",
        files_total=1,
        bytes_total=torrent.size,
        bytes_downloaded=torrent.size,
        progress_percent=100.0,
        has_active_transfer=True,
        matched_transfers=1,
    )


def _transfer_status(torrent: Torrent) -> DownloadTaskStatus:
    total = torrent.size
    done = min(torrent.downloaded, total) if total else torrent.downloaded
    common = {
        "files_total": 1,
        "bytes_total": total,
        "bytes_downloaded": done,
        "progress_percent": round(torrent.progress * 100.0, 1),
        "matched_transfers": 1,
    }
    if torrent.state in _CHECKING_STATES:
        return DownloadTaskStatus(status="processing", has_active_transfer=True, **common)
    if torrent.state in _ACTIVE_STATES:
        return DownloadTaskStatus(status="downloading", has_active_transfer=True, **common)
    # metaDL, stalled, queued, paused, allocating: waiting, no bytes moving.
    return DownloadTaskStatus(status="queued", **common)
