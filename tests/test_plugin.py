import asyncio
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from conftest import PROWLARR_KEY, QBT_KEY, ROOT, SDK
from droppedneedle_plugin import DownloadFailure, EnqueueRequest, TaskHandle

import plugin as p

run = asyncio.run


def handle(job="droppedneedle-task1-0"):
    return TaskHandle(source=p.SOURCE, job_name=job)


def request(payload, job="droppedneedle-task1-0"):
    return EnqueueRequest(
        task_id="task1", source=p.SOURCE, payload=json.dumps(payload), job_name=job
    )


MAGNET = "magnet:?xt=urn:btih:" + "a" * 40


# -- manifest -----------------------------------------------------------------


def test_manifest_is_api_version_2_and_matches_the_code():
    manifest = tomllib.loads((ROOT / "plugin.toml").read_text())
    meta = manifest["plugin"]
    assert meta["api_version"] == 2
    assert re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", meta["name"])
    assert p.SOURCE == "plugin:" + meta["name"]
    module, _, cls = meta["entrypoint"].partition(":")
    assert module == "plugin" and hasattr(p, cls)
    keys = {s["key"] for s in manifest["settings"]}
    # The host only accepts finished files in data_dir or the downloads_dir setting.
    assert "downloads_dir" in keys
    assert {"qbittorrent_url", "qbittorrent_api_key", "downloads_path"} <= keys
    secrets = {s["key"] for s in manifest["settings"] if s["secret"]}
    assert secrets == {"qbittorrent_api_key", "prowlarr_api_key", "torznab_api_key"}
    assert set(manifest["plugin"]["capabilities"]) == {"download_client", "indexer"}


def test_plugin_uses_only_the_standard_library_and_the_helper():
    source = (ROOT / "plugin.py").read_text()
    imported = set(re.findall(r"^(?:from|import) ([a-zA-Z_]+)", source, re.MULTILINE))
    allowed = set(sys.stdlib_module_names) | {"droppedneedle_plugin", "__future__"}
    assert imported <= allowed, imported - allowed


# -- settings -----------------------------------------------------------------


def test_configured_and_staging_defaults_to_the_data_folder(make_plugin, folders):
    plugin = make_plugin(downloads_dir="")
    assert plugin.is_configured()
    assert plugin._config().stage == folders.data / "staging"


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"qbittorrent_url": ""}, "qBittorrent URL"),
        ({"qbittorrent_url": "http://user:pw@qbt"}, "user name or password"),
        ({"downloads_path": "relative/path"}, "absolute"),
        ({"search_backend": "jackett"}, "prowlarr or torznab"),
        ({"prowlarr_api_key": ""}, "Prowlarr API key"),
        ({"categories": "3000,abc"}, "comma-separated"),
        ({"category": "a,b"}, "commas"),
        ({"qbittorrent_api_key": "gAAAAA" + "x" * 120}, "type the key again"),
    ],
)
def test_invalid_settings_are_reported(make_plugin, overrides, message):
    plugin = make_plugin(**overrides)
    assert not plugin.is_configured()
    status = run(plugin.health_check())
    assert status.status == "error" and message in status.message


def test_staging_may_not_overlap_downloads(make_plugin, folders):
    plugin = make_plugin(downloads_dir=str(folders.mount / "staging"))
    assert "overlap" in run(plugin.health_check()).message


def test_health_ok(make_plugin, folders):
    status = run(make_plugin().health_check())
    assert status.status == "ok", status.message
    assert folders.stage.is_dir()


def test_health_torznab(make_plugin):
    assert run(make_plugin(search_backend="torznab").health_check()).status == "ok"


def test_health_reports_a_rejected_key(make_plugin):
    status = run(make_plugin(qbittorrent_api_key="wrong-key-123").health_check())
    assert status.status == "error" and "rejected" in status.message


# -- search -------------------------------------------------------------------


def prowlarr_hit(**fields):
    hit = {
        "title": "Artist - Album (2020) [FLAC]",
        "size": 300,
        "protocol": "torrent",
        "seeders": 5,
        "downloadUrl": f"/1/download?apikey={PROWLARR_KEY}&link=abc",
        "magnetUrl": "",
        "infoHash": "",
        "categories": [{"id": 3040}],
    }
    hit.update(fields)
    return hit


def test_prowlarr_search_filters_and_redacts(make_plugin, services):
    services.prowlarr_results = [
        prowlarr_hit(),
        prowlarr_hit(protocol="usenet", title="Usenet"),
        prowlarr_hit(seeders=0, title="Dead", downloadUrl="/2"),
        prowlarr_hit(categories=[{"id": 3020}], title="Video", downloadUrl="/3"),
        prowlarr_hit(title="Dup", downloadUrl="/1/download?apikey=x&link=abc", infoHash="ABC"),
        prowlarr_hit(title="Dup again", downloadUrl="/other", infoHash="abc"),
        prowlarr_hit(
            title="Artist Album 320",
            seeders=None,
            downloadUrl="",
            magnetUrl="http://px/m",
            categories=[{"id": 3010}],
        ),
    ]
    results = run(make_plugin().search_album("Artist", "Album"))
    titles = [r.title for r in results]
    assert titles == ["Artist - Album (2020) [FLAC]", "Dup", "Artist Album 320"]
    first = json.loads(results[0].payload)
    assert PROWLARR_KEY not in results[0].payload
    assert first["download_url"].startswith(
        services.base + "/1/download?apikey=" + p.KEY_PLACEHOLDER
    )
    assert results[0].quality_tier == "lossless" and results[0].score == 1.0
    proxied = json.loads(results[2].payload)
    assert proxied == {"download_url": "http://px/m", "info_hash": "", "magnet_url": ""}
    assert results[2].score <= 0.69 and results[2].quality_tier == "mp3_320"


TORZNAB = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>
<item><title>Artist - Album FLAC</title><link>/download/abc?apikey=torznab-key-0123456789</link>
<torznab:attr name="seeders" value="3"/><torznab:attr name="size" value="200"/>
<torznab:attr name="category" value="3040"/></item>
<item><title>Artist - Album magnet</title><link>magnet:?xt=urn:btih:bbb</link>
<torznab:attr name="infohash" value="bbb"/></item>
</channel></rss>"""


def test_torznab_search(make_plugin, services):
    services.torznab_body = TORZNAB
    results = run(make_plugin(search_backend="torznab").search_album("Artist", "Album"))
    assert [r.title for r in results] == ["Artist - Album FLAC", "Artist - Album magnet"]
    link = json.loads(results[0].payload)["download_url"]
    assert link == f"{services.base}/download/abc?apikey={p.KEY_PLACEHOLDER}"
    assert json.loads(results[1].payload)["magnet_url"] == "magnet:?xt=urn:btih:bbb"
    assert results[1].score <= 0.69  # no seeder count


@pytest.mark.parametrize(
    "body",
    [
        b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]><rss>&a;</rss>',
        "<rss/>".encode("utf-16"),
    ],
)
def test_torznab_rejects_entities_and_non_utf8(make_plugin, services, body):
    services.torznab_body = body
    with pytest.raises((ValueError, UnicodeDecodeError)):
        run(make_plugin(search_backend="torznab").search_album("Artist", "Album"))


def test_torznab_size_is_bounded(make_plugin, services, monkeypatch):
    monkeypatch.setattr(p, "_MAX_TORZNAB_BYTES", 1000)
    services.torznab_body = b"<rss>" + b" " * 2000 + b"</rss>"
    with pytest.raises(RuntimeError, match="larger than"):
        run(make_plugin(search_backend="torznab").search_album("Artist", "Album"))


def test_redirects_are_not_followed(make_plugin, services):
    plugin = make_plugin(search_backend="torznab", torznab_url=f"{services.base}/redirect")
    with pytest.raises(RuntimeError, match="HTTP 302"):
        run(plugin.search_album("Artist", "Album"))


# -- enqueue ------------------------------------------------------------------


def test_enqueue_restores_the_key_for_its_own_indexer_only(make_plugin, services):
    plugin = make_plugin()
    link = f"{services.base}/1/download?apikey={p.KEY_PLACEHOLDER}&link=abc"
    result = run(plugin.enqueue(request({"download_url": link, "magnet_url": ""})))
    assert result.job_name == "droppedneedle-task1-0" and result.source == p.SOURCE
    [add] = services.adds
    assert add == {
        "urls": f"{services.base}/1/download?apikey={PROWLARR_KEY}&link=abc",
        "category": "droppedneedle",
        "tags": "droppedneedle-task1-0",
    }
    foreign = request(
        {"download_url": f"http://evil.test/?k={p.KEY_PLACEHOLDER}"}, job="droppedneedle-t-1"
    )
    with pytest.raises(DownloadFailure) as caught:
        run(plugin.enqueue(foreign))
    assert caught.value.fault == "source" and len(services.adds) == 1


def test_enqueue_prefers_the_magnet_and_does_not_add_twice(make_plugin, services):
    plugin = make_plugin()
    payload = {"download_url": "http://x/a.torrent", "magnet_url": MAGNET}
    run(plugin.enqueue(request(payload)))
    run(plugin.enqueue(request(payload)))  # a retry recovers by tag
    assert [a["urls"] for a in services.adds] == [MAGNET]


def test_concurrent_enqueue_adds_once(make_plugin, services):
    plugin = make_plugin()

    async def both():
        return await asyncio.gather(
            *(plugin.enqueue(request({"magnet_url": MAGNET})) for _ in range(3))
        )

    assert len(run(both())) == 3
    assert len(services.adds) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"download_url": "http://x/a\nhttp://x/b"},
        {"download_url": "file:///etc/passwd"},
        {"magnet_url": "http://not-a-magnet"},
        {},
    ],
)
def test_enqueue_refuses_bad_links(make_plugin, services, payload):
    with pytest.raises(DownloadFailure) as caught:
        run(make_plugin().enqueue(request(payload)))
    assert caught.value.fault == "source" and not services.adds


def test_enqueue_refuses_a_tag_with_a_comma(make_plugin, services):
    with pytest.raises(DownloadFailure):
        run(make_plugin().enqueue(request({"magnet_url": MAGNET}, job="droppedneedle-a,b")))
    assert not services.adds


def test_enqueue_faults(make_plugin, services):
    plugin = make_plugin()
    services.qbt_down = True
    with pytest.raises(DownloadFailure) as caught:
        run(plugin.enqueue(request({"magnet_url": MAGNET})))
    assert caught.value.fault == "local"
    services.qbt_down = False
    services.add_answer = "Fails."
    with pytest.raises(DownloadFailure) as caught:
        run(plugin.enqueue(request({"magnet_url": MAGNET})))
    assert caught.value.fault == "source"
    services.add_answer, services.add_registers = "Ok.", False
    with pytest.raises(DownloadFailure, match="no torrent appeared") as caught:
        run(plugin.enqueue(request({"magnet_url": MAGNET})))
    assert caught.value.fault == "source"


def test_ambiguous_add_failure_recovers_by_tag(make_plugin, services):
    services.add_status = 500  # registered, but the answer looks like a failure
    result = run(make_plugin().enqueue(request({"magnet_url": MAGNET})))
    assert result.job_name == "droppedneedle-task1-0"
    assert len(services.adds) == 1


def test_untagged_or_foreign_torrents_are_never_adopted(make_plugin, services):
    services.torrents = [
        {
            "hash": "1",
            "category": "droppedneedle",
            "tags": "other",
            "progress": 1,
            "state": "uploading",
        },
        {"hash": "2", "category": "movies", "tags": "droppedneedle-task1-0", "progress": 1},
    ]
    status = run(make_plugin().get_status(handle()))
    assert status.status == "queued" and status.matched_transfers == 0


# -- transfer, staging, import --------------------------------------------------


def add_release(services, folders, plugin, files):
    """Enqueue, then put the finished files on disk."""
    services.next_files = [{"name": name, "progress": 1.0, "size": 1} for name in files]
    run(plugin.enqueue(request({"magnet_url": MAGNET})))
    for name in files:
        path = folders.mount / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"audio of {name}".encode())
    return services.torrent("droppedneedle-task1-0")


def finish(torrent):
    torrent.update(progress=1.0, state="stalledUP", downloaded=torrent["size"])


async def poll_until_done(plugin, job=None):
    for _ in range(200):
        status = await plugin.get_status(handle(job) if job else handle())
        if status.status != "downloading":
            return status
        await asyncio.sleep(0.01)
    raise AssertionError("staging did not finish")


FILES = [
    "Album/CD1/01 - Song.flac",
    "Album/CD2/01 - Song.flac",
    "Album/cover.jpg",
    "Album/info.nfo",
]


def test_full_lifecycle_copies_audio_and_keeps_the_torrent(make_plugin, services, folders):
    plugin = make_plugin()
    torrent = add_release(services, folders, plugin, FILES)
    run(lifecycle(plugin, services, folders, torrent))


async def lifecycle(plugin, services, folders, torrent):
    status = await plugin.get_status(handle())
    assert status.status == "downloading" and status.has_active_transfer
    assert status.progress_percent == 40.0
    seen = await plugin.inspect_materialization(handle())
    assert seen.state == "active" and seen.file_paths == [] and seen.workspace_path == ""

    finish(torrent)
    status = await poll_until_done(plugin)
    assert status.status == "completed" and status.files_completed == 2

    seen = await plugin.inspect_materialization(handle())
    assert seen.state == "completed"
    staged = [Path(path) for path in seen.file_paths]
    assert sorted(path.relative_to(Path(seen.workspace_path)).as_posix() for path in staged) == [
        "Album/CD1/01 - Song.flac",
        "Album/CD2/01 - Song.flac",
    ]
    for path in staged:
        # Inside the staging folder the host accepts, and a copy, not a link.
        assert path.is_relative_to(folders.stage) and path.is_absolute()
        assert not path.is_symlink()
        path.write_bytes(b"retagged")
    assert (folders.mount / FILES[0]).read_bytes() == b"audio of " + FILES[0].encode()

    # After import the host discards: the completed torrent keeps seeding,
    # the staging copies go.
    assert await plugin.discard_client_artifacts(handle()) is True
    assert services.deletes == [] and services.torrents
    assert not Path(seen.workspace_path).exists()
    assert (folders.mount / FILES[0]).exists()


def test_staging_survives_a_restart_and_the_torrent_being_removed(make_plugin, services, folders):
    plugin = make_plugin()
    finish(add_release(services, folders, plugin, FILES[:1]))
    run(poll_until_done(plugin))
    services.torrents.clear()
    services.qbt_down = True
    restarted = make_plugin()
    assert run(restarted.get_status(handle())).status == "completed"
    assert len(run(restarted.inspect_materialization(handle())).file_paths) == 1


def test_an_interrupted_copy_starts_again(make_plugin, services, folders):
    plugin = make_plugin()
    finish(add_release(services, folders, plugin, FILES[:1]))
    job = plugin._job_dir(plugin._config(), handle())
    job.mkdir(parents=True)
    (job / ".copy-leftover").write_bytes(b"half")
    run(poll_until_done(plugin))
    assert not (job / ".copy-leftover").exists()
    assert (job / FILES[0]).read_bytes() == b"audio of " + FILES[0].encode()


def test_a_release_without_audio_blames_the_source(make_plugin, services, folders):
    plugin = make_plugin()
    finish(add_release(services, folders, plugin, ["Album/readme.txt"]))
    with pytest.raises(DownloadFailure) as caught:
        run(poll_until_done(plugin))
    assert caught.value.fault == "source"


def test_a_file_name_escaping_the_downloads_folder_is_refused(make_plugin, services, folders):
    plugin = make_plugin()
    torrent = add_release(services, folders, plugin, [])
    services.files[torrent["hash"]] = [{"name": "../outside.flac", "progress": 1.0}]
    (folders.mount.parent / "outside.flac").write_bytes(b"x")
    finish(torrent)
    with pytest.raises(DownloadFailure) as caught:
        run(poll_until_done(plugin))
    assert caught.value.fault == "unknown"
    assert not any(folders.stage.rglob("*.flac"))


def test_a_symlinked_file_is_refused(make_plugin, services, folders):
    plugin = make_plugin()
    torrent = add_release(services, folders, plugin, [])
    secret = folders.mount.parent / "secret.flac"
    secret.write_bytes(b"x")
    (folders.mount / "link.flac").symlink_to(secret)
    services.files[torrent["hash"]] = [{"name": "link.flac", "progress": 1.0}]
    finish(torrent)
    with pytest.raises(DownloadFailure):
        run(poll_until_done(plugin))


def test_cancel_removes_only_an_incomplete_torrent_with_its_data(make_plugin, services, folders):
    plugin = make_plugin()
    torrent = add_release(services, folders, plugin, FILES[:1])
    assert run(plugin.abort(handle())) is True
    assert services.deletes == [{"hashes": torrent["hash"], "deleteFiles": "true"}]
    # Already gone is success, so cleanup does not loop.
    assert run(plugin.discard_client_artifacts(handle())) is True


def test_abort_keeps_a_completed_torrent(make_plugin, services, folders):
    plugin = make_plugin()
    finish(add_release(services, folders, plugin, FILES[:1]))
    assert run(plugin.abort(handle())) is True
    assert services.deletes == []


def test_a_brief_outage_replays_the_last_status(make_plugin, services, folders):
    plugin = make_plugin()
    add_release(services, folders, plugin, FILES[:1])
    assert run(plugin.get_status(handle())).progress_percent == 40.0
    services.qbt_down = True
    status = run(plugin.get_status(handle()))
    assert status.status == "downloading" and status.progress_percent == 40.0
    assert not status.has_active_transfer


def test_a_rejected_key_while_polling_is_a_local_fault(make_plugin, services, folders):
    add_release(services, folders, make_plugin(), FILES[:1])
    with pytest.raises(DownloadFailure) as caught:
        run(make_plugin(qbittorrent_api_key="wrong-key-123").get_status(handle()))
    assert caught.value.fault == "local"


def test_a_torrent_error_is_a_local_fault(make_plugin, services, folders):
    plugin = make_plugin()
    add_release(services, folders, plugin, FILES[:1])["state"] = "missingFiles"
    with pytest.raises(DownloadFailure) as caught:
        run(plugin.get_status(handle()))
    assert caught.value.fault == "local"


def test_waiting_and_checking_states(make_plugin, services, folders):
    plugin = make_plugin()
    torrent = add_release(services, folders, plugin, FILES[:1])
    torrent["state"] = "stalledDL"
    status = run(plugin.get_status(handle()))
    assert status.status == "queued" and not status.has_active_transfer
    torrent.update(state="checkingUP", progress=1.0)
    assert run(plugin.get_status(handle())).status == "processing"


# -- the real protocol, as DroppedNeedle runs it ------------------------------


def test_runs_under_the_droppedneedle_helper(settings, services, folders):
    services.prowlarr_results = [prowlarr_hit()]
    env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": f"{SDK}{os.pathsep}{ROOT}"}
    process = subprocess.Popen(
        [sys.executable, "-m", "droppedneedle_plugin", "plugin:TorrentPlugin"],
        cwd=folders.data,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    def call(request_id, method, params=None):
        message = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
        process.stdin.write((json.dumps(message) + "\n").encode())
        process.stdin.flush()
        while True:
            answer = json.loads(process.stdout.readline())
            if answer.get("id") == request_id:
                return answer

    try:
        manifest = tomllib.loads((ROOT / "plugin.toml").read_text())["plugin"]
        init = call(
            1,
            "initialize",
            {
                "protocol_version": 1,
                "host": {"name": "droppedneedle", "version": "3.1.0"},
                "plugin": {
                    "name": manifest["name"],
                    "version": manifest["version"],
                    "api_version": 2,
                    "entrypoint": manifest["entrypoint"],
                    "capabilities": manifest["capabilities"],
                },
                "settings": settings,
                "plugin_dir": str(ROOT),
                "data_dir": str(folders.data),
            },
        )
        assert init["result"] == {
            "protocol_version": 1,
            "capabilities": ["download_client", "indexer"],
        }
        health = call(2, "plugin.health")["result"]
        assert health["status"] == "ok" and health["configured"] is True, health
        found = call(
            3,
            "indexer.search_album",
            {
                "artist_name": "Artist",
                "album_title": "Album",
                "year": None,
                "track_count": 10,
                "timeout": 30,
            },
        )["result"]
        assert len(found) == 1 and PROWLARR_KEY not in json.dumps(found)
        enqueued = call(
            4,
            "download_client.enqueue",
            {
                "task_id": "t9",
                "source": p.SOURCE,
                "files": [],
                "payload": found[0]["payload"],
                "job_name": "droppedneedle-t9-0",
                "download_type": "album",
            },
        )["result"]
        assert enqueued["job_name"] == "droppedneedle-t9-0"
        assert services.adds[0]["urls"].endswith(f"apikey={PROWLARR_KEY}&link=abc")
        status = call(5, "download_client.status", {"handle": enqueued})["result"]
        assert status["status"] == "downloading"
        bad = call(
            6,
            "download_client.enqueue",
            {
                "task_id": "t9",
                "source": p.SOURCE,
                "payload": "{}",
                "job_name": "droppedneedle-t9-1",
            },
        )
        assert bad["error"]["data"] == {"fault": "source"}
        assert QBT_KEY not in json.dumps(bad)
        call(7, "shutdown")
    finally:
        process.stdin.close()
        process.wait(timeout=10)
        process.stdout.close()
        process.stderr.close()
