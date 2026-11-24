# DroppedNeedle torrent plugin (v3)

Prowlarr or Torznab search with qBittorrent downloads for **DroppedNeedle v3**
(plugin API 2). This is a port of
[alphyriver/droppedneedle-torrent-plugin](https://github.com/alphyriver/droppedneedle-torrent-plugin),
which was written for DroppedNeedle v2 (plugin API 1) and is refused by v3 with
`written for api_version 1; this DroppedNeedle reads api_version 2`.

Status: 45 tests pass against the plugin helper of DroppedNeedle **v3.1.0**
(`29b41137`), including a run under the real helper process. Not yet tested
against a live qBittorrent and Prowlarr; do one real download before relying on it.

## Requirements

- DroppedNeedle v3 (tested with v3.1.0), with Python plugins enabled (the default image).
- qBittorrent 5.2 or newer with a Web UI API key. Username/password login is not supported.
- Prowlarr, or one Torznab feed.
- The qBittorrent category's save folder mounted into the DroppedNeedle container
  (read-only is fine).
- Disk space for one extra copy of each release while it imports. Copies use
  `copy_file_range`, so on btrfs or XFS with staging on the same filesystem they
  are reflinks that share blocks.

## Install

In **Settings > Plugins**, paste `https://github.com/benoittravers/droppedneedle-torrent-plugin`
and press **Preview**, then **Install**. It installs disabled; fill in the settings,
enable it, and add **Torrents** to source priority.

| Setting | Example / meaning |
| --- | --- |
| qBittorrent URL | `http://qbittorrent:8080` |
| qBittorrent API key | qBittorrent > Options > Web UI > API key |
| Torrent downloads folder | `/qbittorrent-downloads`: where the category's save folder is mounted in DroppedNeedle |
| Import staging folder | `/plugin-staging`, or empty for the plugin's data folder (`/app/plugins/.data/prowlarr-qbittorrent/staging`) |
| qBittorrent category | `droppedneedle` (default); create it in qBittorrent with the save folder you mounted |
| Search backend | `prowlarr` (default) or `torznab` |
| Prowlarr URL / API key | for Prowlarr |
| Torznab URL / API key | for Torznab; the feed URL, with or without `/api` |
| Audio category IDs | `3000` (default), comma-separated |

The staging folder must not be inside the torrent downloads folder, or the other way
round. DroppedNeedle only imports files from the plugin's data folder or from the
staging folder setting, so that is where the copies go.

Example Docker Compose mounts:

```yaml
volumes:
  - ./plugins:/app/plugins                               # keep installed plugins
  - /mnt/media/torrents/droppedneedle:/qbittorrent-downloads:ro
  - /mnt/media/droppedneedle/plugin-staging:/plugin-staging
```

### Coming from the v2 plugin

DroppedNeedle's v2 to v3 upgrade carries plugin settings over, switched off. The setting
keys are the same, except `staging_path` is now `downloads_dir` (Import staging
folder), so set that again. If you used the old fork's `prepare_migration.py`, your API keys
were stored encrypted with the old installation key, and the plugin cannot open them.
Health then says "still encrypted from the old fork". Type the keys again.

## Behavior

- **Search:** Prowlarr results are limited to torrents. Zero-seeder releases and
  audio-video category 3020 are dropped. Duplicates are removed by info hash, magnet
  or link. Proxied magnets (Prowlarr/Jackett return them as HTTP redirects) are sent
  to qBittorrent as download links. Releases with no seeder count are capped at a score of
  0.69, below DroppedNeedle's default 0.70 automatic threshold, so a person picks them.
- **API keys:** indexer keys inside grab links are replaced with a placeholder in
  search results (which reach browsers and the database). The real key is put back at
  download time, and only for a link on the configured indexer's own host.
- **Tracking downloads:** each download gets a qBittorrent tag named after the
  DroppedNeedle job, in the configured category, so it is found again after a restart
  or an add that timed out. Torrents without that exact tag are never adopted or deleted.
- **Finished torrents keep seeding.** Their audio files are copied into
  `<staging>/<job hash>/`. DroppedNeedle imports those copies and removes them, so
  retagging or moving never touches the torrent's files. The download shows as
  "downloading" at 100% while the copy runs, and "completed" once it is done. An
  interrupted copy starts again, and a finished one survives restarts and the torrent
  being removed.
- **Cancel or failure:** deletes the torrent and its partial data only if it is
  **incomplete** and carries the task's tag. Completed torrents are always kept. The
  staging copy is removed.
- **Failures are labelled for DroppedNeedle's retry logic:**
  - *This machine:* qBittorrent unreachable, a rejected API key, a qBittorrent
    "error"/"missing files" state, or a staging disk problem. The release is not blamed.
  - *The release:* rejected or dead links, and torrents with no audio files.
  - A brief qBittorrent outage while polling replays the last known progress instead of
    failing the download.
- **Safety limits:** no HTTP redirects are followed. Torznab responses are capped at
  8 MiB, and XML with DTDs or entities is refused. Torrent file names that leave the
  downloads folder, or that are links, are refused.
- **Dependencies:** none beyond Python's standard library and DroppedNeedle's
  `droppedneedle_plugin` helper.

Keep the category save path and plugin folders stable while downloads are active. The
plugin assumes the torrent's save path is the category folder you mounted.

## Tests

```sh
git clone https://github.com/DroppedNeedle/DroppedNeedle upstream   # for sdk/python
DROPPEDNEEDLE_SOURCE=$PWD/upstream python3.11 -m pytest -q
python3.11 -m ruff check . && python3.11 -m ruff format --check .
```

The tests use fake qBittorrent, Prowlarr and Torznab services on a local port, real
files in a temporary folder, and one test that runs the plugin under the
`droppedneedle_plugin` helper over JSON-RPC, as DroppedNeedle does. CI runs them on
every push and pull request.

AGPL-3.0-only; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
