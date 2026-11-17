"""Upload changed Parquet files to GitHub Releases on sportsleader-data.

Adapted from bayesratings/gh_publish.py. One release per league and data set,
so each season file keeps a stable URL. A changed file is deleted and uploaded
again under the same name. Other season files on that release are left alone,
and this module does not rewrite README.md.

Auth is GITHUB_TOKEN (contents: write), which GitHub Actions provides.
"""

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

OWNER = "WalrusQuant"
REPO = "sportsleader-data"
API = "https://api.github.com"
UPLOAD = "https://uploads.github.com"
TOKEN_ENV = "GITHUB_TOKEN"


class GitHubError(RuntimeError):
    def __init__(self, status, body):
        text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body or "")
        super().__init__(f"GitHub HTTP {status}: {text[:300]}")
        self.status = status


def asset_url(league, dataset, season) -> str:
    return (f"https://github.com/{OWNER}/{REPO}/releases/download/"
            f"{league}_{dataset}/{season}.parquet")


def manifest_url() -> str:
    return f"https://github.com/{OWNER}/{REPO}/releases/download/manifest/manifest.json"


def _token():
    token = os.environ.get(TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(f"{TOKEN_ENV} is not set")
    return token


def _request(method, url, data=None, headers=None, timeout=180):
    base = {
        "Authorization": f"Bearer {_token()}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "sportsleader-data-publish",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if headers:
        base.update(headers)
    req = urllib.request.Request(url, data=data, headers=base, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _json(method, url, payload=None, ok=(200, 201)):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if payload is not None else None
    status, body = _request(method, url, data, headers)
    if status not in ok:
        raise GitHubError(status, body)
    return json.loads(body) if body else None


def _release(tag):
    url = f"{API}/repos/{OWNER}/{REPO}/releases/tags/{urllib.parse.quote(tag)}"
    status, body = _request("GET", url)
    if status == 200:
        return json.loads(body)
    if status != 404:
        raise GitHubError(status, body)
    return _json("POST", f"{API}/repos/{OWNER}/{REPO}/releases", {
        "tag_name": tag,
        "name": tag,
        "body": "Published by the nightly update.",
        "draft": False,
        "prerelease": False,
        "generate_release_notes": False,
        "make_latest": "true" if tag == "manifest" else "false",
    })


def finish(out, status_file, stamp, log=print):
    """export.main hook. The nightly job uploads; this does not."""
    log(f"{stamp} SKIP github: run python -m bayesratings.nightly to publish")
    return 0


def fetch_release_asset(tag, name) -> bytes:
    """Download one release asset through the API. The public download URL
    can lag a write that landed seconds ago; the API read does not."""
    rel = _release(tag)
    assets = list(rel.get("assets") or [])
    if not any(a.get("name") == name for a in assets):
        status, body = _request(
            "GET", f"{API}/repos/{OWNER}/{REPO}/releases/{rel['id']}/assets?per_page=100")
        if status != 200:
            raise GitHubError(status, body)
        assets = json.loads(body)
    asset = next((a for a in assets if a.get("name") == name), None)
    if asset is None:
        raise GitHubError(404, f"no asset {name}".encode())
    status, body = _request(
        "GET", f"{API}/repos/{OWNER}/{REPO}/releases/assets/{asset['id']}",
        headers={"Accept": "application/octet-stream"})
    if status != 200:
        raise GitHubError(status, body)
    return body


def fetch_manifest() -> dict:
    """Latest published manifest. Uses the API when a token is set."""
    if os.environ.get(TOKEN_ENV, "").strip():
        return json.loads(fetch_release_asset("manifest", "manifest.json").decode())
    req = urllib.request.Request(manifest_url(), headers={"User-Agent": "sportsleader-data-publish"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def manifest_fingerprint(manifest) -> str:
    """Identity of the file list. generated_at is not part of it, so two
    merges of the same files compare equal."""
    files = []
    for entry in (manifest or {}).get("files") or []:
        files.append((entry.get("path"), entry.get("sha256"), entry.get("rows")))
    blob = json.dumps(sorted(files), separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


def _file_item(entry) -> dict:
    item = {k: v for k, v in entry.items() if not str(k).startswith("_")}
    if item.get("league") and item.get("dataset") and item.get("season") is not None:
        item["url"] = asset_url(item["league"], item["dataset"], item["season"])
    return item


def merge_manifest(remote, changed, catalog) -> dict:
    """Copy of `remote` with this run's files replaced or added. Every other
    file is kept. `changed` is a list of file entries or a path -> entry map.
    `catalog` is a function (files) -> datasets block, or that block itself."""
    remote = dict(remote or {})
    by_path = {}
    for entry in remote.get("files") or []:
        item = _file_item(entry)
        if item.get("path"):
            by_path[item["path"]] = item
    if isinstance(changed, dict):
        pairs = changed.items()
    else:
        pairs = ((e.get("path"), e) for e in changed or [])
    for path, entry in pairs:
        item = _file_item(entry)
        item["path"] = path
        by_path[path] = item
    files = sorted(by_path.values(), key=lambda f: f.get("path") or "")
    out = dict(remote)
    out["files"] = files
    out["datasets"] = catalog(files) if callable(catalog) else catalog
    out["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out["manifest_url"] = manifest_url()
    return out


def manifest_bytes(manifest) -> bytes:
    return (json.dumps(manifest, indent=1) + "\n").encode()


def upload_manifest(manifest) -> None:
    body = manifest_bytes(manifest)
    replace_asset("manifest", "manifest.json", body,
                  f"sha256:{hashlib.sha256(body).hexdigest()}")


def commit_manifest(changed, catalog, fetch, upload, attempts=6, sleep=time.sleep):
    """Re-read the manifest immediately before writing it, and merge only this
    run's files. If another writer lands in that window, try again. Both
    writers doing this is what keeps a backfill upload that finishes during
    a nightly run. There is no shared job queue."""
    incoming = {}
    for entry in changed or []:
        item = _file_item(entry)
        if not item.get("path"):
            raise ValueError("manifest entry has no path")
        incoming[item["path"]] = item
    last_error = None
    for attempt in range(attempts):
        remote = fetch()
        token = manifest_fingerprint(remote)
        latest = fetch()
        if manifest_fingerprint(latest) != token:
            sleep(0.2 * (attempt + 1))
            continue
        merged = merge_manifest(latest, incoming, catalog)
        upload(merged)
        confirmed = fetch()
        if manifest_fingerprint(confirmed) != manifest_fingerprint(merged):
            last_error = "manifest changed during upload"
            sleep(0.2 * (attempt + 1))
            continue
        have = {f.get("path") for f in confirmed.get("files") or []}
        missing = [p for p in incoming if p not in have]
        if missing:
            last_error = "uploaded files missing from manifest"
            sleep(0.2 * (attempt + 1))
            continue
        return merged
    raise RuntimeError(f"manifest merge did not settle ({last_error or 'conflict'})")


def replace_asset(tag, name, data, label):
    """Replace one asset on a release. Other assets stay."""
    rel = _release(tag)
    status, body = _request("GET", f"{API}/repos/{OWNER}/{REPO}/releases/{rel['id']}/assets?per_page=100")
    if status != 200:
        raise GitHubError(status, body)
    for asset in json.loads(body):
        if asset.get("name") == name:
            dst, raw = _request("DELETE", f"{API}/repos/{OWNER}/{REPO}/releases/assets/{asset['id']}")
            if dst not in (204, 404):
                raise GitHubError(dst, raw)
    qs = urllib.parse.urlencode({"name": name, "label": label})
    url = f"{UPLOAD}/repos/{OWNER}/{REPO}/releases/{rel['id']}/assets?{qs}"
    status, body = _request("POST", url, data, {"Content-Type": "application/octet-stream"})
    if status not in (200, 201):
        raise GitHubError(status, body)
    return json.loads(body)
