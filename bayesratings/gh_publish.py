"""Upload changed Parquet files to GitHub Releases on sportsleader-data.

Adapted from bayesratings/gh_publish.py. One release per league and data set,
so each season file keeps a stable URL. A changed file is deleted and uploaded
again under the same name. Other season files on that release are left alone,
and this module does not rewrite README.md.

Auth is GITHUB_TOKEN (contents: write), which GitHub Actions provides.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

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
