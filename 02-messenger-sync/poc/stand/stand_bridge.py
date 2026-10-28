#!/usr/bin/env python3
"""Run the unmodified bridge CLI against the stand mock API (test stand only)."""
import os
import sys
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bridge as b  # noqa: E402

BASE = os.environ['STAND_API_BASE']
MOCK_HOST = urllib.parse.urlsplit(BASE).hostname
RealApi, RealBridge, real_check = b.Api, b.Bridge, b.check_media_url


def stand_api(cfg):
    return RealApi(cfg, tg_base=BASE + '/tg', max_base=BASE + '/max', tg_file_base=BASE + '/tgfile')


def stand_bridge(cfg, api, store, ids):
    return RealBridge(cfg, api, store, ids, send_intervals={'tg': 0.2, 'max': 0.2})


def stand_check(url):
    if urllib.parse.urlsplit(url).hostname != MOCK_HOST:
        real_check(url)


b.Api, b.Bridge, b.check_media_url = stand_api, stand_bridge, stand_check

if __name__ == '__main__':
    try:
        sys.exit(b.main() or 0)
    except (b.BridgeError, FileNotFoundError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
