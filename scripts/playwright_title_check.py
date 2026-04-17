from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BROWSERS_PATH = ROOT_DIR / ".playwright-browsers"


def main() -> int:
    if len(sys.argv) < 2:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error": "usage: python scripts/playwright_title_check.py <url>",
                }
            )
        )
        return 1

    url = sys.argv[1].strip()
    if not url:
        print(json.dumps({"status": "error", "error": "url must not be empty"}))
        return 1

    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(DEFAULT_BROWSERS_PATH))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        title = page.title()
        final_url = page.url
        browser.close()

    print(
        json.dumps(
            {
                "status": "success",
                "url": url,
                "final_url": final_url,
                "title": title,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
