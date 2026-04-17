from __future__ import annotations

import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BROWSERS_PATH = ROOT_DIR / ".playwright-browsers"
FORM_URL = "https://httpbin.org/forms/post"


def main() -> int:
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(DEFAULT_BROWSERS_PATH))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(FORM_URL, wait_until="domcontentloaded", timeout=30000)
        page.fill('input[name="custname"]', "John Test")
        page.fill('input[name="custemail"]', "test@email.com")
        with page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
            page.locator("form").evaluate(
                "(form) => form.requestSubmit ? form.requestSubmit() : form.submit()"
            )
        confirmation_text = page.locator("body").inner_text()
        final_url = page.url
        browser.close()

    print(
        json.dumps(
            {
                "status": "success",
                "url": FORM_URL,
                "final_url": final_url,
                "confirmation_text": confirmation_text,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
