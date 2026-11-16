"""Capture PNG screenshots of the HTML report (requires `playwright install chromium`)."""
from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parents[1]
REPORT = HERE / "report" / "abinitio_to_pyspark_demo.html"
OUT = HERE / "docs" / "screenshots"
SECTIONS = {  # file name -> 1-based <section> index
    "02_graph_vs_dag.png": 1,
    "03_component_mapping.png": 2,
    "04_code_side_by_side.png": 3,
    "05_before_after_data.png": 4,
    "06_reconciliation.png": 5,
    "07_findings.png": 6,
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=1.5)
        page.goto(REPORT.as_uri())
        page.screenshot(path=str(OUT / "01_overview.png"))
        for name, idx in SECTIONS.items():
            page.locator("main > section").nth(idx - 1).screenshot(path=str(OUT / name))
        page.screenshot(path=str(OUT / "00_full_page.png"), full_page=True)
        browser.close()
    for f in sorted(OUT.glob("*.png")):
        print(f.relative_to(HERE))


if __name__ == "__main__":
    sys.exit(main())
