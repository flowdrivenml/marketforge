"""Check local links and fragments in the built Zensical site."""

import tomllib
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Page(HTMLParser):
    def __init__(self, path: Path):
        super().__init__()
        self.anchors = set()
        self.links = []
        self.feed(path.read_text(encoding="utf-8"))

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if attributes.get("id"):
            self.anchors.add(attributes["id"])
        for key in ("href", "src"):
            if attributes.get(key):
                self.links.append(attributes[key])


def main():
    root = Path(__file__).resolve().parent
    with (root / "zensical.toml").open("rb") as config:
        project = tomllib.load(config)["project"]
    site = root / project["site_dir"]
    prefix = urlsplit(project["site_url"]).path.rstrip("/")
    pages = {path: Page(path) for path in site.rglob("*.html")}
    if not pages:
        raise SystemExit("Build the documentation site first.")
    problems = set()
    for source, page in pages.items():
        for link in page.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            path = unquote(url.path)
            if prefix and (path == prefix or path.startswith(prefix + "/")):
                path = "/" + path.removeprefix(prefix).lstrip("/")
            target = (
                (
                    site / path.lstrip("/")
                    if path.startswith("/")
                    else source.parent / path
                )
                if path
                else source
            ).resolve()
            if target.is_dir():
                target /= "index.html"
            fragment = unquote(url.fragment)
            if not target.is_file() or (
                fragment and target in pages and fragment not in pages[target].anchors
            ):
                problems.add(f"{source.relative_to(site)} -> {link}")
    if problems:
        raise SystemExit("\n".join(sorted(problems)))
    print(f"Checked local links, assets and fragments across {len(pages)} HTML pages.")


if __name__ == "__main__":
    main()
