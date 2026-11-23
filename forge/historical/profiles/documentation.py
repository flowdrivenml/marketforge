"""Published normalization profile documentation links."""

import re

from forge.shared.documentation import SITE_URL

PROFILE_OVERVIEW = SITE_URL + "workflows/catalogue/profiles/"
PROFILE_REFERENCE = SITE_URL + "reference/profiles/"
PROFILE_FIELDS = SITE_URL + "reference/profile-fields/"


def guide_url(profile_type: str) -> str:
    return SITE_URL + f"workflows/catalogue/profile-{profile_type}/"


def field_url(path: str, section: str) -> str:
    if (path.startswith("feeds.<feed>.") and path.count(".") > 2) or (
        ".rules[]." in path and not path.endswith(".rules[].target")
    ):
        # The generated index explains repeated structures at section level.
        anchor = section
    elif path == section:
        # The section heading already owns the unsuffixed slug.
        anchor = f"{section}_1"
    else:
        anchor = re.sub(r"[^a-z0-9_]", "", path)
    return f"{PROFILE_FIELDS}#{anchor}"
