"""URL parsers for the four job portals.

Each scraped file holds one link per line. The link encodes the ad id, a
title slug and (depending on the portal) the location. Parsers are plain
Python so they can be unit-tested and reused as Spark UDFs.
"""
import re

# /empleos/<title-and-company-slug>-<id>.html
BUMERAN = re.compile(r"^/empleos/(?P<slug>.+)-(?P<ad_id>\d{6,})\.html$")

# /ofertas-de-trabajo/oferta-de-trabajo-de-<title>-en-<location>-<hex id>#lc=...
# The id is 16 hex chars, sometimes followed by a constant 16-char suffix; we keep the first 16.
COMPUTRABAJO = re.compile(
    r"^/ofertas-de-trabajo/oferta-de-trabajo-de-(?P<slug>.+)-(?P<ad_id>[0-9A-F]{16})(?P<suffix>[0-9A-F]{16})?(?:#.*)?$"
)

# /<title>-en-<location>-ID-<id>
BUSCOTRABAJO = re.compile(r"^/(?P<slug>.+)-ID-(?P<ad_id>\d+)$")

# https://www.mipleo.com.pe/ofertas-de-trabajo/oferta-de-trabajo-en-<region>/<title>-<id>.html
MIPLEO = re.compile(
    r"^https?://(?:www\.)?mipleo\.com\.pe/ofertas-de-trabajo/oferta-de-trabajo-en-(?P<region>[a-z0-9-]+)"
    r"/(?P<slug>.+)-(?P<ad_id>\d{5,})\.html$"
)

PORTALS = ("bumeran", "computrabajo", "buscotrabajo", "mipleo")

# Listing/pagination links the scraper also captured; they are not ads.
NAVIGATION = re.compile(r"^(#|/empleos-[a-z0-9-]+\.html(\?.*)?|/ofertas(/\d+)?(\?.*)?)$")


def link_kind(raw):
    """'junk' (empty/placeholder), 'navigation' (listing page) or 'candidate' (should be an ad)."""
    link = (raw or "").strip().strip('"').strip()
    if not link or link == "0":
        return "junk"
    if NAVIGATION.match(link):
        return "navigation"
    return "candidate"


def _split_location(slug):
    """'title-words-en-la-molina' -> ('title words', 'la-molina'), using the last '-en-'."""
    idx = slug.rfind("-en-")
    if idx <= 0:
        return slug, None
    return slug[:idx], slug[idx + 4:]


def _to_text(slug):
    return re.sub(r"\s+", " ", slug.replace("-", " ")).strip() or None


def parse_link(portal, raw):
    """Return a dict with ad_id, title, location_slug, region_slug, or None for junk lines."""
    if raw is None:
        return None
    link = raw.strip().strip('"').strip()
    if not link or link == "0":
        return None

    portal = portal.lower()
    location = region = None

    if portal == "bumeran":
        m = BUMERAN.match(link)
        if not m:
            return None
        title_slug = m["slug"]          # title and company are mixed in the slug
    elif portal == "computrabajo":
        m = COMPUTRABAJO.match(link)
        if not m:
            return None
        title_slug, location = _split_location(m["slug"])
    elif portal == "buscotrabajo":
        m = BUSCOTRABAJO.match(link)
        if not m:
            return None
        title_slug, location = _split_location(m["slug"])
    elif portal == "mipleo":
        m = MIPLEO.match(link)
        if not m:
            return None
        title_slug, region = m["slug"], m["region"]
    else:
        raise ValueError(f"unknown portal: {portal}")

    return {
        "ad_id": m["ad_id"],
        "title": _to_text(title_slug),
        "location_slug": location,
        "region_slug": region,
        "url_path": link.split("#")[0],
    }


FILE_NAME = re.compile(
    r"(?P<y>\d{4})_(?P<m>\d{1,2})_(?P<d>\d{1,2})_(?P<freq>dia|semana)_(?P<portal>[A-Za-z]+)_Links\.csv$"
)


def parse_file_name(name):
    """'2023_11_2_dia_bumeran_Links.csv' -> ('2023-11-02', 'dia', 'bumeran')."""
    m = FILE_NAME.search(name)
    if not m:
        return None
    date = f"{m['y']}-{int(m['m']):02d}-{int(m['d']):02d}"
    return date, m["freq"], m["portal"].lower()
