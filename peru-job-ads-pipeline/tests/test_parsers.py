import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from jobads.parsers import link_kind, parse_file_name, parse_link  # noqa: E402


def test_computrabajo_long_and_short_ids_match():
    long_ = ("/ofertas-de-trabajo/oferta-de-trabajo-de-supervisor-de-marketing-en-santiago-de-surco-"
             "A8CA28F9363686ED61373E686DCF3405#lc=ListOffers-Score-2")
    short = "/ofertas-de-trabajo/oferta-de-trabajo-de-supervisor-de-marketing-en-santiago-de-surco-A8CA28F9363686ED"
    a, b = parse_link("computrabajo", long_), parse_link("computrabajo", short)
    assert a["ad_id"] == b["ad_id"] == "A8CA28F9363686ED"
    assert a["title"] == "supervisor de marketing"
    assert a["location_slug"] == "santiago-de-surco"
    assert "#" not in a["url_path"]


def test_location_uses_last_en():
    r = parse_link("computrabajo", "/ofertas-de-trabajo/oferta-de-trabajo-de-contador-en-empresas-en-la-molina-"
                                   "222F27F08E1186C3")
    assert r["title"] == "contador en empresas"
    assert r["location_slug"] == "la-molina"


def test_bumeran():
    r = parse_link("bumeran", "/empleos/help-desk-csti-corp-1115857027.html")
    assert r["ad_id"] == "1115857027"
    assert r["title"] == "help desk csti corp"
    assert r["location_slug"] is None


def test_buscotrabajo():
    r = parse_link("buscotrabajo", "/se-busca-nana-ninera-en-arequipa-ID-10512881")
    assert r["ad_id"] == "10512881"
    assert r["title"] == "se busca nana ninera"
    assert r["location_slug"] == "arequipa"


def test_mipleo():
    r = parse_link("mipleo", "https://www.mipleo.com.pe/ofertas-de-trabajo/oferta-de-trabajo-en-ancash/"
                             "auxiliar-de-decoracion-tiempo-completo-chimbote-retail-5144705.html")
    assert r["ad_id"] == "5144705"
    assert r["region_slug"] == "ancash"


def test_non_ads():
    assert link_kind('""') == "junk"
    assert link_kind("0") == "junk"
    assert link_kind("/empleos-publicacion-menor-a-15-dias.html?page=2") == "navigation"
    assert link_kind("/ofertas/3?fechainicio=0") == "navigation"
    assert parse_link("bumeran", '""') is None


def test_file_names():
    assert parse_file_name("2023_06_11_semana_bumeran_Links.csv") == ("2023-06-11", "semana", "bumeran")
    assert parse_file_name("2023_11_2_dia_Computrabajo_Links.csv") == ("2023-11-02", "dia", "computrabajo")
    assert parse_file_name("notes.csv") is None
