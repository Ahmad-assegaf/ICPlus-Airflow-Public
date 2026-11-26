"""DAG Airflow 3.1.1 untuk AI Data Platform News Pipeline -> Neon DB (public.alfin_berita_ai).

Konversi dari DAG lama (BashOperator + file JSON lokal) menjadi:
- Airflow 3.1.1 TaskFlow API (@task) dengan alur 5 task identik:
  [scrape_cnbc_tech, scrape_detik_inet, scrape_therundown_ai]
      >> ai_enrichment_filter >> load_to_postgres
- 100% in-memory via XCom (tanpa menulis file JSON ke disk worker Celery).
- Zero dependensi tambahan di luar image standar apache/airflow:3.1.1
  (tanpa beautifulsoup4, pandas, maupun python-dotenv).

Konfigurasi di UI intern-airflow (Admin -> Variables):
- Key   : NEWS_PIPELINE_DB_URL
- Value : postgresql+psycopg2://<user>:<pass>@<host>/<db>?sslmode=require
(Fallback otomatis ke env var NEWS_PIPELINE_DB_URL / DATABASE_URL / POSTGRES_*.)
"""

from __future__ import annotations

import os
import random
import re
import time
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

from airflow.sdk import DAG, Variable, task

JAKARTA = ZoneInfo("Asia/Jakarta")
TARGET_BULAN_MUNDUR = 5

KAMUS_TOPIK = {
    "Technology": [
        "ai", "artificial intelligence", "kecerdasan buatan", "machine learning",
        "algoritma", "model bahasa", "llm", "neural network", "deep learning",
    ],
    "Infrastructure": [
        "data center", "pusat data", "cloud", "server", "gpu", "nvidia",
        "komputasi", "jaringan", "infrastruktur", "compute", "chips", "semiconductor",
    ],
    "Energy": [
        "air", "listrik", "pln", "energi", "karbon", "pembangkit", "jatiluhur",
        "ramah lingkungan", "emisi", "energy", "power", "nuclear", "grid", "emission",
    ],
    "Investment": [
        "investasi", "triliun", "miliar", "dana", "saham", "startup", "akuisisi",
        "proyek", "investor", "ekonomi", "investment", "funding", "billion",
        "million", "acquisition", "valuation",
    ],
    "Generative AI & LLM": [
        "llm", "large language model", "genai", "generative ai", "chatgpt",
        "gemini", "claude", "llama", "hugging face", "mistral", "prompt",
    ],
    "AI Hardware & Chips": [
        "gpu", "nvidia", "amd", "tpu", "chip", "semiconductor",
        "tensor", "silicon", "perangkat keras ai",
    ],
    "Data Center & Cloud": [
        "data center", "pusat data", "aws", "azure", "google cloud",
        "cloud computing", "komputasi awan", "server", "hosting",
    ],
    "AI Regulation & Ethics": [
        "regulasi", "undang-undang", "etika", "bias ai", "hak cipta",
        "legalitas", "kebijakan", "keamanan data", "privasi", "reputasi",
    ],
    "Cyber Security in AI": [
        "ransomware", "hacker", "peretas", "malware", "phishing",
        "kebocoran data", "serangan siber", "cyber threat",
    ],
}

KAMUS_LISENSI = {
    "Open Source": [
        "open source", "llama", "hugging face", "mistral", "sumber terbuka",
        "gratis", "komunitas", "open-source", "open weights",
    ],
    "Komersil": [
        "chatgpt", "openai", "gemini", "claude", "berbayar", "langganan",
        "lisensi", "microsoft", "aws", "google cloud", "anthropic", "enterprise",
    ],
}

UPSERT_NEWS_SQL = """
    INSERT INTO public.alfin_berita_ai (
        judul,
        tanggal,
        link,
        gambar,
        isi_berita,
        sumber,
        kategori_bisnis,
        ringkasan_ai,
        teknologi_dibahas,
        kategori_lisensi,
        skor_relevansi_it,
        waktu_scrape
    ) VALUES (
        %(judul)s,
        %(tanggal)s,
        %(link)s,
        %(gambar)s,
        %(isi_berita)s,
        %(sumber)s,
        %(kategori_bisnis)s,
        %(ringkasan_ai)s,
        CAST(%(teknologi_dibahas)s AS text[]),
        %(kategori_lisensi)s,
        %(skor_relevansi_it)s,
        %(waktu_scrape)s
    )
    ON CONFLICT (link) DO UPDATE SET
        judul = EXCLUDED.judul,
        tanggal = EXCLUDED.tanggal,
        gambar = EXCLUDED.gambar,
        isi_berita = EXCLUDED.isi_berita,
        sumber = EXCLUDED.sumber,
        kategori_bisnis = EXCLUDED.kategori_bisnis,
        ringkasan_ai = EXCLUDED.ringkasan_ai,
        teknologi_dibahas = EXCLUDED.teknologi_dibahas,
        kategori_lisensi = EXCLUDED.kategori_lisensi,
        skor_relevansi_it = EXCLUDED.skor_relevansi_it,
        waktu_scrape = EXCLUDED.waktu_scrape
"""


# ---------------------------------------------------------------------------
# Mini HTML DOM Parser (Pure Python Stdlib, Pengganti BeautifulSoup)
# ---------------------------------------------------------------------------
class _MiniNode:
    __slots__ = ("tag", "attrs", "contents")

    def __init__(self, tag: str, attrs: dict[str, str]) -> None:
        self.tag = tag
        self.attrs = attrs
        self.contents: list = []

    def get(self, key: str, default=None):
        return self.attrs.get(key, default)

    @property
    def text(self) -> str:
        out: list[str] = []

        def _walk(node: _MiniNode) -> None:
            for child in node.contents:
                if isinstance(child, str):
                    out.append(child)
                elif child.tag not in ("script", "style"):
                    _walk(child)

        _walk(self)
        return "".join(out)

    def _matches(self, name=None, attrs=None, class_=None, href=None, string=None) -> bool:
        if name is not None:
            if isinstance(name, (list, tuple, set)):
                if self.tag not in name:
                    return False
            elif self.tag != name:
                return False
        if href is True and "href" not in self.attrs:
            return False
        if class_ is not None:
            cls_val = self.attrs.get("class", "")
            cls_tokens = cls_val.split()
            if isinstance(class_, re.Pattern):
                if not any(class_.search(t) for t in cls_tokens) and not class_.search(cls_val):
                    return False
            elif isinstance(class_, str):
                if class_ not in cls_tokens:
                    return False
        if attrs:
            for k, v in attrs.items():
                actual = self.attrs.get(k)
                if actual is None:
                    return False
                if isinstance(v, re.Pattern):
                    if not v.search(actual):
                        return False
                elif actual != v:
                    return False
        if string is not None:
            t = self.text
            if isinstance(string, re.Pattern):
                if not string.search(t):
                    return False
            elif string not in t:
                return False
        return True

    def find_all(self, name=None, attrs=None, class_=None, href=None, string=None) -> list[_MiniNode]:
        results: list[_MiniNode] = []

        def _walk(node: _MiniNode) -> None:
            for child in node.contents:
                if isinstance(child, _MiniNode):
                    if child._matches(name=name, attrs=attrs, class_=class_, href=href, string=string):
                        results.append(child)
                    _walk(child)

        _walk(self)
        return results

    def find(self, name=None, attrs=None, class_=None, href=None, string=None) -> _MiniNode | None:
        def _walk(node: _MiniNode) -> _MiniNode | None:
            for child in node.contents:
                if isinstance(child, _MiniNode):
                    if child._matches(name=name, attrs=attrs, class_=class_, href=href, string=string):
                        return child
                    found = _walk(child)
                    if found is not None:
                        return found
            return None

        return _walk(self)


class _MiniDOMParser(HTMLParser):
    VOID_TAGS = {
        "area", "base", "br", "col", "embed", "hr", "img",
        "input", "link", "meta", "param", "source", "track", "wbr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _MiniNode("[document]", {})
        self.stack: list[_MiniNode] = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = {k.lower(): (v if v is not None else "") for k, v in attrs}
        node = _MiniNode(tag, attr_map)
        self.stack[-1].contents.append(node)
        if tag not in self.VOID_TAGS:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = {k.lower(): (v if v is not None else "") for k, v in attrs}
        self.stack[-1].contents.append(_MiniNode(tag, attr_map))

    def handle_data(self, data: str) -> None:
        self.stack[-1].contents.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in self.VOID_TAGS:
            return
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                break


def _parse_html(html_text: str) -> _MiniNode:
    parser = _MiniDOMParser()
    parser.feed(html_text)
    parser.close()
    return parser.root


# ---------------------------------------------------------------------------
# Helper Koneksi Neon DB (Airflow Variable -> Env Fallback)
# ---------------------------------------------------------------------------
def _resolve_db_url():
    from sqlalchemy.engine import URL

    for var_name in ("NEWS_PIPELINE_DB_URL", "SECTOR_GROWTH_DB_URL"):
        try:
            val = Variable.get(var_name, default=None)
            if val:
                val = val.strip()
                if val.startswith("postgres://"):
                    return "postgresql+psycopg2://" + val[len("postgres://"):]
                if val.startswith("postgresql://"):
                    return "postgresql+psycopg2://" + val[len("postgresql://"):]
                return val
        except Exception:
            pass

    for env_name in ("NEWS_PIPELINE_DB_URL", "DATABASE_URL"):
        val = os.getenv(env_name)
        if val:
            val = val.strip()
            if val.startswith("postgres://"):
                return "postgresql+psycopg2://" + val[len("postgres://"):]
            if val.startswith("postgresql://"):
                return "postgresql+psycopg2://" + val[len("postgresql://"):]
            return val

    db_host = os.getenv("POSTGRES_HOST")
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    if all([db_host, db_name, db_user, db_pass]):
        return URL.create(
            drivername="postgresql+psycopg2",
            username=db_user,
            password=db_pass,
            host=db_host,
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=db_name,
            query={"sslmode": os.getenv("POSTGRES_SSLMODE", "require")},
        )

    raise ValueError(
        "Airflow Variable 'NEWS_PIPELINE_DB_URL' (atau env DATABASE_URL / POSTGRES_*) belum diatur!"
    )


def _get_engine_with_retry(max_retries: int = 5):
    from sqlalchemy import create_engine, text

    engine = create_engine(
        _resolve_db_url(),
        connect_args={"connect_timeout": 20},
        pool_pre_ping=True,
    )
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Mencoba koneksi ke Neon DB ({attempt}/{max_retries})...")
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Koneksi Neon berhasil!")
            return engine
        except Exception as exc:
            print(f"Koneksi gagal: {exc}")
            if attempt == max_retries:
                engine.dispose()
                raise
            wait_sec = 10 * attempt
            print(f"Mencoba kembali dalam {wait_sec} detik...")
            time.sleep(wait_sec)


def _get_existing_links(sumber: str) -> set[str]:
    from sqlalchemy import text

    engine = _get_engine_with_retry()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT link
                    FROM public.alfin_berita_ai
                    WHERE sumber = :sumber
                      AND link IS NOT NULL
                    """
                ),
                {"sumber": sumber},
            ).fetchall()
        links = {row[0] for row in rows}
        print(f"Ditemukan {len(links)} artikel {sumber} lama di Neon DB.")
        return links
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Helper Teks, Tanggal, & Rule-Based NLP Enrichment
# ---------------------------------------------------------------------------
def _bersihkan_teks_id(teks: str) -> str:
    teks = re.sub(r"(?i)baca juga:?.*?(?=\n|$)", "", teks)
    teks = re.sub(r"(?i)scroll to (?:resume|continue with) content", "", teks)
    teks = re.sub(r"(?i)simak video:?.*?(?=\n|$)", "", teks)
    teks = re.sub(r"(?i)adverst?isement", "", teks)
    return re.sub(r"\s+", " ", teks).strip()


def _bersihkan_teks_en(teks: str) -> str:
    teks = re.sub(r"(?i)read more", "", teks)
    teks = re.sub(r"(?i)subscribe", "", teks)
    teks = re.sub(r"(?i)share this", "", teks)
    return re.sub(r"\s+", " ", teks).strip()


def _standarisasi_tanggal_id(tgl_str: str | None) -> tuple[datetime, str]:
    kamus_bulan = {
        "jan": 1, "januari": 1, "feb": 2, "februari": 2,
        "mar": 3, "maret": 3, "apr": 4, "april": 4, "mei": 5,
        "jun": 6, "juni": 6, "jul": 7, "juli": 7,
        "agu": 8, "agustus": 8, "sep": 9, "september": 9,
        "okt": 10, "oktober": 10, "nov": 11, "november": 11,
        "des": 12, "desember": 12,
    }
    sekarang = datetime.now()
    if not tgl_str:
        return sekarang, sekarang.strftime("%Y-%m-%d")

    tgl_str = tgl_str.lower().strip()
    pola_meta = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", tgl_str)
    if pola_meta:
        try:
            dt = datetime(int(pola_meta.group(1)), int(pola_meta.group(2)), int(pola_meta.group(3)))
            return dt, dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    pola_teks = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", tgl_str)
    if pola_teks:
        hari, kata_bulan, tahun = int(pola_teks.group(1)), pola_teks.group(2), int(pola_teks.group(3))
        angka_bulan = kamus_bulan.get(kata_bulan) or kamus_bulan.get(kata_bulan[:3], 1)
        try:
            dt = datetime(tahun, angka_bulan, hari)
            return dt, dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return sekarang, sekarang.strftime("%Y-%m-%d")


def _standarisasi_tanggal_en(tgl_str: str | None) -> tuple[datetime, str]:
    kamus_bulan = {
        "jan": 1, "january": 1, "feb": 2, "february": 2,
        "mar": 3, "march": 3, "apr": 4, "april": 4, "may": 5,
        "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    sekarang = datetime.now()
    if not tgl_str:
        return sekarang, sekarang.strftime("%Y-%m-%d")

    tgl_str = str(tgl_str).lower().strip()
    pola_teks = re.search(
        r"([a-z]+)\s+(\d{1,2}),?\s+(\d{4})|(\d{1,2})\s+([a-z]+)\s+(\d{4})",
        tgl_str,
    )
    if pola_teks:
        if pola_teks.group(1):
            kata_bulan, hari, tahun = pola_teks.group(1), int(pola_teks.group(2)), int(pola_teks.group(3))
        else:
            hari, kata_bulan, tahun = int(pola_teks.group(4)), pola_teks.group(5), int(pola_teks.group(6))
        angka_bulan = kamus_bulan.get(kata_bulan) or kamus_bulan.get(kata_bulan[:3], 1)
        try:
            dt = datetime(tahun, angka_bulan, hari)
            return dt, dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    pola_iso = re.search(r"(\d{4})-(\d{2})-(\d{2})", tgl_str)
    if pola_iso:
        try:
            dt = datetime(int(pola_iso.group(1)), int(pola_iso.group(2)), int(pola_iso.group(3)))
            return dt, dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return sekarang, sekarang.strftime("%Y-%m-%d")


def _parse_iso_date(val: str | None) -> date | None:
    if not val:
        return None
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", str(val))
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _proses_rule_based_nlp(isi_teks: str) -> dict:
    if not isi_teks:
        return {"topik_dibahas": [], "kategori_lisensi": "Tidak Diketahui", "skor_relevansi_it": 0}

    teks_lower = str(isi_teks).lower()
    topik_ditemukan = [
        topik for topik, kata_kunci in KAMUS_TOPIK.items()
        if any(k in teks_lower for k in kata_kunci)
    ]

    skor_open = sum(teks_lower.count(k) for k in KAMUS_LISENSI["Open Source"])
    skor_komersil = sum(teks_lower.count(k) for k in KAMUS_LISENSI["Komersil"])

    if skor_open > skor_komersil:
        lisensi = "Open Source"
    elif skor_komersil > skor_open:
        lisensi = "Komersil"
    elif skor_open > 0:
        lisensi = "Campuran"
    else:
        lisensi = "Tidak Diketahui"

    skor = sum(teks_lower.count(k) for k in KAMUS_TOPIK["Infrastructure"]) * 10
    skor += sum(teks_lower.count(k) for k in KAMUS_TOPIK["Technology"]) * 5
    skor = min(skor, 100)

    return {
        "topik_dibahas": topik_ditemukan,
        "kategori_lisensi": lisensi,
        "skor_relevansi_it": skor,
    }


# ---------------------------------------------------------------------------
# Deklarasi DAG Airflow 3.1.1
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ai_data_platform_pipeline",
    description="AI Data Platform News ETL Pipeline (CNBC, Detik, The Rundown AI -> Neon DB).",
    start_date=datetime(2026, 1, 1, tzinfo=JAKARTA),
    schedule="0 22 * * 0",  # Setiap Minggu jam 22:00 WIB
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "ai_data_platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["ai_data_platform", "news", "finops"],
):

    @task
    def scrape_cnbc_tech() -> list[dict]:
        import requests

        batas_kadaluarsa = datetime.now() - timedelta(days=TARGET_BULAN_MUNDUR * 30)
        existing_links = _get_existing_links("CNBC Tech")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
            )
        }

        semua_berita: list[dict] = []
        seen_links: set[str] = set()
        halaman = 1
        halaman_maksimal = 100
        pencarian_selesai = False

        while halaman <= halaman_maksimal and not pencarian_selesai:
            url = (
                "https://www.cnbcindonesia.com/tag/artificial-intelligence"
                if halaman == 1
                else f"https://www.cnbcindonesia.com/tag/artificial-intelligence/{halaman}"
            )
            print(f"[CNBC] Memeriksa halaman {halaman}...")
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            dom = _parse_html(res.text)

            list_artikel = dom.find_all("article")
            if not list_artikel:
                list_artikel = [li for li in dom.find_all("li") if li.find("h2")]
            if not list_artikel:
                break

            kadaluarsa_cnt = 0
            sudah_ada_cnt = 0

            for artikel in list_artikel:
                el_judul = artikel.find("h2")
                el_link = artikel.find("a")
                el_tgl = artikel.find(class_=re.compile(r"(date|label)"))
                if not (el_judul and el_link):
                    continue

                link = el_link.get("href")
                if not link:
                    continue
                judul = el_judul.text.strip()
                tgl_mentah = el_tgl.text.strip() if el_tgl else ""

                if link in existing_links:
                    sudah_ada_cnt += 1
                    if sudah_ada_cnt >= 3:
                        print(f"[CNBC] Artikel lama ditemukan di halaman {halaman}. Stop.")
                        pencarian_selesai = True
                        break
                    continue

                obj_waktu, tgl_seragam = _standarisasi_tanggal_id(tgl_mentah)
                if obj_waktu < batas_kadaluarsa:
                    kadaluarsa_cnt += 1
                    if kadaluarsa_cnt >= 5:
                        pencarian_selesai = True
                        break
                    continue

                if link not in seen_links:
                    seen_links.add(link)
                    semua_berita.append({"judul": judul, "link": link, "tanggal_seragam": tgl_seragam})

            halaman += 1
            time.sleep(1)

        print(f"[CNBC] Ditemukan {len(semua_berita)} kandidat baru. Mulai deep scrape...")
        berita_final: list[dict] = []

        for idx, berita in enumerate(semua_berita, 1):
            print(f"[CNBC {idx}/{len(semua_berita)}] {berita['judul'][:45]}...")
            isi = "Gagal mengekstrak isi teks."
            gambar = "https://via.placeholder.com/600x400?text=Tidak+Ada+Gambar"
            tgl_akurat = None

            try:
                r = requests.get(berita["link"], headers=headers, timeout=15)
                r.raise_for_status()
                d = _parse_html(r.text)

                div_isi = (
                    d.find("div", class_="detail_text")
                    or d.find("div", class_="detail_wrap")
                    or d.find("article")
                )
                if div_isi:
                    paras = [p.text.strip() for p in div_isi.find_all("p") if p.text.strip()]
                    if paras:
                        isi = _bersihkan_teks_id(" ".join(paras))

                meta_img = d.find("meta", attrs={"property": "og:image"})
                if meta_img and meta_img.get("content"):
                    img_url = meta_img.get("content")
                    if img_url.startswith("http") and len(img_url) > 10:
                        gambar = img_url

                meta_date = (
                    d.find("meta", attrs={"name": "publishdate"})
                    or d.find("meta", attrs={"property": "article:published_time"})
                )
                if meta_date and meta_date.get("content"):
                    _, tgl_akurat = _standarisasi_tanggal_id(meta_date.get("content"))
                else:
                    el_date = d.find(class_="date")
                    if el_date:
                        _, tgl_akurat = _standarisasi_tanggal_id(el_date.text.strip())
            except Exception as exc:
                print(f"[CNBC WARN] Gagal detail {berita['link']}: {exc}")

            berita_final.append(
                {
                    "judul": berita["judul"],
                    "tanggal": tgl_akurat or berita["tanggal_seragam"],
                    "link": berita["link"],
                    "gambar": gambar,
                    "isi_berita": isi,
                    "sumber": "CNBC Tech",
                    "kategori_bisnis": "AI & Data Infrastruktur",
                }
            )
            time.sleep(1)

        print(f"[CNBC] Selesai: {len(berita_final)} artikel baru.")
        return berita_final

    @task
    def scrape_detik_inet() -> list[dict]:
        import requests

        batas_kadaluarsa = datetime.now() - timedelta(days=TARGET_BULAN_MUNDUR * 30)
        existing_links = _get_existing_links("Detik Inet")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko)"
            )
        }

        semua_berita: list[dict] = []
        seen_links: set[str] = set()
        halaman = 1
        halaman_maksimal = 100
        pencarian_selesai = False

        while halaman <= halaman_maksimal and not pencarian_selesai:
            url = f"https://www.detik.com/tag/artificial-intelligence/?sortby=time&page={halaman}"
            print(f"[Detik] Memeriksa halaman {halaman}...")
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            dom = _parse_html(res.text)

            list_artikel = dom.find_all("article")
            if not list_artikel:
                break

            kadaluarsa_cnt = 0
            sudah_ada_cnt = 0

            for artikel in list_artikel:
                a_tag = artikel.find("a", href=True)
                judul_el = artikel.find("h2") or artikel.find("h3")
                if not (a_tag and judul_el):
                    continue

                link = a_tag.get("href")
                judul = judul_el.text.strip()

                if link in existing_links:
                    sudah_ada_cnt += 1
                    if sudah_ada_cnt >= 3:
                        print(f"[Detik] Artikel lama ditemukan di halaman {halaman}. Stop.")
                        pencarian_selesai = True
                        break
                    continue

                el_tgl = artikel.find(class_=re.compile("date"))
                if el_tgl:
                    obj_waktu, _ = _standarisasi_tanggal_id(el_tgl.text.strip())
                    if obj_waktu < batas_kadaluarsa:
                        kadaluarsa_cnt += 1
                        if kadaluarsa_cnt >= 5:
                            pencarian_selesai = True
                            break
                        continue

                if link not in seen_links:
                    seen_links.add(link)
                    semua_berita.append({"judul": judul, "link": link})

            halaman += 1
            time.sleep(1)

        print(f"[Detik] Ditemukan {len(semua_berita)} kandidat baru. Mulai deep scrape...")
        berita_final: list[dict] = []

        for idx, berita in enumerate(semua_berita, 1):
            print(f"[Detik {idx}/{len(semua_berita)}] {berita['judul'][:45]}...")
            isi = "Gagal mengekstrak isi teks."
            tgl_mentah = ""
            gambar = "https://via.placeholder.com/600x400?text=No+Image"

            try:
                r = requests.get(berita["link"], headers=headers, timeout=15)
                d = _parse_html(r.text)

                div_isi = d.find("div", class_="detail__body-text")
                if div_isi:
                    paras = [p.text.strip() for p in div_isi.find_all("p") if p.text.strip()]
                    if paras:
                        isi = _bersihkan_teks_id(" ".join(paras))

                el_date = d.find("div", class_="detail__date")
                if el_date:
                    tgl_mentah = el_date.text.strip()
                else:
                    meta_date = d.find("meta", attrs={"name": "publishdate"})
                    if meta_date:
                        tgl_mentah = meta_date.get("content", "")

                meta_img = d.find("meta", attrs={"property": "og:image"})
                if meta_img and meta_img.get("content"):
                    gambar = meta_img.get("content")
            except Exception as exc:
                print(f"[Detik WARN] Gagal detail {berita['link']}: {exc}")

            obj_waktu, tgl_seragam = _standarisasi_tanggal_id(tgl_mentah)
            if obj_waktu < batas_kadaluarsa:
                continue

            berita_final.append(
                {
                    "judul": berita["judul"],
                    "tanggal": tgl_seragam,
                    "link": berita["link"],
                    "gambar": gambar,
                    "isi_berita": isi,
                    "sumber": "Detik Inet",
                    "kategori_bisnis": "AI & Data Infrastruktur",
                }
            )
            time.sleep(1)

        print(f"[Detik] Selesai: {len(berita_final)} artikel baru.")
        return berita_final

    @task
    def scrape_therundown_ai() -> list[dict]:
        import requests

        batas_kadaluarsa = datetime.now() - timedelta(days=TARGET_BULAN_MUNDUR * 30)
        existing_links = _get_existing_links("The Rundown AI")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

        semua_berita: list[dict] = []
        seen_links: set[str] = set()
        halaman = 1
        halaman_maksimal = 50
        pencarian_selesai = False

        while halaman <= halaman_maksimal and not pencarian_selesai:
            url = (
                "https://www.therundown.ai/articles-category/ai"
                if halaman == 1
                else f"https://www.therundown.ai/articles-category/ai?page={halaman}"
            )
            print(f"[Rundown] Memeriksa halaman {halaman}...")
            try:
                res = requests.get(url, headers=headers, timeout=15)
                if res.status_code != 200:
                    print(f"[Rundown WARN] Status {res.status_code} di halaman {halaman}. Stop.")
                    break

                dom = _parse_html(res.text)
                kartu_artikel = dom.find_all("a", href=True)
                if not kartu_artikel:
                    break

                ada_valid = False
                sudah_ada_cnt = 0

                for kartu in kartu_artikel:
                    judul_el = kartu.find(["h2", "h3"])
                    if not judul_el:
                        continue
                    judul = judul_el.text.strip()
                    link = kartu.get("href", "")
                    if not link.startswith("http"):
                        link = f"https://www.therundown.ai{link}"

                    if len(judul) > 15 and "/articles/" in link:
                        ada_valid = True
                        if link in existing_links:
                            sudah_ada_cnt += 1
                        elif link not in seen_links:
                            seen_links.add(link)
                            semua_berita.append({"judul": judul, "link": link})

                if sudah_ada_cnt >= 3:
                    print(f"[Rundown] Artikel lama ditemukan di halaman {halaman}. Stop.")
                    pencarian_selesai = True
                    break
                if not ada_valid:
                    print(f"[Rundown] Tidak ada artikel valid di halaman {halaman}. Stop.")
                    break

                halaman += 1
                time.sleep(random.uniform(3, 7))
            except Exception as exc:
                print(f"[Rundown WARN] Error halaman {halaman}: {exc}")
                break

        print(f"[Rundown] Ditemukan {len(semua_berita)} kandidat baru. Mulai deep scrape...")
        berita_final: list[dict] = []

        for idx, berita in enumerate(semua_berita, 1):
            print(f"[Rundown {idx}/{len(semua_berita)}] {berita['judul'][:45]}...")
            isi = "Gagal mengekstrak isi teks."
            gambar = "https://via.placeholder.com/600x400?text=The+Rundown+AI"
            tgl_akurat = None

            try:
                r = requests.get(berita["link"], headers=headers, timeout=15)
                d = _parse_html(r.text)

                div_isi = d.find("article") or d.find("div", class_=re.compile(r"(content|prose|body)"))
                if div_isi:
                    paras = [p.text.strip() for p in div_isi.find_all(["p", "li"]) if p.text.strip()]
                    if paras:
                        isi = _bersihkan_teks_en(" ".join(paras))

                meta_img = d.find("meta", attrs={"property": "og:image"})
                if meta_img and meta_img.get("content"):
                    gambar = meta_img.get("content")

                meta_date = (
                    d.find("meta", attrs={"property": "article:published_time"})
                    or d.find("meta", attrs={"name": "publishdate"})
                )
                if meta_date and meta_date.get("content"):
                    _, tgl_akurat = _standarisasi_tanggal_en(meta_date.get("content"))
                else:
                    tgl_el = d.find("time") or d.find("span", string=re.compile(r"202\d"))
                    if tgl_el:
                        _, tgl_akurat = _standarisasi_tanggal_en(tgl_el.text)
            except Exception as exc:
                print(f"[Rundown WARN] Gagal detail {berita['link']}: {exc}")

            tgl_final = tgl_akurat or datetime.now().strftime("%Y-%m-%d")
            obj_waktu, _ = _standarisasi_tanggal_en(tgl_final)
            if obj_waktu < batas_kadaluarsa:
                break

            berita_final.append(
                {
                    "judul": berita["judul"],
                    "tanggal": tgl_final,
                    "link": berita["link"],
                    "gambar": gambar,
                    "isi_berita": isi,
                    "sumber": "The Rundown AI",
                    "kategori_bisnis": "AI & Data Infrastruktur",
                }
            )
            time.sleep(random.uniform(2, 5))

        print(f"[Rundown] Selesai: {len(berita_final)} artikel baru.")
        return berita_final

    @task
    def ai_enrichment_filter(
        cnbc_rows: list[dict],
        detik_rows: list[dict],
        rundown_rows: list[dict],
    ) -> list[dict]:
        berita_gabungan = (cnbc_rows or []) + (detik_rows or []) + (rundown_rows or [])
        if not berita_gabungan:
            print("Tidak ada artikel baru dari ketiga sumber.")
            return []

        seen: set[str] = set()
        berita_lolos: list[dict] = []

        for berita in berita_gabungan:
            link = (berita.get("link") or "").strip()
            if not link or link in seen:
                continue
            seen.add(link)

            isi_teks = berita.get("isi_berita", "") or ""
            ringkasan = (
                isi_teks[:250].rsplit(" ", 1)[0] + "..."
                if len(isi_teks) > 250
                else isi_teks
            )
            hasil = _proses_rule_based_nlp(isi_teks)

            if hasil["skor_relevansi_it"] >= 15:
                item = dict(berita)
                item["ringkasan_ai"] = ringkasan
                item["teknologi_dibahas"] = hasil["topik_dibahas"]
                item["kategori_lisensi"] = hasil["kategori_lisensi"]
                item["skor_relevansi_it"] = hasil["skor_relevansi_it"]
                berita_lolos.append(item)

        print(
            f"Rule-Based NLP selesai: {len(berita_lolos)}/{len(berita_gabungan)} "
            "artikel lolos filter (skor_relevansi_it >= 15)."
        )
        return berita_lolos

    @task
    def load_to_postgres(enriched_rows: list[dict]) -> int:
        from psycopg2.extras import execute_batch

        if not enriched_rows:
            print("Tidak ada artikel lolos filter untuk di-UPSERT ke Neon DB.")
            return 0

        now = datetime.now()
        records: list[dict] = []
        seen_links: set[str] = set()

        for baris in enriched_rows:
            link = (baris.get("link") or "").strip()
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            teknologi = baris.get("teknologi_dibahas") or []
            if isinstance(teknologi, str):
                teknologi = [x.strip() for x in teknologi.split(",") if x.strip()]
            elif not isinstance(teknologi, list):
                teknologi = []

            records.append(
                {
                    "judul": baris.get("judul", ""),
                    "tanggal": _parse_iso_date(baris.get("tanggal")),
                    "link": link,
                    "gambar": baris.get("gambar", ""),
                    "isi_berita": baris.get("isi_berita", ""),
                    "sumber": baris.get("sumber", ""),
                    "kategori_bisnis": baris.get("kategori_bisnis", ""),
                    "ringkasan_ai": baris.get("ringkasan_ai", ""),
                    "teknologi_dibahas": [str(item) for item in teknologi],
                    "kategori_lisensi": baris.get("kategori_lisensi", ""),
                    "skor_relevansi_it": int(baris.get("skor_relevansi_it", 0)),
                    "waktu_scrape": now,
                }
            )

        engine = _get_engine_with_retry()
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                execute_batch(cur, UPSERT_NEWS_SQL, records, page_size=200)
            raw_conn.commit()
            print(f"UPSERT selesai: {len(records)} berita diproses ke public.alfin_berita_ai.")
            return len(records)
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
            engine.dispose()

    # Dependency identik dengan DAG lama: 3 scraper paralel -> enrichment -> load
    cnbc_data = scrape_cnbc_tech()
    detik_data = scrape_detik_inet()
    rundown_data = scrape_therundown_ai()
    enriched = ai_enrichment_filter(cnbc_data, detik_data, rundown_data)
    load_to_postgres(enriched)