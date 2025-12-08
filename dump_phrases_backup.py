#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor
from tqdm import tqdm


# =========================
#   ENV / DB CONFIG
# =========================

load_dotenv()

PG_DB       = os.getenv("PG_DB")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")

if not PG_DB or not PG_USER or not PG_PASSWORD:
    print("[ERROR] PG_DB / PG_USER / PG_PASSWORD must be set in .env", file=sys.stderr)
    sys.exit(1)

DSN = (
    f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} "
    f"host={PG_HOST} port={PG_PORT}"
)


def get_conn():
    return psycopg2.connect(DSN, cursor_factory=DictCursor)


# =========================
#   MAIN
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Сохранить резервную выгрузку id–фраза из БД в папку с mp3."
    )
    parser.add_argument(
        "-a", "--audio-dir",
        required=True,
        help="Каталог с mp3-файлами (сюда положим backup-файл).",
    )
    parser.add_argument(
        "-o", "--output",
        help="Имя файла бэкапа (по умолчанию phrases_backup.tsv в audio-dir).",
    )

    args = parser.parse_args()

    audio_dir = Path(args.audio_dir)
    if not audio_dir.exists():
        print(f"[ERROR] audio dir not found: {audio_dir}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = audio_dir / out_path
    else:
        out_path = audio_dir / "phrases_backup.tsv"

    print(f"[INFO] audio dir : {audio_dir}")
    print(f"[INFO] backup to : {out_path}")

    conn = get_conn()

    # Считаем, сколько всего фраз
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM phrases;")
        total = cur.fetchone()[0]

    print(f"[INFO] phrases in DB: {total:,}", file=sys.stderr)

    # Читаем и пишем в TSV
    with conn.cursor() as cur, out_path.open("w", encoding="utf-8") as fout:
        # заголовок
        fout.write("id\tphrase\n")

        cur.execute("SELECT id, phrase FROM phrases ORDER BY id;")

        pbar = tqdm(total=total, desc="dumping", unit="phr")
        rows_written = 0

        for row in cur:
            pid = row["id"]
            phr = row["phrase"] or ""

            # на всякий случай убираем табы/переводы строк внутри фразы
            phr_clean = phr.replace("\t", " ").replace("\r", " ").replace("\n", " ")

            fout.write(f"{pid}\t{phr_clean}\n")
            rows_written += 1
            pbar.update(1)

        pbar.close()

    conn.close()
    print(f"[DONE] written {rows_written:,} rows to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
