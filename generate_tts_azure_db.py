#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path
from time import sleep

from dotenv import load_dotenv
from tqdm import tqdm
import psycopg2
from psycopg2.extras import DictCursor

try:
    import azure.cognitiveservices.speech as speechsdk
except ImportError:
    print("[ERROR] azure-cognitiveservices-speech not installed. Run: pip install azure-cognitiveservices-speech")
    sys.exit(1)


# =========================
#   ENV / DB / AZURE CONFIG
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

AZURE_TTS_KEY    = os.getenv("AZURE_TTS_KEY")
AZURE_TTS_REGION = os.getenv("AZURE_TTS_REGION")

if not AZURE_TTS_KEY or not AZURE_TTS_REGION:
    print("[ERROR] AZURE_TTS_KEY / AZURE_TTS_REGION must be set in .env", file=sys.stderr)
    sys.exit(1)

DSN = (
    f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} "
    f"host={PG_HOST} port={PG_PORT}"
)


def get_conn():
    return psycopg2.connect(DSN, cursor_factory=DictCursor)


def create_speech_config(language: str, voice: str) -> speechsdk.SpeechConfig:
    speech_config = speechsdk.SpeechConfig(
        subscription=AZURE_TTS_KEY,
        region=AZURE_TTS_REGION,
    )
    speech_config.speech_synthesis_language = language
    speech_config.speech_synthesis_voice_name = voice

    # ВАЖНО: без этого Azure отдаёт WAV, а не MP3
    speech_config.set_speech_synthesis_output_format(
        speechsdk.SpeechSynthesisOutputFormat.Audio16Khz128KBitRateMonoMp3
    )

    return speech_config



def synthesize_to_file(
    speech_config: speechsdk.SpeechConfig,
    text: str,
    out_path: Path,
) -> tuple[bool, str | None]:
    """
    Синтезирует text в MP3-файл out_path.
    Возвращает (успех, текст_ошибки_или_None).
    """
    audio_config = speechsdk.audio.AudioOutputConfig(filename=str(out_path))
    synthesizer = speechsdk.SpeechSynthesizer(
        speech_config=speech_config,
        audio_config=audio_config,
    )

    result = synthesizer.speak_text_async(text).get()

    if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
        return True, None

    err = f"TTS failed, reason={result.reason}"
    if result.reason == speechsdk.ResultReason.Canceled:
        cancellation = result.cancellation_details
        err += f", cancel_reason={cancellation.reason}"
        if cancellation.error_details:
            err += f", details={cancellation.error_details}"

    return False, err


# =========================
#   MAIN
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Generate MP3 for phrases from PostgreSQL via Azure TTS (with resume & dry-run)."
    )
    parser.add_argument(
        "-o", "--out-dir",
        required=True,
        help="Каталог для MP3-файлов. Имя файла = {phrase_id:06d}.mp3",
    )
    parser.add_argument(
        "--language",
        default="es-ES",
        help="Язык синтеза (по умолчанию es-ES).",
    )
    parser.add_argument(
        "--voice",
        default="es-ES-AlvaroNeural",
        help="Имя голоса Azure (по умолчанию es-ES-AlvaroNeural).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Размер пачки фраз, читаемых из БД за раз. По умолчанию 100.",
    )
    parser.add_argument(
        "--max-phrases",
        type=int,
        default=0,
        help="Максимум фраз для обработки за запуск (0 = без ограничения). Удобно для частичных прогонов.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=5,
        help="Максимальное число попыток TTS на одну фразу (по tts_attempts).",
    )
    parser.add_argument(
        "--sleep-on-error",
        type=float,
        default=5.0,
        help="Пауза (сек.) после ошибки перед продолжением.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Эмуляция: не вызываем Azure, не создаём файлы и не обновляем БД.",
    )

    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = get_conn()
    conn.autocommit = False

    speech_config = None
    if not args.dry_run:
        speech_config = create_speech_config(args.language, args.voice)

    # Считаем, сколько всего фраз требуют TTS
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM phrases
            WHERE tts_ok = false
              AND tts_attempts < %s
            """,
            (args.max_attempts,),
        )
        total_pending = cur.fetchone()[0]

    if args.max_phrases > 0 and args.max_phrases < total_pending:
        total_target = args.max_phrases
    else:
        total_target = total_pending

    print(f"[INFO] Pending phrases (tts_ok=false, attempts<{args.max_attempts}): {total_pending:,}", file=sys.stderr)
    print(f"[INFO] Will process up to {total_target:,} phrases this run.", file=sys.stderr)

    if total_target == 0:
        print("[INFO] Nothing to do.", file=sys.stderr)
        conn.close()
        return

    processed = 0
    done = 0
    skipped = 0
    failed = 0

    pbar = tqdm(total=total_target, desc="TTS", unit="phr")

    try:
        while processed < total_target:
            # берём очередную пачку ещё неозвученных фраз
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, phrase, tts_attempts
                    FROM phrases
                    WHERE tts_ok = false
                      AND tts_attempts < %s
                    ORDER BY id
                    LIMIT %s
                    """,
                    (args.max_attempts, args.batch_size),
                )
                rows = cur.fetchall()

            if not rows:
                break  # больше нечего озвучивать

            for row in rows:
                if processed >= total_target:
                    break

                phrase_id = row["id"]
                phrase    = row["phrase"]
                attempts  = row["tts_attempts"] or 0

                out_file = out_dir / f"{phrase_id:06d}.mp3"

                # Если файл уже существует и не пустой — считаем озвученным
                if out_file.exists() and out_file.stat().st_size > 0 and not args.dry_run:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE phrases SET tts_ok = true, tts_error = NULL WHERE id = %s",
                            (phrase_id,),
                        )
                    skipped += 1
                    processed += 1
                    pbar.update(1)
                    continue

                if args.dry_run:
                    # эмуляция
                    print(f"[DRY-RUN] Would TTS id={phrase_id} attempts={attempts}, phrase='{phrase[:60]}...'", file=sys.stderr)
                    processed += 1
                    skipped += 1
                    pbar.update(1)
                    continue

                ok, err = synthesize_to_file(speech_config, phrase, out_file)
                attempts += 1

                with conn.cursor() as cur:
                    if ok:
                        cur.execute(
                            """
                            UPDATE phrases
                            SET tts_ok = true,
                                tts_attempts = %s,
                                tts_error = NULL
                            WHERE id = %s
                            """,
                            (attempts, phrase_id),
                        )
                        done += 1
                    else:
                        cur.execute(
                            """
                            UPDATE phrases
                            SET tts_ok = false,
                                tts_attempts = %s,
                                tts_error = %s
                            WHERE id = %s
                            """,
                            (attempts, err, phrase_id),
                        )
                        failed += 1

                conn.commit()
                processed += 1
                pbar.update(1)

                if not ok and args.sleep_on_error > 0:
                    sleep(args.sleep_on_error)

    finally:
        pbar.close()
        conn.close()

    print("\n=== SUMMARY ===", file=sys.stderr)
    print(f"Target this run : {total_target:,}", file=sys.stderr)
    print(f"Done (OK)       : {done:,}", file=sys.stderr)
    print(f"Skipped         : {skipped:,}", file=sys.stderr)
    print(f"Failed          : {failed:,}", file=sys.stderr)
    if args.dry_run:
        print("[INFO] DRY-RUN mode: no Azure calls, DB not modified.", file=sys.stderr)


if __name__ == "__main__":
    main()
