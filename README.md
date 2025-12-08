# hablai-tts

python3 generate_tts_azure_db.py \
  -o data/tts_mp3 \
  --max-phrases 10 \
  --dry-run



читаем все фразы из таблицы phrases;
сохраняем в TSV id<TAB>phrase в папке с аудио;
по желанию — доп. поля (freq, length), но минимум — пара id-phrase;

прогресс показываем через tqdm.
  python3 dump_phrases_backup.py \
  -a data/tts_mp3

