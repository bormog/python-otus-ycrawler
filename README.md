### YCrawler
Скрипт, который асинхронно парсит сайт https://news.ycombinator.com
- Скрипт начинает обход с https://news.ycombinator.com
- Скачивает топ-новостей (30 штук) и сохраняет их на диск
- Через определенный интервал (15) проверяет главную страницу на предмет появления новых постов
- Для каждой новости скачиваются ссылки на другие страницы из комментариев

## Как запускать
```
python main.py
```

### Опции
```
- repeat_interval, default = 15 - Интервал с которым скрипт будет проверять главную страницу на предмет появления новых новостей
- page_limit, default = 30 - Количество новостей, которые будут считаны с главной страницы
- download_dir, default = pages - Директория в которую будут сохраняться новости
- dry_run, default = True - Запуск без сохранения файлов на диск
- logfile, default = None - логфайл
- logfile, default = INFO - логлевел
```