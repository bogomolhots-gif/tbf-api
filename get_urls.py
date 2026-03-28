import json
import os

# 1. Читаем твою базу
with open("tobacco_intelligence_v3.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# 2. Достаем абсолютно все ссылки
all_urls = [item.get("url") for item in data if item.get("url")]

# 3. Смотрим, что уже есть в urls.txt, чтобы не дублировать
existing_urls = set()
if os.path.exists("urls.txt"):
    with open("urls.txt", "r", encoding="utf-8") as f:
        existing_urls = set(line.strip() for line in f)

# 4. Отбираем только те, которых еще нет в текстовике
new_urls = [url for url in all_urls if url not in existing_urls]

# 5. Дописываем новые ссылки в конец urls.txt
if new_urls:
    with open("urls.txt", "a", encoding="utf-8") as f:
        for url in new_urls:
            f.write(url + "\n")
    print(f"✅ Успех! Добавлено {len(new_urls)} новых ссылок в urls.txt.")
else:
    print("ℹ️ Все ссылки уже есть в urls.txt, добавлять нечего.")