import requests
import time
import os
import re
import traceback
from typing import Dict, Any, Optional, List
from flask import Flask, request, jsonify
from threading import Lock, Thread, Semaphore

# ========== НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==========
# Обязательные переменные
API_TOKEN = os.environ.get('USEDESK_API_TOKEN')
if not API_TOKEN:
    raise RuntimeError("Переменная окружения USEDESK_API_TOKEN не установлена!")

# Необязательные переменные (если не заданы, используются значения по умолчанию)
AGENT_USER_ID = int(os.environ.get('AGENT_USER_ID', 284224))
MAX_CONCURRENT_TASKS = int(os.environ.get('MAX_CONCURRENT_TASKS', 50))

TICKET_GET_URL = "https://api.usedesk.ru/ticket"
TICKET_COMMENT_URL = "https://api.usedesk.ru/create/comment"
TICKET_UPDATE_URL = "https://secure.usedesk.ru/uapi/update/ticket"
CLIENT_SEARCH_URL = "https://api.usedesk.ru/client"
TICKETS_LIST_URL = "https://api.usedesk.ru/tickets"

REQUEST_DELAY = 0.5

EXCLUDED_PHRASES = [
    "Ваше обращение зарегистрировано под номером",
    "Первое сообщение в тикете",
    "Мы получили все Ваши предыдущие обращения. Номер текущего",
    "browserName:",
    "Тикет объединен с тикетом",
    "Тема ранее :",
    "Вероятно невалидный email",
    "Эскалация на агентов",
    "Благодарим за Ваше обращение в службу поддержки МКК",
    "Первое сообщение в тиккет",
    "Перенесено из тикета",
    "Данное сообщение сформировано автоматически",
    "Получено заявление"
]

EXCLUDED_EMAIL_PATTERNS = [
    "ARBITR", "platiza", "info", "moyapochta", "reply", "robot", "call4life"
]

# Фразы, при наличии которых в note клиента объединение запрещается
SKIP_MERGE_PHRASES = ["не клиент", "не объединять"]
# ===============================

# Блокировки для предотвращения одновременной обработки одного клиента
_client_locks = {}
_client_lock_dict_lock = Lock()
LOCK_TIMEOUT = 30

def _get_client_lock(client_id: int) -> Lock:
    with _client_lock_dict_lock:
        if client_id not in _client_locks:
            _client_locks[client_id] = Lock()
        return _client_locks[client_id]

def lock_client(client_id: int, timeout: float = LOCK_TIMEOUT) -> bool:
    return _get_client_lock(client_id).acquire(blocking=True, timeout=timeout)

def unlock_client(client_id: int) -> None:
    _get_client_lock(client_id).release()

_task_semaphore = Semaphore(MAX_CONCURRENT_TASKS)

app = Flask(__name__)

# ------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ------------------------------------------------------------

def get_ticket_details(ticket_id: int) -> Optional[Dict[str, Any]]:
    payload = {"api_token": API_TOKEN, "ticket_id": ticket_id}
    try:
        print(f"    Запрос данных тикета {ticket_id}...")
        response = requests.post(TICKET_GET_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            print(f"    ❌ Ошибка API: {data['error']}")
            return None
        return data
    except Exception as e:
        print(f"    ❌ Ошибка: {e}")
        return None

def get_open_tickets_by_client(client_id: int) -> List[Dict[str, Any]]:
    try:
        params = {"api_token": API_TOKEN, "client_id": client_id, "fstatus": 1}
        response = requests.get(TICKETS_LIST_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        print(f"Ошибка при получении тикетов клиента {client_id}: {e}")
        return []

def clean_html_wrappers(message: str) -> str:
    message = re.sub(r'<html[^>]*>', '', message, flags=re.IGNORECASE)
    message = re.sub(r'</html>', '', message, flags=re.IGNORECASE)
    body_match = re.search(r'<body[^>]*>(.*?)</body>', message, flags=re.IGNORECASE | re.DOTALL)
    if body_match:
        message = body_match.group(1)
    else:
        message = re.sub(r'<body[^>]*>', '', message, flags=re.IGNORECASE)
        message = re.sub(r'</body>', '', message, flags=re.IGNORECASE)
    return message

def add_comment_to_ticket(ticket_id: int, comment_text: str,
                          comment_type: str = "public",
                          user_id: Optional[int] = None,
                          client_id: Optional[int] = None) -> bool:
    payload = {
        "api_token": API_TOKEN,
        "ticket_id": ticket_id,
        "message": comment_text,
        "type": comment_type,
        "from": "user"
    }
    if user_id:
        payload["user_id"] = str(user_id)
    if client_id:
        payload["client_id"] = client_id
    try:
        print(f"    ➕ Добавление {comment_type} комментария в тикет {ticket_id}...")
        print(f"    Сообщение (начало): {comment_text[:150]}...")
        response = requests.post(TICKET_COMMENT_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        print(f"    Ответ сервера: {result}")
        return result.get("status") == "success"
    except Exception as e:
        print(f"    ❌ Исключение: {e}")
        return False

def update_ticket_status(ticket_id: int, status_id: str) -> bool:
    payload = {"api_token": API_TOKEN, "ticket_id": ticket_id, "status": status_id}
    try:
        print(f"    🔄 Обновление статуса тикета {ticket_id} на {status_id}...")
        response = requests.post(TICKET_UPDATE_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        print(f"    Ответ сервера: {result}")
        return result.get("status") == "success"
    except Exception as e:
        print(f"    ❌ Исключение: {e}")
        return False

def add_tags_to_ticket(ticket_id: int, tags: List[str]) -> bool:
    if not tags:
        return True
    success = True
    for tag in tags:
        payload = {"api_token": API_TOKEN, "ticket_id": ticket_id, "tag": tag}
        try:
            print(f"    🏷️ Добавление тега '{tag}' к тикету {ticket_id}...")
            response = requests.post(TICKET_UPDATE_URL, json=payload)
            response.raise_for_status()
            result = response.json()
            print(f"    Ответ сервера: {result}")
            if result.get("status") != "success":
                success = False
        except Exception as e:
            print(f"    ❌ Ошибка при добавлении тега '{tag}': {e}")
            success = False
    return success

def extract_full_info_from_duplicate(ticket_data: Dict[str, Any]) -> Dict[str, Any]:
    result = {"subject": "Тема не найдена", "tags": [], "comments": []}
    if "ticket" in ticket_data and "subject" in ticket_data["ticket"]:
        result["subject"] = ticket_data["ticket"]["subject"]
    if "tags" in ticket_data:
        result["tags"] = ticket_data["tags"]
    for c in ticket_data.get("comments", []):
        comment_info = {
            "id": c.get("id"),
            "message": c.get("message", ""),
            "files": []
        }
        if "files" in c and c["files"]:
            for f in c["files"]:
                comment_info["files"].append({"name": f.get("name", "Без имени"), "url": f.get("file", "#")})
        result["comments"].append(comment_info)
    return result

def merge_duplicate_into_main(main_ticket_id: int, dup_ticket_id: int) -> bool:
    try:
        # 1. Проверяем, что дубль существует и имеет статус 1 (открыт)
        dup_data = get_ticket_details(dup_ticket_id)
        if not dup_data:
            print(f"  ⚠️ Не удалось получить данные для тикета {dup_ticket_id}, пропускаем.")
            return False

        dup_ticket = dup_data.get('ticket', dup_data)
        dup_status = dup_ticket.get('status_id') or dup_ticket.get('status')
        if isinstance(dup_status, dict):
            dup_status = dup_status.get('id')
        if dup_status != 1:
            print(f"  ⏭️ Тикет {dup_ticket_id} не является открытым (статус {dup_status}), пропускаем.")
            return False

        print(f"\n  Обработка дубля ID: {dup_ticket_id} (основной: {main_ticket_id})")
        info = extract_full_info_from_duplicate(dup_data)

        # 2. Перенос тегов (если есть)
        if info["tags"]:
            if add_tags_to_ticket(main_ticket_id, info["tags"]):
                print(f"  ✅ Теги добавлены в основной тикет {main_ticket_id}.")
            else:
                print(f"  ⚠️ Не удалось добавить теги, продолжаем без них.")
        else:
            print(f"  ℹ️ В дубле нет тегов.")

        # 3. Копирование комментариев
        print(f"    Найдено комментариев для копирования: {len(info['comments'])}")
        for comm in info["comments"]:
            raw_message = comm["message"]
            if any(phrase in raw_message for phrase in EXCLUDED_PHRASES):
                print(f"    ⏭️ Пропуск комментария (содержит исключённый текст): ID в дубле {comm.get('id', '?')}")
                continue
            cleaned_message = clean_html_wrappers(raw_message)
            subject_display = info["subject"] if info["subject"] != "Тема не найдена" else "без темы"
            source_text = f'Перенесено из тикета <a href="https://secure.usedesk.ru/tickets/{dup_ticket_id}">#{dup_ticket_id}</a> (Тема: {subject_display})<br><br>'
            comment_text = source_text + cleaned_message
            if comm["files"]:
                comment_text += "\n\n<b>Вложения:</b><br>"
                for f in comm["files"]:
                    comment_text += f'- <a href="{f["url"]}">{f["name"]}</a><br>'
            success = add_comment_to_ticket(main_ticket_id, comment_text, comment_type="private", user_id=AGENT_USER_ID)
            if success:
                print(f"  ✅ Приватный комментарий скопирован (ID в дубле: {comm.get('id', '?')}).")
            else:
                print(f"  ❌ Ошибка при копировании комментария (ID в дубле: {comm.get('id', '?')}).")

        # 4. Обновление статуса дубля на 10 (Объединён)
        if update_ticket_status(dup_ticket_id, "10"):
            print(f"  ✅ Статус дубля {dup_ticket_id} изменён на 'Объединён'.")
        else:
            print(f"  ❌ Ошибка при обновлении статуса дубля {dup_ticket_id}.")
            return False

        # 5. Добавление комментария в дубль
        dup_comment = f'Тикет объединен с тикетом <a href="https://secure.usedesk.ru/tickets/{main_ticket_id}">#{main_ticket_id}</a>'
        if add_comment_to_ticket(dup_ticket_id, dup_comment, comment_type="private", user_id=AGENT_USER_ID):
            print(f"  ✅ Приватный комментарий в дубль {dup_ticket_id} добавлен.")
        else:
            print(f"  ❌ Ошибка при добавлении приватного комментария в дубль.")
        time.sleep(REQUEST_DELAY)
        return True
    except Exception as e:
        print(f"  ❌ Исключение в merge_duplicate_into_main: {e}")
        traceback.print_exc()
        return False

def should_skip_email(email: str) -> bool:
    email_lower = email.lower()
    for pattern in EXCLUDED_EMAIL_PATTERNS:
        if pattern.lower() in email_lower:
            return True
    return False

def is_ticket_allowed(ticket: Dict[str, Any]) -> bool:
    assignee_id = ticket.get('assignee_id')
    group = ticket.get('group')
    if assignee_id is not None and assignee_id != 0:
        return False
    group_id = None
    if isinstance(group, dict):
        group_id = group.get('id')
    else:
        group_id = group
    if group_id == 72354:
        return False
    return True

def wait_for_status_open(ticket_id: int, max_attempts: int = 3, delay: int = 10) -> bool:
    """
    Ожидает, пока статус тикета не станет 1 (открыт).
    Делает до max_attempts попыток с паузой delay секунд между ними.
    Возвращает True, если статус стал 1, иначе False.
    """
    for attempt in range(max_attempts):
        details = get_ticket_details(ticket_id)
        if not details:
            print(f"  Попытка {attempt+1}: не удалось получить данные тикета")
            if attempt == max_attempts - 1:
                return False
            time.sleep(delay)
            continue
        ticket = details.get('ticket', details)
        status = ticket.get('status_id') or ticket.get('status')
        if isinstance(status, dict):
            status = status.get('id')
        print(f"  Попытка {attempt+1}: статус тикета = {status}")
        if status == 1:
            return True
        # Если статус не 8, продолжать ожидание не имеет смысла
        if status != 8:
            print(f"  Статус {status} не является 1 или 8, прерываем ожидание.")
            return False
        if attempt < max_attempts - 1:
            print(f"  Ждём {delay} секунд перед следующей попыткой...")
            time.sleep(delay)
    return False

def extract_client_note(ticket_data: Dict[str, Any]) -> Optional[str]:
    """
    Извлекает поле note из профиля клиента, связанного с тикетом.
    Сначала ищет в корневом объекте 'client', затем в комментариях типа 'client'.
    Возвращает строку note или None, если не найдено.
    """
    # 1. Пытаемся найти client на верхнем уровне ответа
    if 'client' in ticket_data and isinstance(ticket_data['client'], dict):
        note = ticket_data['client'].get('note')
        if note:
            return note

    # 2. Ищем в комментариях, где from == 'client' и есть поле client с note
    comments = ticket_data.get('comments', [])
    for comment in comments:
        if comment.get('from') == 'client':
            client_obj = comment.get('client')
            if client_obj and isinstance(client_obj, dict):
                note = client_obj.get('note')
                if note:
                    return note
    return None

def should_skip_due_to_client_note(ticket_data: Dict[str, Any]) -> bool:
    """
    Проверяет, содержит ли note клиента запрещающие фразы.
    Возвращает True, если объединение нужно пропустить.
    """
    note = extract_client_note(ticket_data)
    if not note:
        return False

    note_lower = note.lower()
    for phrase in SKIP_MERGE_PHRASES:
        if phrase in note_lower:
            print(f"⛔ В note клиента найдена фраза '{phrase}': {note[:200]}... Объединение отменено.")
            return True
    return False

# ------------------------------------------------------------
# НОВАЯ ФУНКЦИЯ: ПРОВЕРКА НА CAP-ФАЙЛЫ
# ------------------------------------------------------------
def has_cap_file(ticket_data: Dict[str, Any]) -> bool:
    """
    Проверяет, есть ли в комментариях тикета файлы с именем, начинающимся на 'CAP'.
    Возвращает True, если хотя бы один файл подходит.
    """
    comments = ticket_data.get('comments', [])
    for comment in comments:
        files = comment.get('files', [])
        # Также проверяем одиночный файл в поле 'file' (если есть и это словарь)
        single_file = comment.get('file')
        if single_file and isinstance(single_file, dict):
            files.append(single_file)
        for file_obj in files:
            if not isinstance(file_obj, dict):
                continue
            name = file_obj.get('name', '')
            if name and re.match(r'^CAP', name, re.IGNORECASE):
                return True
    return False
# ------------------------------------------------------------

# ------------------------------------------------------------
# ФОНОВАЯ ОБРАБОТКА
# ------------------------------------------------------------

def process_webhook_async(data: Dict[str, Any]):
    try:
        # Извлекаем ticket_id, email и channel_id
        if 'ticket' in data and isinstance(data['ticket'], dict):
            ticket_data = data['ticket']
            ticket_id = ticket_data.get('id')
            client_email = ticket_data.get('email')
            channel_id = ticket_data.get('channel_id')
        else:
            ticket_id = data.get('ticket_id')
            client_email = data.get('client_email') or data.get('email')
            channel_id = data.get('channel_id')

        if not ticket_id or not client_email:
            print(f"❌ Не хватает данных: ticket_id={ticket_id}, email={client_email}")
            return

        print(f"\n=== Обработка вебхука (фон): тикет {ticket_id}, email {client_email}, channel_id {channel_id} ===")

        if channel_id != 62224:
            print(f"⏭️ Тикет из канала {channel_id} (не 62224). Объединение не выполняется.")
            return

        if should_skip_email(client_email):
            print(f"⏭️ Email {client_email} в списке исключений. Объединение не выполняется.")
            return

        ticket_details = get_ticket_details(ticket_id)
        if not ticket_details:
            print(f"❌ Не удалось получить данные для тикета {ticket_id}. Объединение невозможно.")
            return

        # --- НОВАЯ ПРОВЕРКА: note клиента ---
        if should_skip_due_to_client_note(ticket_details):
            print(f"⏭️ Тикет {ticket_id} пропущен из-за пометки в профиле клиента.")
            return

        # --- НОВАЯ ПРОВЕРКА НА CAP-ФАЙЛЫ ---
        if has_cap_file(ticket_details):
            existing_tags = ticket_details.get('tags', [])
            if "CAP_system" not in existing_tags:
                print(f"  🏷️ Обнаружен CAP-файл в тикете {ticket_id}, добавляем тег 'CAP_system'")
                add_tags_to_ticket(ticket_id, ["CAP_system"])
            else:
                print(f"  ℹ️ Тег 'CAP_system' уже есть в тикете {ticket_id}")
        # ---------------------------------

        if 'ticket' in ticket_details:
            ticket_obj = ticket_details['ticket']
        else:
            ticket_obj = ticket_details

        # Проверка статуса
        status = ticket_obj.get('status_id') or ticket_obj.get('status')
        if isinstance(status, dict):
            status = status.get('id')
        if status == 1:
            print("Тикет уже открыт, продолжаем.")
        elif status == 8:
            print("Тикет в статусе 8, ожидаем смены на 1...")
            if not wait_for_status_open(ticket_id):
                print(f"⏭️ Тикет {ticket_id} не стал открытым после ожидания, пропускаем.")
                return
            # После ожидания получаем актуальные данные тикета
            ticket_details = get_ticket_details(ticket_id)
            if not ticket_details:
                print(f"❌ Не удалось получить данные для тикета {ticket_id} после ожидания.")
                return
            # Повторная проверка note (на случай, если за время ожидания пометка изменилась)
            if should_skip_due_to_client_note(ticket_details):
                print(f"⏭️ Тикет {ticket_id} пропущен из-за пометки в профиле клиента (после ожидания).")
                return
            # Повторная проверка CAP-файлов (на случай, если файлы добавились после обновления)
            if has_cap_file(ticket_details):
                existing_tags = ticket_details.get('tags', [])
                if "CAP_system" not in existing_tags:
                    print(f"  🏷️ Обнаружен CAP-файл в тикете {ticket_id} (после ожидания), добавляем тег 'CAP_system'")
                    add_tags_to_ticket(ticket_id, ["CAP_system"])
            # ---------------------------------
            if 'ticket' in ticket_details:
                ticket_obj = ticket_details['ticket']
            else:
                ticket_obj = ticket_details
            print("Тикет стал открытым, продолжаем.")
        else:
            print(f"⏭️ Тикет {ticket_id} имеет статус {status} (не 1 и не 8), объединение невозможно.")
            return

        if not is_ticket_allowed(ticket_obj):
            print(f"⏭️ Тикет {ticket_id} не разрешён для объединения: assignee_id={ticket_obj.get('assignee_id')}, group={ticket_obj.get('group')}")
            return

        client_id = ticket_obj.get('client_id')
        if not client_id:
            print(f"❌ Не удалось определить client_id для тикета {ticket_id}.")
            return

        print(f"Найден client_id: {client_id}")

        if not lock_client(client_id):
            print(f"❌ Не удалось получить блокировку для клиента {client_id} (таймаут {LOCK_TIMEOUT} сек). Возможно, уже обрабатывается.")
            return

        try:
            # Даём API немного времени для синхронизации
            time.sleep(2)

            all_open_tickets = get_open_tickets_by_client(client_id)

            # Добавляем текущий тикет, если его нет в списке
            current_ticket_in_list = any(t['id'] == ticket_id for t in all_open_tickets)
            if not current_ticket_in_list:
                print(f"Тикет {ticket_id} не найден в списке открытых, добавляем вручную.")
                if is_ticket_allowed(ticket_obj):
                    all_open_tickets.append(ticket_obj)
                    print(f"Тикет {ticket_id} добавлен в список для обработки.")
                else:
                    print(f"Тикет {ticket_id} не добавлен: not allowed")

            max_iterations = 5
            main_ticket_id = None
            processed_dup_ids = set()

            for iteration in range(max_iterations):
                # Фильтруем разрешённые тикеты
                allowed_tickets = []
                for t in all_open_tickets:
                    if not is_ticket_allowed(t):
                        print(f"Тикет {t['id']} исключён из обработки: assignee_id={t.get('assignee_id')}, group={t.get('group')}")
                        continue
                    allowed_tickets.append(t)

                if not allowed_tickets:
                    print(f"Итерация {iteration+1}: нет разрешённых открытых тикетов.")
                    break

                allowed_tickets.sort(key=lambda t: t['id'])
                if len(allowed_tickets) < 2:
                    print(f"Итерация {iteration+1}: только один разрешённый открытый тикет ({allowed_tickets[0]['id']}).")
                    break

                current_main = allowed_tickets[0]
                if main_ticket_id is None:
                    main_ticket_id = current_main['id']
                elif main_ticket_id != current_main['id']:
                    print(f"⚠️ Обнаружен более старый тикет {current_main['id']}, делаем его основным.")
                    main_ticket_id = current_main['id']

                duplicates = []
                for t in allowed_tickets[1:]:
                    if t['id'] in processed_dup_ids:
                        continue
                    status_val = t.get('status_id')
                    if status_val is None:
                        status_val = t.get('status')
                    if isinstance(status_val, dict):
                        status_val = status_val.get('id')
                    if status_val == 1:
                        duplicates.append(t)
                    else:
                        print(f"Тикет {t['id']} имеет статус {status_val} (не открыт), пропускаем.")

                if not duplicates:
                    print(f"Итерация {iteration+1}: нет новых дублей для объединения.")
                    break

                print(f"Итерация {iteration+1}: основной тикет {main_ticket_id}, дубли: {[d['id'] for d in duplicates]}")
                for dup in duplicates:
                    merge_duplicate_into_main(main_ticket_id, dup['id'])
                    processed_dup_ids.add(dup['id'])

                time.sleep(2)
                all_open_tickets = get_open_tickets_by_client(client_id)
                # Снова добавляем текущий тикет, если его нет
                current_ticket_in_list = any(t['id'] == ticket_id for t in all_open_tickets)
                if not current_ticket_in_list and is_ticket_allowed(ticket_obj):
                    all_open_tickets.append(ticket_obj)

            if main_ticket_id:
                print(f"✅ Объединение завершено. Основной тикет: {main_ticket_id}")
            else:
                print("ℹ️ Не было основного тикета для объединения.")

        finally:
            unlock_client(client_id)

    except Exception as e:
        print("=" * 50)
        print("ОШИБКА В ФОНОВОЙ ОБРАБОТКЕ:")
        traceback.print_exc()
        print("=" * 50)

# ------------------------------------------------------------
# МАРШРУТЫ FLASK
# ------------------------------------------------------------

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data"}), 400

    print(f"Получен вебхук, ставим в очередь обработки")

    def run_with_semaphore():
        try:
            _task_semaphore.acquire()
            process_webhook_async(data)
        finally:
            _task_semaphore.release()

    thread = Thread(target=run_with_semaphore)
    thread.daemon = True
    thread.start()

    return jsonify({"status": "accepted"}), 200

@app.route('/', methods=['POST'])
def root_webhook():
    return webhook()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)