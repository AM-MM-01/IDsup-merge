import requests
import time
import os
import re
from typing import Dict, Any, Optional, List
from flask import Flask, request, jsonify

# ========== НАСТРОЙКИ ==========
API_TOKEN = os.environ.get('USEDESK_API_TOKEN', "bc6ed228bf8fa594e3b4b8bc8637ba3ce13b6ad5")

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
    "Перенесено из тикета"
]

EXCLUDED_EMAIL_PATTERNS = [
    "ARBITR", "platiza", "info", "moyapochta", "reply", "robot", "call4life"
]

AGENT_USER_ID = int(os.environ.get('AGENT_USER_ID', 207358))
# ===============================

app = Flask(__name__)

def get_client_id_by_email(email: str) -> Optional[int]:
    try:
        params = {"api_token": API_TOKEN, "email": email}
        response = requests.get(CLIENT_SEARCH_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if data and isinstance(data, list) and len(data) > 0:
            return data[0].get("id")
        return None
    except Exception as e:
        print(f"Ошибка при поиске клиента по email {email}: {e}")
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
    print(f"\n  Обработка дубля ID: {dup_ticket_id} (основной: {main_ticket_id})")
    dup_data = get_ticket_details(dup_ticket_id)
    if not dup_data:
        print(f"  ⚠️ Не удалось получить данные для тикета {dup_ticket_id}, пропускаем.")
        return False
    info = extract_full_info_from_duplicate(dup_data)

    if info["tags"]:
        if add_tags_to_ticket(main_ticket_id, info["tags"]):
            print(f"  ✅ Теги добавлены в основной тикет {main_ticket_id}.")
        else:
            print(f"  ⚠️ Не удалось добавить теги, продолжаем без них.")
    else:
        print(f"  ℹ️ В дубле нет тегов.")

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

    if update_ticket_status(dup_ticket_id, "10"):
        print(f"  ✅ Статус дубля {dup_ticket_id} изменён на 'Объединён'.")
    else:
        print(f"  ❌ Ошибка при обновлении статуса дубля {dup_ticket_id}.")
        return False

    dup_comment = f'Тикет объединен с тикетом <a href="https://secure.usedesk.ru/tickets/{main_ticket_id}">#{main_ticket_id}</a>'
    if add_comment_to_ticket(dup_ticket_id, dup_comment, comment_type="private", user_id=AGENT_USER_ID):
        print(f"  ✅ Приватный комментарий в дубль {dup_ticket_id} добавлен.")
    else:
        print(f"  ❌ Ошибка при добавлении приватного комментария в дубль.")
    time.sleep(REQUEST_DELAY)
    return True

def should_skip_email(email: str) -> bool:
    email_lower = email.lower()
    for pattern in EXCLUDED_EMAIL_PATTERNS:
        if pattern.lower() in email_lower:
            return True
    return False

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data"}), 400

    # Извлекаем ticket_id и email
    if 'ticket' in data and isinstance(data['ticket'], dict):
        ticket_data = data['ticket']
        ticket_id = ticket_data.get('id')
        client_email = ticket_data.get('email')
    else:
        ticket_id = data.get('ticket_id')
        client_email = data.get('client_email') or data.get('email')

    if not ticket_id or not client_email:
        return jsonify({"error": "Missing ticket_id or client_email"}), 400

    print(f"\n=== Получен вебхук: тикет {ticket_id}, email {client_email} ===")

    # Проверка на исключаемые email
    if should_skip_email(client_email):
        print(f"⏭️ Email {client_email} в списке исключений. Объединение не выполняется.")
        return jsonify({"status": "skipped", "reason": "excluded_email"}), 200

    # Получаем детали тикета, чтобы извлечь client_id
    ticket_details = get_ticket_details(ticket_id)
    if not ticket_details:
        print(f"❌ Не удалось получить данные для тикета {ticket_id}. Объединение невозможно.")
        return jsonify({"error": "Cannot fetch ticket details"}), 500

    # Извлекаем client_id из данных тикета
    client_id = None
    if 'ticket' in ticket_details:
        client_id = ticket_details['ticket'].get('client_id')
    if not client_id and 'client_id' in ticket_details:
        client_id = ticket_details['client_id']

    if not client_id:
        print(f"❌ Не удалось определить client_id для тикета {ticket_id}. Объединение невозможно.")
        # Для отладки: выводим структуру ответа
        print(f"Ответ API: {ticket_details}")
        return jsonify({"error": "Client ID not found"}), 500

    print(f"Найден client_id: {client_id}")

    # Получаем все открытые тикеты клиента
    open_tickets = get_open_tickets_by_client(client_id)
    if not open_tickets:
        print(f"ℹ️ У клиента {client_email} нет открытых тикетов.")
        return jsonify({"status": "ok", "message": "No open tickets"}), 200

    # Сортируем по ID (старые первыми)
    open_tickets.sort(key=lambda t: t['id'])
    if len(open_tickets) < 2:
        print(f"ℹ️ У клиента {client_email} только один открытый тикет ({open_tickets[0]['id']}). Объединение не требуется.")
        return jsonify({"status": "ok", "message": "Only one open ticket"}), 200

    main_ticket = open_tickets[0]
    duplicate_tickets = open_tickets[1:]

    print(f"Основной тикет: {main_ticket['id']}")
    print(f"Дубли: {[t['id'] for t in duplicate_tickets]}")

    for dup in duplicate_tickets:
        if dup.get('status', {}).get('id') == 10:
            print(f"⏭️ Тикет {dup['id']} уже имеет статус 'Объединён', пропускаем.")
            continue
        merge_duplicate_into_main(main_ticket['id'], dup['id'])

    print(f"🏷️ Добавление тега 'merge' в основной тикет {main_ticket['id']}...")
    if add_tags_to_ticket(main_ticket['id'], ["merge"]):
        print(f"✅ Тег 'merge' успешно добавлен в тикет {main_ticket['id']}.")
    else:
        print(f"⚠️ Не удалось добавить тег 'merge' в тикет {main_ticket['id']}.")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)