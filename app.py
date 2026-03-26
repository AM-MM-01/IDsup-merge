import requests
import time
import os
import re
import traceback
from typing import Dict, Any, Optional, List
from flask import Flask, request, jsonify
from threading import Lock

# ========== НАСТРОЙКИ ==========
API_TOKEN = os.environ.get('USEDESK_API_TOKEN', "c126b5f87d07a42e872367607f8e35e41acfa40b")

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

AGENT_USER_ID = int(os.environ.get('AGENT_USER_ID', 284224))
# ===============================

# Блокировки для предотвращения одновременной обработки одного клиента
_client_locks = {}
_client_lock_dict_lock = Lock()

def _get_client_lock(client_id: int) -> Lock:
    with _client_lock_dict_lock:
        if client_id not in _client_locks:
            _client_locks[client_id] = Lock()
        return _client_locks[client_id]

def lock_client(client_id: int) -> bool:
    """Пытается захватить блокировку для клиента. Возвращает True, если успешно."""
    return _get_client_lock(client_id).acquire(blocking=False)

def unlock_client(client_id: int) -> None:
    _get_client_lock(client_id).release()

app = Flask(__name__)

# ----------------------------------------------------------------------
# (все остальные функции: get_ticket_details, clean_html_wrappers,
#  add_comment_to_ticket, update_ticket_status, add_tags_to_ticket,
#  extract_full_info_from_duplicate, merge_duplicate_into_main,
#  should_skip_email, get_open_tickets_by_client)
# Вставляем их сюда без изменений (см. предыдущий ответ)
# ----------------------------------------------------------------------

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        if not data:
            print("❌ Нет JSON-данных в запросе")
            return jsonify({"error": "No JSON data"}), 400

        print(f"Получены данные: {data}")

        # Извлекаем ticket_id и email
        if 'ticket' in data and isinstance(data['ticket'], dict):
            ticket_data = data['ticket']
            ticket_id = ticket_data.get('id')
            client_email = ticket_data.get('email')
        else:
            ticket_id = data.get('ticket_id')
            client_email = data.get('client_email') or data.get('email')

        if not ticket_id or not client_email:
            print(f"❌ Не хватает данных: ticket_id={ticket_id}, email={client_email}")
            return jsonify({"error": "Missing ticket_id or client_email"}), 400

        print(f"\n=== Получен вебхук: тикет {ticket_id}, email {client_email} ===")

        if should_skip_email(client_email):
            print(f"⏭️ Email {client_email} в списке исключений. Объединение не выполняется.")
            return jsonify({"status": "skipped", "reason": "excluded_email"}), 200

        # Получаем client_id через детали тикета
        ticket_details = get_ticket_details(ticket_id)
        if not ticket_details:
            print(f"❌ Не удалось получить данные для тикета {ticket_id}. Объединение невозможно.")
            return jsonify({"error": "Cannot fetch ticket details"}), 500

        client_id = None
        if 'ticket' in ticket_details:
            client_id = ticket_details['ticket'].get('client_id')
        if not client_id and 'client_id' in ticket_details:
            client_id = ticket_details['client_id']

        if not client_id:
            print(f"❌ Не удалось определить client_id для тикета {ticket_id}.")
            return jsonify({"error": "Client ID not found"}), 500

        print(f"Найден client_id: {client_id}")

        # Попытка заблокировать клиента
        if not lock_client(client_id):
            print(f"⏭️ Клиент {client_id} уже обрабатывается, пропускаем.")
            return jsonify({"status": "skipped", "reason": "already_processing"}), 200

        try:
            # Основная логика
            open_tickets = get_open_tickets_by_client(client_id)
            if not open_tickets:
                print(f"ℹ️ У клиента {client_email} нет открытых тикетов.")
                return jsonify({"status": "ok", "message": "No open tickets"}), 200

            open_tickets.sort(key=lambda t: t['id'])
            if len(open_tickets) < 2:
                print(f"ℹ️ У клиента {client_email} только один открытый тикет ({open_tickets[0]['id']}). Объединение не требуется.")
                return jsonify({"status": "ok", "message": "Only one open ticket"}), 200

            main_ticket = open_tickets[0]
            duplicate_tickets = open_tickets[1:]

            print(f"Основной тикет: {main_ticket['id']}")
            print(f"Дубли: {[t['id'] for t in duplicate_tickets]}")

            for dup in duplicate_tickets:
                try:
                    status_id = None
                    if 'status' in dup:
                        if isinstance(dup['status'], dict):
                            status_id = dup['status'].get('id')
                        else:
                            status_id = dup['status']
                    if status_id == 10:
                        print(f"⏭️ Тикет {dup['id']} уже имеет статус 'Объединён', пропускаем.")
                        continue
                    merge_duplicate_into_main(main_ticket['id'], dup['id'])
                except Exception as e:
                    print(f"❌ Ошибка при обработке дубля {dup.get('id', '?')}: {e}")
                    traceback.print_exc()

            print(f"🏷️ Добавление тега 'merge' в основной тикет {main_ticket['id']}...")
            if add_tags_to_ticket(main_ticket['id'], ["merge"]):
                print(f"✅ Тег 'merge' успешно добавлен в тикет {main_ticket['id']}.")
            else:
                print(f"⚠️ Не удалось добавить тег 'merge' в тикет {main_ticket['id']}.")

            return jsonify({"status": "success"}), 200
        finally:
            unlock_client(client_id)

    except Exception as e:
        print("=" * 50)
        print("ОШИБКА В WEBHOOK:")
        traceback.print_exc()
        print("=" * 50)
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

@app.route('/', methods=['POST'])
def root_webhook():
    return webhook()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)