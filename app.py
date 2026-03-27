import requests
import time
import os
import re
import traceback
from typing import Dict, Any, Optional, List
from flask import Flask, request, jsonify
from threading import Lock, Thread, Semaphore

# ========== НАСТРОЙКИ ==========
API_TOKEN = os.environ.get('USEDESK_API_TOKEN', "28fc2322dbdafe78adca1213b8b6e4d3d3d00fe1")

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

AGENT_USER_ID = int(os.environ.get('AGENT_USER_ID', 284224))
MAX_CONCURRENT_TASKS = int(os.environ.get('MAX_CONCURRENT_TASKS', 5))  # ограничим количество одновременно выполняемых задач
# ===============================

# Блокировки для предотвращения одновременной обработки одного клиента
_client_locks = {}
_client_lock_dict_lock = Lock()
LOCK_TIMEOUT = 30  # секунд ожидания

def _get_client_lock(client_id: int) -> Lock:
    with _client_lock_dict_lock:
        if client_id not in _client_locks:
            _client_locks[client_id] = Lock()
        return _client_locks[client_id]

def lock_client(client_id: int, timeout: float = LOCK_TIMEOUT) -> bool:
    return _get_client_lock(client_id).acquire(blocking=True, timeout=timeout)

def unlock_client(client_id: int) -> None:
    _get_client_lock(client_id).release()

# Пул потоков (ограничиваем количество одновременно обрабатываемых вебхуков)
_task_semaphore = Semaphore(MAX_CONCURRENT_TASKS)

app = Flask(__name__)

# ------------------------------------------------------------
# Все основные функции (get_ticket_details, get_open_tickets_by_client,
# clean_html_wrappers, add_comment_to_ticket, update_ticket_status,
# add_tags_to_ticket, extract_full_info_from_duplicate,
# merge_duplicate_into_main, should_skip_email, is_ticket_allowed)
# остаются без изменений (см. предыдущую версию).
# ------------------------------------------------------------

def process_webhook_async(data: Dict[str, Any]):
    """Фоновая задача для обработки вебхука."""
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

        # Проверка канала
        if channel_id != 62224:
            print(f"⏭️ Тикет из канала {channel_id} (не 62224). Объединение не выполняется.")
            return

        if should_skip_email(client_email):
            print(f"⏭️ Email {client_email} в списке исключений. Объединение не выполняется.")
            return

        # Получаем детали тикета
        ticket_details = get_ticket_details(ticket_id)
        if not ticket_details:
            print(f"❌ Не удалось получить данные для тикета {ticket_id}. Объединение невозможно.")
            return

        # Извлекаем плоскую структуру тикета
        if 'ticket' in ticket_details:
            ticket_obj = ticket_details['ticket']
        else:
            ticket_obj = ticket_details

        if not is_ticket_allowed(ticket_obj):
            print(f"⏭️ Тикет {ticket_id} не разрешён для объединения: assignee_id={ticket_obj.get('assignee_id')}, group={ticket_obj.get('group')}")
            return

        client_id = ticket_obj.get('client_id')
        if not client_id:
            print(f"❌ Не удалось определить client_id для тикета {ticket_id}.")
            return

        print(f"Найден client_id: {client_id}")

        # Блокируем клиента с ожиданием
        if not lock_client(client_id):
            print(f"❌ Не удалось получить блокировку для клиента {client_id} (таймаут {LOCK_TIMEOUT} сек). Возможно, уже обрабатывается.")
            return

        try:
            max_iterations = 5
            main_ticket_id = None
            for iteration in range(max_iterations):
                all_open_tickets = get_open_tickets_by_client(client_id)
                if not all_open_tickets:
                    print(f"Итерация {iteration+1}: нет открытых тикетов.")
                    break

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
                    status_val = t.get('status_id')
                    if status_val is None:
                        status_val = t.get('status')
                    if isinstance(status_val, dict):
                        status_val = status_val.get('id')
                    if status_val != 10:
                        duplicates.append(t)

                if not duplicates:
                    print(f"Итерация {iteration+1}: нет новых дублей для объединения.")
                    break

                print(f"Итерация {iteration+1}: основной тикет {main_ticket_id}, дубли: {[d['id'] for d in duplicates]}")
                for dup in duplicates:
                    merge_duplicate_into_main(main_ticket_id, dup['id'])

                time.sleep(1)

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

@app.route('/webhook', methods=['POST'])
def webhook():
    """Сразу возвращаем 200 и запускаем обработку в фоновом потоке."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data"}), 400

    print(f"Получен вебхук, ставим в очередь обработки")

    # Используем семафор, чтобы ограничить количество одновременно выполняющихся задач
    # и не создать слишком много потоков
    def run_with_semaphore():
        try:
            _task_semaphore.acquire()
            process_webhook_async(data)
        finally:
            _task_semaphore.release()

    thread = Thread(target=run_with_semaphore)
    thread.daemon = True  # чтобы поток завершился при остановке приложения
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