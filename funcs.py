"""
здесь содержатся функции для бота, такие как проверка подписки на канал,
парсер клавиатур из json, запись в БД истории юзера, отправка сообщений и т.д.
"""

import html
import copy
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from aiogram.utils.keyboard import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.types.web_app_info import WebAppInfo
from aiogram.types import ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from cryptography.fernet import Fernet
#
from apps import bot_info
import apps.logger as logger
import apps.file_id_uploader as file_uploader
from modules import MAX_CHARS_USERS_HISTORY, create_connect, get_key_b64, FSMStates, dp, headers, get_host
from modules import JIVO_INTEGRATOR_URL
# для заявки АМО
import apps.amo_leads as amo_leads

# получаем ключ из переменных окружения
key_b64 = get_key_b64()

bot_lead_stages = {
    "default": "1-start",
    "start": "1-start",
    "from_group": "2-free learn",
    "free_learning": "2-free learn",
    "start_learning": "2-free learn",
    "quiz_free_learning" : "2-free learn",
    "closed_community" : "3-zs",
    "spin" : "4-spin",
    "4-spin": "4-spin",
    "mtt" : "4-mtt",
    "4-mtt": "4-mtt",
    "cash" : "4-cash",
    "4-cash": "4-cash",
    "is_ph_registered_spin": "5-spin",
    "is_ph_registered_mtt": "5-mtt",
    "is_ph_registered_cash": "5-cash",
    "course_manager" : "6-wlcm",
    "7-wlcmd": "7-wlcmd",
    "8-fld": "8-fld",
    "9-fld": "9-fld",
    "iq_quiz": "1-quizstart",
    "iq_quiz_results": "2-res",
    "notif_iq_quiz": "3-quizd",
    "tg_media": "1-rasilk",
    "motivation_1": "10-fld",
    "wildwest_1": "11-fld",
    "dima": "12-fld",
    "select_quiz_results_spin": "2-sq_spin",
    "select_quiz_results_mtt": "2-sq_mtt",
    "select_quiz_results_cash": "2-sq_cash",
    "12startdog": "1.2-startdog",
    "13startdog": "1.3-startdog",
    "41cash": "4.1-cash",
    "iq_quiz_vk": "1-vkquiz",
    "check_subs-channel": "2-vkquiz",
    "leaderboard": "1-dogrev_vkq",
    "leaderboardqs": "1-dogrev_qs",
    "iqdiscipqs": "2-dogrev_qs",
    "iqdiscip": "2-dvkspin",
    "notif_case_1": "получил уведомление с кейсом",
    "thre": "Регнулся на эфир",
    "not1": "Уведомление за час до начала эфира",
    "iq_quiz_vk_res": "3-vkquiz",
    "vk_iq_quiz_results": "получил результаты iq-теста по вк-воронке",
    "gift_iq_result": "получил в подарок бесплатный доступ к курсам",
    "get_iq_result": "получил результат iq-теста (ВК-ветка)",
    "select_quiz": "1-selectquiz",
    "spinquiz21":"2.1-spinquiz",
    "spinquiz22":"2.2-spinquiz",
    "spinquiz23":"2.3-spinquiz",
    "spinquiz":"1-spinquiz",

    "11-fld": "11-fld",
    "12-fld": "12-fld",
    "1.4-case": "1.4-case",
    "4.1-cash": "4.1-cash",
}

# этот класс нужен для безопасного использования format, чтобы не возникали исключения
# в случаях, когда плейсхолдера переданного в параметры format() в строке нет
class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

# При вызове обновляет данные в БД в funnel, записывая все этапы воронок из bot_lead_stages
async def update_funnel_db():
    db = await create_connect()
    await db.execute("""DELETE FROM funnel;""")
    for key, label in bot_lead_stages.items():
        await db.execute("""INSERT INTO funnel (label, key) VALUES ($1, $2);""", label, key)
    await db.close()
    await logger.info(f"В БД в funnel обновлены данные по этапам воронки ({len(bot_lead_stages.keys())})")

async def add_msg_to_jivo_integration_queue(user_id, text):
    """
    Для добавления сообщения в jivo_integration_queue.
    Задача от Академика:
    Сейчас когда ПХ шлет "тригерное сообщение" игроку, мы его в Jivo видим как новый чат.
    Можно сделать так, чтобы мы этого не видели?
    Т.е. новый чат-диалог создавался только в случае если человек ответил нам?
    """
    db = await create_connect()
    await db.execute(
        """INSERT INTO jivo_integration_queue (user_id, text)
            VALUES ($1, $2)""", user_id, text)
    await db.close()

async def get_msgs_to_jivo_integration_queue(user_id):
    """
    Получает сообщения из jivo_integration_queue для юзера,
    которые is_active и у которых не прошло 2 недели с момента их отправки в ТГ
    (это нужно для задачи от Академика, описание в функции add_msg_to_jivo_integration_queue)
    """

    # сколько времени должно пройти, чтобы не учитывать сообщение
    delta_time = datetime.utcnow() - timedelta(weeks=2)
    db = await create_connect()
    rows = await db.fetch(
        """
        SELECT text, create_at FROM jivo_integration_queue
        WHERE user_id = $1
          AND is_active = TRUE
          AND create_at >= $2
          ORDER BY id ASC
        """,
        user_id,
        delta_time
    )

    await db.close()
    # возвращаем список словарей
    return [dict(row) for row in rows] if rows else []

async def deactivate_msgs_for_user(user_id, end_date):
    """
    Деактивирует (ставит is_active в false) записи для юзера от начала и до даты end_date
    """
    db = await create_connect()
    await db.execute(
        """
        UPDATE jivo_integration_queue
        SET is_active = FALSE
        WHERE user_id = $1
          AND create_at <= $2
        """,
        user_id,
        end_date
    )

    await db.close()


async def send_to_jivo(user_id, text=None, file_type=None, file_path=None, file_name=None, **kwargs):
    try:

        data = {
            "message": {
                "type": file_type if file_type else "text",
                "file": file_path,
                "file_name": file_name,
                "text": text
            },
            "sender": {
                "id": str(user_id)
            },
            "service": {
                "source": "telegram",
                "object": "bot",
                "object_id": str(bot_info.bot_info.get_id())
            }
        }

        if not text and not file_path:
            await logger.error(f"Нет данных для отправки сообщения! Данные: {data}")
            return None

        # Добавляем параметры из kwargs
        data['sender'].update(kwargs)

        # Принимаем 3 попытки отправить сообщение в jivo
        for _ in range(1, 3):
            async with aiohttp.ClientSession() as session:
                # async with session.post("https://tg-intensive.firestorm.team/jivo/api/v1/jivo/send_message", json=data) as response:
                async with session.post(f"{JIVO_INTEGRATOR_URL}/api/v1/jivo/send_message", json=data) as response:
                    if response.status == 200:
                        await logger.info(f"Сообщение доставлено в JIVO, статускод: {response.status}, ответ от сервера: {await response.text()}")
                        result = await response.json()
                        return result
                    else:
                        await logger.error(f"Сообщение не доставлено в JIVO, попытка #{_}. Статускод: {response.status}, ошибка: {await response.text()}")
                        await asyncio.sleep(0.6)
                        continue
        return False
    except Exception as error:
        await logger.error(f"Произошла ошибка при попытке доставить сообщение в JIVO: {error}. data={data}")
        return None

# Функция вызывается при успешной доставке сообщения юзеру, и добавления его в веб-панель в историю сообщений
async def add_msg_to_history(chat_id, author_id, content, type="text", name=None):
    body = {
        'content': content,
        'chat_id': chat_id,
        'author_id': author_id,
        'type': type,
        'name': name
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(get_host()+'/messages_history/add-message', json=body) as response:
            text = await response.text()
            try:
                r = json.loads(text)
                return True
            except json.JSONDecodeError as error:
                await logger.error(f"Не удалось сохранить сообщение в истории: {error}. Текст сообщения: {text}")
                return False

# для записи сообщений от бота (чтобы видеть продвижение по воронке)
async def save_user_funnel(user_id, label):
    """
    label - это то как сообщение названо в МИРО (из таблицы соответствий)
    name - это то как в MAP названо сообщение
    """
    if not label:
        return
    db = await create_connect()
    await db.execute(
        """INSERT INTO user_funnel (user_id, label, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, label)
            DO NOTHING""",
        user_id, bot_lead_stages.get(label, label), label
        )
    await db.close()

# Сохраняет в БД инфу, что юзер прошёл воронку
async def save_funnel_passed(user_id, funnel_name):
    db = await create_connect()
    await logger.info(f"Юзер {user_id} прошёл воронку. Уведомления он более получать не будет!")
    await db.execute(
            """
            INSERT INTO funnel_passed (user_id, funnel_name, passed)
            VALUES ($1, $2, $3)
            """,
            user_id,
            funnel_name,
            True
        )
    await db.close()
    return True


async def close_old_notifications(user_id, callback):
    db = await create_connect()
    row = await db.fetch(
        """
        SELECT label FROM user_funnel
        WHERE user_id = $1
        AND name = $2
        """,
        user_id,
        callback
    )

    if row:
        # значит он перешёл на сообщение на котором уже был
        await logger.debug(f"Юзер {user_id} уже был на сообщении {callback} ранее")
    else:
        await logger.debug(f"Юзер {user_id} не был на сообщении {callback} ранее, закрываем все уведомления!")
        await db.execute(
            """
            UPDATE notifications
            SET is_active = $1
            WHERE user_id = $2
            """,
            False,
            user_id
        )
    await db.close()

async def get_quiz_results(bot, user_id):
    import mysql.connector
    from modules import MYSQL_CONFIG
    from apps.iq_quiz import results
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    select_query = """
    SELECT iq_score FROM quiz_sessions WHERE telegram_id = %s
    ORDER BY id DESC
    LIMIT 1
    """
    get_scores = False
    cursor.execute(select_query, (user_id,))
    for i in range(5):
        iq_score = cursor.fetchone()
        if not iq_score:
            await logger.error(f"Не удалось извлечь iq_score пользователя {user_id}, попытка №{i+1}/{5}...")
            await asyncio.sleep(5)
        else:
            get_scores = True
            break
    db = await create_connect()
    if get_scores:
        iq_score = int(iq_score[0])
        cursor.close()
        conn.close()
        for res_data in results:
            if res_data['score'] >= iq_score:
                msg_data = {
                    "text": f"Ваш IQ Score: {iq_score}\n\nУровень: {res_data['level']}\n\n{res_data['text']}"
                }
                await send_message(bot=bot,
                    user_id=user_id,
                    msg_data=msg_data,
                    route="vk_iq_quiz_results",
                    notification=True
                    )
                break
        rows = await db.execute(
            """
            DELETE FROM events
            WHERE user_id = $1
            AND event_type = $2
            """,
            user_id,
            "iq_quiz_vk"
        )
        await db.execute(
            """
            INSERT INTO funnel_history (user_id, label)
            VALUES ($1, $2)
            """,
            user_id,
            "подписался на канал для получения резов iq-квиза"
        )
    # Отключаем уведомление
    await db.execute(
        """
        UPDATE notifications
        SET is_active = $1
        WHERE user_id = $2
        AND label = $3
        """,
        False,
        user_id,
        "get_iq_result"
    )
    await db.close()
    return True

# сохраняем все переходы по боту
async def save_funnel_history(user_id, label):
    if label:
        db = await create_connect()
        await db.execute(
            """INSERT INTO funnel_history (user_id, label)
               VALUES ($1, $2)""",
            user_id, bot_lead_stages.get(label, label)
            )
        await db.close()
    else:
        await logger.error(f"В словаре соответствий не найден этап {label}")
        return None

# для сохранения в БД ивента
async def save_event(user_id, event, rewrite=False):
    db = await create_connect()
    if rewrite:
        await db.execute(
            """INSERT INTO events (user_id, event_type, event_date)
               VALUES ($1, $2, $3)
               ON CONFLICT (user_id, event_type)
               DO UPDATE SET event_date = EXCLUDED.event_date""",
            user_id, event, datetime.utcnow()
        )
    else:
        await db.execute(
            """INSERT INTO events (user_id, event_type, event_date)
               VALUES ($1, $2, $3)
               ON CONFLICT (user_id, event_type)
               DO NOTHING""",
            user_id, event, datetime.utcnow()
        )
    await db.close()

# для проверки регистрации на PokerHub
async def is_pokerhub_registered(user_id):
    input_data = {
        "users": [user_id]
        }
    async with aiohttp.ClientSession() as session:
        async with session.post('https://pokerhub.pro/api/tg/getusers', json=input_data) as response:
            # Проверка успешности запроса
            if response.status == 200:
                data = await response.json()
                return True if data else False
            else:
                return False

# запускает для конкретного юзера машину состояний
async def set_user_state(bot, user_id: int, collect_data: list, if_collected: str):
    # если мы включаем FSM для последующей отправки в АМО, то проверяем активную заявку
    if if_collected == "send_amo":
        # если у юзера уже есть активная заявка в амо
        amo_lead = await amo_leads.check_active_lead(user_id=user_id)
        if amo_lead:
            return False

    key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
    context = FSMContext(storage=dp.storage, key=key)
    await context.clear()  # Очищаем состояние перед обновлением данных
    await context.update_data({"collect": copy.deepcopy(collect_data)})
    await context.update_data(if_collected=if_collected)
    await context.set_state(FSMStates.fsm_context)
    return True

# проверяет, указал-ли юзер данные в запущенной FSM-машине
async def check_fsm_data(bot, user_id, fsm_data):
    key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
    context = FSMContext(storage=dp.storage, key=key)
    current_state = await context.get_state()
    if current_state:
        user_data = await context.get_data()
        collected_data = {
            data.get("name"): data.get("value")
            for data in user_data.get("collect", [])
            if data.get("value") is not None
        }
        if fsm_data in collected_data.keys():
            return True
        else:
            return False
    # возвращаем True если машина состояний не запущена (т.к. это значит что она была успешно завершена)
    else:
        return True

# возвращает True, если FSM запущена. Иначе False
async def is_fsm_active(bot, user_id):
    key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
    context = FSMContext(storage=dp.storage, key=key)
    # если машина запущена
    if await context.get_state():
        return True
    return False

# функция останавливает работу машины состояний
async def stop_fsm(bot, user_id):
    key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
    context = FSMContext(storage=dp.storage, key=key)
    # если машина запущена
    if await context.get_state():
        # отключаем
        await context.clear()
        return True
    # если машина состояний и так отключена - возвращаем False
    return False

# экранирует строку от символов <, > и &, чтобы не было ошибок, т.к. parse_mode=HTML
def escape_string(input_string):
    # < > и & будут отображаться в сообщении, но ТГ не будет их воспринимать как теги,
    # поэтому ошибки не будет. Либо можно вообще их удалять через replace
    if input_string is None:
        return ""
    return html.escape(input_string)

# получаем словарь инфы о юзере
async def get_user_info(bot, user_id):
    try:
        # Получение информации о пользователе
        user = await bot.get_chat(user_id)
        username = user.username
        first_name = user.first_name
        last_name = user.last_name
        return {"username": username, "first_name": first_name, "last_name": last_name}
    except Exception as e:
        await logger.error(f'Ошибка получения информации о пользователе: {e}')
        return {"username": "unknown", "first_name": "друг", "last_name": ""}

async def encrypt_message(message):
    f = Fernet(key_b64)
    encrypted_message = f.encrypt(str(message).encode())
    return encrypted_message.decode('utf-8')


# для подстановки значений в плейсхолдеры
async def placeholders_replace(data, user_id, user_data):
    if data:
        if "{crypted_user_id}" in data:
            # подменяем плейсхолдер
            data = data.format(crypted_user_id = await encrypt_message(message=user_id))
        if "{user_id}" in data:
            data = data.format(user_id = user_id)

        # тут добавляем всевозможные значения, которые будем пытаться подставить в строку сообщения
        placeholders = SafeDict(**user_data)
        # объявляем объекты для сообщения
        data = data.format_map(placeholders)
    return data


# эта функция для сборки клавиатур на основе списка кнопок из json
async def get_keyboard(buttons_list, user_id, user_data):
    # сюда будем собирать кнопки
    buttons = []
    keyboard_type = None
    # проходимся по рядам кнопок
    for row in buttons_list:
        buttons.append([]) # добавляем ряд для кнопок (этот список наполняем далее кнопками)
        # проходимся по кнопкам в ряду
        for button in row:
            if button.get("type", None) == "button":
                request_contact = button.get("request_contact", None)
                buttons[-1].append(KeyboardButton(text=button["title"], request_contact=request_contact))
                if not keyboard_type:
                    keyboard_type = ReplyKeyboardMarkup
            else:
                # ищем web_app
                if (web_app_url:=button.get("web_app", None)):
                    web_app = WebAppInfo(url=await placeholders_replace(data=web_app_url, user_id=str(user_id), user_data=user_data))
                # пытаемся найти ссылку для этой кнопки
                if (link:=button.get("link", None)):
                    # если в ссылке есть плейсхолдеры, они будут подставлены
                    link = await placeholders_replace(data=link, user_id=str(user_id), user_data=user_data)
                # если ссылки для кнопки нет, то делаем callback для неё. Иначе колбек = False
                callback = button.get('callback', None) if not link else None
                # добавляем кнопку
                buttons[-1].append(InlineKeyboardButton(text=button['title'], callback_data=callback, url=link if link else None, web_app=web_app if web_app_url else None))
                if not keyboard_type:
                    keyboard_type = InlineKeyboardMarkup
    # тут делаем клаву
    if keyboard_type == ReplyKeyboardMarkup:
        return keyboard_type(keyboard=buttons, resize_keyboard=True)
    elif keyboard_type == InlineKeyboardMarkup:
        return keyboard_type(inline_keyboard=buttons, resize_keyboard=True)
    else:
        return None


# для отправки данных юзера в АМО СРМ
async def send_amo(bot, user_id):
    # получаем данные из FSM
    key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
    context = FSMContext(storage=dp.storage, key=key)
    current_state = await context.get_state()
    if current_state:
        try:
            data = await context.get_data()
            # формируем тело сообщение, переводя транслитом
            collected_data = {item.get('name'): item.get('value') for item in data.get("collect") if item.get('value')}
            # если никакие данные собраны не были
            if not collected_data:
                return False

            # добавляем доп. данные (юзернейм, id) на случай, если они нужны в заявке
            collected_data.update(data.get('addition_data', {}))

            # создание/обновление заявки
            lead_id = await amo_leads.process_lead(data=collected_data,
                lead_id=data.get("lead_id", None))

            # если успешно создана/обновлена заявка
            if lead_id is not None:
                # добавляем id лида в машину состояний
                await context.update_data(lead_id=lead_id)
                return True
            else:
                return False
        except Exception as error:
            await logger.error(f"Не удалось отправить заявку в АМО от юзера {user_id}. Ошибка: {error}")
            return False

    # если FSM не запущена, значит он уже отправил данные в АМО
    else:
        return True


# роверяет, подписался-ли юзер на канал
async def check_subs(channel, user_id, bot):
    try:
        user_channel_status = await bot.get_chat_member(chat_id=channel, user_id=user_id)
    except Exception as error:
        await logger.error(f"Не удалось проверить подписчика канала {channel}. Возможно, бот не является админом канала. Ошибка: {error}")
        return None
    else:
        if user_channel_status.status != 'left':
            await add_history(user_id=user_id, text=f"Подписался на {channel}")
            return True
        else:
            return False

# для записи в БД действие юзера
async def add_history(user_id, text):
    db = await create_connect()
    await db.execute(
        """INSERT INTO user_history (user_id, text) VALUES ($1, LEFT($2, $3))""", user_id,
        text, MAX_CHARS_USERS_HISTORY
    )
    await db.close()

# для проверки действий юзера
async def run_action(action, user_id, bot):
    result = False
    func = action.get("func", "")
    # если это действие - проверка подписки на канал
    if func == "check_subs":
        channel = action.get("channel")
        result = await check_subs(channel=channel, user_id=user_id, bot=bot)
    # проверка перехода и реги на покерхаб
    elif func == "check_pokerhub":
        # возвращает True если юзер зареган
        result = await is_pokerhub_registered(user_id=user_id)
    # запуск машины состояний для юзера для сбора данных
    elif func == "start_fsm":
        collect_data = action.get("collect_data")
        if_collected = action.get("if_collected")
        result = await set_user_state(bot=bot, user_id=user_id, collect_data=collect_data, if_collected=if_collected)
    # функция для проверки, оставил-ли юзер нужные данные в FSM
    elif func == "check_fsm_data":
        fsm_data = action.get("data_name")
        result = await check_fsm_data(bot=bot, user_id=user_id, fsm_data=fsm_data)
    # функция для отключения FSM
    elif func == "stop_fsm":
        result = await stop_fsm(bot=bot, user_id=user_id)
    # проверка, запущена-ли FSM. True если запущена, иначе False
    elif func == "is_fsm_active":
        result = await is_fsm_active(bot=bot, user_id=user_id)
    # функция для отправки данных от юзера из FSM в АМО СРМ
    elif func == "send_amo":
        result = await send_amo(bot=bot, user_id=user_id)
    # функция отправляет медиафайл (или просто файл)
    elif func == "send_file":
        result = await send_message(bot=bot, user_id=user_id, msg_data=action, route=action.get('label'))
    # функция для отправки результатов iq-квиза
    elif func == "get_iq-res":
        await get_quiz_results(bot=bot, user_id=user_id)
        return True
    # функция помечает юзера в БД в таблице funnel_passed, что юзер прошёл воронку
    elif func == "funnel_passed":
        # Передаём пока имя воронки - default, т.к. пока не знаю как разные воронки отслеживать
        await save_funnel_passed(user_id=user_id, funnel_name="default")
        return True
    # функция закрывает отложеное уведомление по callback-названию
    elif func == "close_notifications":
        """
        "action": {
            "func": "close_notifications",
            "labels": ["12startdog"],
            "is_ok": "pass",
            "reverse_result": true
        },
        """
        # закрывает уведомление в БД по его id
        db = await create_connect()
        for label in action.get('labels'):
            await db.execute("UPDATE notifications SET is_active = $1 WHERE user_id = $2 AND label = $3 AND is_active = $4", False, user_id, label, True)
        await db.close()
        result = True
    # функция закрывает все отложеное уведомления
    elif func == "close_all_notifications":
        """
        "action": {
            "func": "close_all_notifications",
            "is_ok": "pass",
            "reverse_result": true
        },
        """
        # закрывает уведомление в БД по его id
        db = await create_connect()
        await db.execute("UPDATE notifications SET is_active = $1 WHERE user_id = $2 AND is_active = $3", False, user_id, True)
        await db.close()
        result = True
    # функция для записи данных в гегл-таблицу
    elif func == "push_gsheet":
        # ТУТ БУДУТ ДАННЫЕ
        result = True
    # функция просто возвращает True, ничего больше не делая
    elif func == "return_ok":
        result = True

    # проверяем результат выполнения функции, меняем Traue на False если есть "reverse_result"
    return result if not action.get("reverse_result", False) else not result

# общая функция для отправки сообщения
async def send_message(bot, user_id, msg_data, persona="default", route="start", user_data={}, notification=False):

    text = msg_data.get("text", None)
    # user_data отсутствует, если сообщение отправляется как отложенное уведомление
    if not user_data:
        user_data = await get_user_info(bot=bot, user_id=user_id)
    # тут мы экранируем символы < > и &, чтобы не было ошибок при отправке сообщения
    user_data = {key: escape_string(value) for key, value in user_data.items()}

    # вставляем данные на места плейсхолдеров, если они есть
    text = await placeholders_replace(data=text, user_id=user_id, user_data=user_data) if text else None

    file=msg_data.get("file", None)
    files=msg_data.get("files", None)
    file_path = None
    files_group = None
    thumbnail_path = None
    content_type = None
    filename = None

    if type(files) == list and len(files) > 0:

        files_group = files

    # проверяем, есть-ли файл для отправки
    elif type(file) == dict:
        # путь к отправляемому файлу
        file_path = file.get("file_path", None)

        # имя файла, как он будет отображаться в ТГ
        filename = file.get("tg_filename", None)

        # путь к файлу превьюшки файла
        thumbnail_path = file.get("thumbnail", None)

        # тип файла
        content_type = file.get("content_type", 'document')

    # проверяем, есть-ли кнопки для этого сообщения
    keyboard = None
    if (buttons_list:=msg_data.get("buttons", None)):
        keyboard = await get_keyboard(buttons_list=buttons_list, user_id=user_id, user_data=user_data)
    # если есть метка, что нужно удалить клаву
    elif msg_data.get("remove_keyboard", False):
        keyboard = ReplyKeyboardRemove()

    """
    bot=экземпляр бота
    chat_id=id чата куда отправить
    label=метка для записи в БД, чтобы по ней ориентироваться в файлах
    filepath=путь к файлу
    text=текст сообщения
    thumbnail_path=путь к картинке превьюшки (необязательный параметр)
    filename=имя файла, которое будет отображаться в ТГ
    content_type=тип отправляемого файла (записываем в БД)
    """

    # если есть файл, который нужно отправить, то отправляем через эту функцию
    if (file and file_path) or files_group:
        result = await file_uploader.send_file_by_label(
            bot=bot,
            chat_id=user_id,
            label=persona,
            filepath=file_path,
            files_group = files_group,
            text=text,
            thumbnail_path=thumbnail_path,
            filename=filename,
            content_type=content_type,
            reply_markup=keyboard
        )
    # если сообщение без файла, то отправляем вот таким образом
    else:
        try:
            result = True if await bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode='HTML',
                reply_markup=keyboard
            ) else False
        except Exception as error:
            await logger.error(f"Ошибка при отправке сообщения для юзера с id={user_id}. Ошибка: {error}")
            result = False

    # если успешно отправлено
    if result:
        # если есть что записать в историю юзера
        if (user_history:=msg_data.get("user_history", None)):
            await add_history(user_id=user_id, text=user_history)
            await add_msg_to_history(chat_id=user_id, author_id="-1", content=user_history, type="text")
        # записываем историю перехода по боту (тут все переходы записываем)
        await save_funnel_history(user_id=user_id, label=route)
        # записываем/обновляем продвижение юзера по воронке бота (тут только уникальные переходы)
        await save_user_funnel(user_id=user_id, label=route)

    if notification and result and (notifications:=msg_data.get('notifications', None)):
        from apps.notifier import notificator
        await notificator.add_notifications(user_id=user_id,
            notifications=notifications)

    # возвращаем результат отправки (True/False)
    return result
