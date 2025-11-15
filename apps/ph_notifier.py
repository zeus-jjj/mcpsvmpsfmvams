import asyncio
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import aiogram.exceptions
from aiogram import Router
from aiogram.types import User
from os import getenv
from aiohttp import ClientSession
from datetime import datetime, timedelta
import json
import html
import pytz
import apps.logger as logger
from threading import Thread
import re
#
from modules import MAX_CHARS_USERS_HISTORY, bot, create_connect
import apps.funcs as funcs
from apps.bot_info import bot_info

router = Router()

"""
[key='rating'] добавляет клавиатуру для оценки чего-либо от 1 до 5
[key='custom'] добавляет кастомную кнопку. Примеры:
    [key='custom' btn_text='нажми меня'] добавит кнопку "нажми меня"
    [key='custom' btn_text='текст на кнопке' answer='текст сообщения, которое получит юзер при нажатии на кнопку']
[key='link' url='https:\\www.vk.com', btn_text='текст на кнопке'] добавляет кнопку-ссылку с текстом
"""

KEYBOARDS = {
    "rating": {
        "buttons": [
            {"title":"1️⃣", "call":"1"},
            {"title":"2️⃣", "call":"2"},
            {"title":"3️⃣", "call":"3"},
            {"title":"4️⃣", "call":"4"},
            {"title":"5️⃣", "call":"5"}
        ],
        "answer": "⭐️ Спасибо за оценку! Ваше мнение важно для нас! ⭐️",
        "discord_msg": "Пользователь @{username} выбрал оценку [{value}].\nТекст сообщения: {msg_text}",
        "in_row": 5
        },

    "custom": {
        "buttons": None,
        "answer": "",
        "discord_msg": "Пользователь @{username} нажал кнопку [{value}].\nТекст сообщения: {msg_text}",
        "in_row": 5,
        "params": {
                "btn_text": "'",
                "answer": "'"
                }
        },
    "link": {
        "buttons": None,
        "answer": "",
        "discord_msg": None,
        "in_row": 1,
        "params": {
            "url": "'",
            "btn_text": "'"
            }
        }
    }

# bot = Bot(getenv('BOT_TOKEN'), parse_mode='HTML')
ds_token = getenv('DS_TOKEN')
ds_channel = getenv('DS_CHANNEL')

# файл конфигурации
conf_file = "conf.json"

# шаблон для записи в JSON
DEFAULT_DATA = {
    "last_send": "",
    "errors": {}
}

# для обращения к ДС АПИ
headers = {
        "Authorization": f"Bot {ds_token}",
        "Content-Type": "application/json"
    }

# вызывается при нажатии на любую кнопку в сообщении уведомления с покерхаб (ph)
@router.callback_query(lambda call: call.data.startswith('pokerhub-'))
async def user_click_handler(call, bot):
    params = call.data.split('-')[1:]
    await call.answer()

    # сюда попадёт выборка из БД для нажатой кнокпи, если такая инфа есть в БД
    key_data = None

    # получаем id чата
    chat_id = call.message.chat.id

    # получаем id сообщения
    message_id = call.message.message_id

    # получаем объект чата
    chat = await bot.get_chat(chat_id)

    # получаем информацию о пользователе, который нажал на кнопку
    user: User = chat
    user_id = user.id
    username = user.username
    await funcs.touch_user_activity(user_id)


    # получаем текст самого сообщения
    message_text = call.message.text

    # проверяем, сколько параметров
    if len(params) < 2:
        msg_text = KEYBOARDS[params[0]]['discord_msg'].format(username=username, msg_text=message_text)
    else:
        # проверяем, есть-ли в БД данные для данной кнопки
        db = await create_connect()
        key_data = await db.fetchrow(
            """SELECT id, key_text, answer FROM msg_keys WHERE user_id = $1 AND message_id = $2 AND key_label = $3""",
            str(user_id), message_id, params[1]
        )
        await db.close()

        # если есть
        if key_data:
            msg_text = KEYBOARDS[params[0]]['discord_msg'].format(username=username, value=f"{key_data['key_text']}", msg_text=message_text)
        # если данных нет - отвечаем обычным шаблоном
        else:
            msg_text = KEYBOARDS[params[0]]['discord_msg'].format(username=username, value=f"{params[1]}", msg_text=message_text)
    await logger.info(f"{msg_text}")

    # удаляем сообщение с кнопками из чата пользователя
    await bot.delete_message(chat_id, message_id)

    # удаляем инфу о кнопках из БД
    db = await create_connect()
    await db.execute(
    """DELETE FROM msg_keys WHERE user_id = $1 AND message_id = $2""",
    str(user_id), message_id
    )
    await db.close()

    # если в БД есть ответ для этого сообщения - отправляем его
    if key_data and (answer:=key_data.get("answer", "")):
        await bot.send_message(chat_id=chat_id, text=answer)
    # если нет, отправляем юзеру сообщение в ответ, если есть текст для ответа в шаблонах
    if (message_text:=KEYBOARDS[params[0]]['answer']):
        await bot.send_message(chat_id=chat_id, text=message_text)

    # отправляем событие в дискорд
    await send_to_discord(text=msg_text)

# отправляет сообщение в дискорд
async def send_to_discord(text: str):
    # формируем сообщение
    msg = f"```? @{bot_info.get_username()} [notification from pokerhub]\n{text}```"

    # отправляем сообщение в дискорд
    response = await send_msg(msg=msg)

    # проверяем статускод ответа от дискорда после попытки отправить сообщение
    if response and response.status == 200:
        await logger.info(f"Уведомление о действии юзера отправлено в дискорд!")
    else:
        await logger.error(f"Не удалось отправить сообщение о действии юзера. Ошибка: {response.status}")

# ищет в тексте уведомления вставки клавиатур
async def find_keyboards(text: str):

    # сюда будут попадать кнопки
    keyboards = []
    inline_keyboard = None
    # список кастомных кнопок
    custom_keys = []

    for key, value in KEYBOARDS.items():
        # задаём паттерн
        pattern = rf"\[key='{key}'.*?\]"
        # Ищем все совпадения с заданным паттерном в тексте
        matches = re.findall(pattern, text)

        for match in matches:
            # счётчик добавленных кнопок (чтобы можно было ряды создавать)
            keys_count = 0
            keys = []
            # сюда извлекается текст для ответа юзеру при нажатии им кнопки
            answer = None
            # сюда попадают извлечённые параметры для кастомной клавиатуры
            params = {}

            # если есть в KEYBOARDS кнопки - добавляем их
            if value["buttons"]:
                for btn in value["buttons"]:
                    keys_count += 1
                    if keys_count > value['in_row']:
                        keys_count = 0
                        keyboards.append(keys)
                        keys = []
                    keys.append(InlineKeyboardButton(text=btn["title"], callback_data=f"pokerhub-{key}-{btn['call']}"))
                # добавляем последние кнопки (если они есть)
                if keys:
                    keyboards.append(keys)

            # если же в KEYBOARDS нет кнопок - значит нужно вставить кастомные кнопки,
            # либо это кнопка-ссылка
            else:
                # проходимся по параметрам из KEYBOARDS
                for label, border in value['params'].items():
                    # ищем в тексте параметр
                    param = re.search(rf"{label}={border}([^']+){border}", match)
                    # если найден
                    if param:
                        # извлекаем значение параметра
                        params[label] = param.group(1)

                # проверка, какие параметры были извлечены
                # если это кнопка-ссылка
                if (url:=params.get("url", None)) and (btn_text:=params.get("btn_text", None)):
                    keys.append(InlineKeyboardButton(text=btn_text, url=url))
                # если это просто кнопка с ответом в дискорд при нажатии
                if (btn_text:=params.get("btn_text", None)) and not params.get("url", None):
                    keys.append(InlineKeyboardButton(text=btn_text, callback_data=f"pokerhub-{key}-{len(custom_keys)}"))
                # параметр, отвечающий за текст ответа при нажатии кнопки
                if "answer" in params:
                    answer = params.get("answer", None)

                # добавляем инфу о кнопке в словарь, если это не кнопка-ссылка
                if not url:
                    custom_keys.append({"text": btn_text, "answer": answer, "number": len(custom_keys)})

                # добавляем последние кнопки (если они есть)
                if keys:
                    keyboards.append(keys)

            # убираем из текста паттерн
            text = text.replace(match, "")


    # если кнопки расставлены - создаём клавиатуру
    if keyboards:
        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=keyboards)
    else:
        inline_keyboard = []

    return text, inline_keyboard, custom_keys


async def add_error(user, users_errors, conf_data, err_text):
    # если для этого юзера ещё не было ошибок - создаём список
    if user.get('user_id') not in conf_data['errors']:
        conf_data['errors'][user.get('user_id')] = []

    # если же такая ошибка уже есть в json-конфиге - пропускаем
    if [user.get('id'), user.get('date')] in conf_data['errors'][user.get('user_id')]:
        return users_errors, conf_data

    if user.get('user_id') not in users_errors:
        users_errors[user.get('user_id')] = []

    # добавляем для данного юзера ошибку
    users_errors[user.get('user_id')].append(
        {
            "notification_id":user.get('id'),
            "username":user.get('user'),
            "id":user.get('user_id'),
            "error":err_text,
            "date":user.get('date')
        }
    )
    conf_data['errors'][user.get('user_id')].append([user.get("id"), user.get("date")])

    return users_errors, conf_data


async def send_alerts(notifications, conf_data):
    alerts = [] # id тех, кому удалось отправить уведосление
    users_errors = {} # инфа о тех, кому не удалось отправить уведомление

    for user in notifications: # проходимся по всем юзерам
        if not user.get('user_id'):
            await logger.error(f"Не удалось обнаружить id пользователя для доставки ему уведомления! Данные пользователя: {user}")
            continue
        # отправляем месседж юзеру
        try:
            await logger.debug(f"Попытка отправить уведомление с ПХ с id={user['id']}")
            # убираем теги
            message_text = html.unescape(user.get('text'))
            # пытаемся найти кнопки в сообщении
            message_text, keyboard, custom_keys = await find_keyboards(text=message_text)

            if keyboard:
                sent_message = await bot.send_message(chat_id=user.get('user_id'), text=message_text, reply_markup=keyboard)
                message_id = sent_message.message_id

                # если мы тут - то сообщение удачно отправлено, и можно сделать записи в БД
                # если в сообщении были кастомные кнопки
                if custom_keys:
                    db = await create_connect()
                    # проходимся по кнопкам
                    for key in custom_keys:
                        await db.execute(
                        """INSERT INTO msg_keys (user_id, message_id, key_text, answer, key_label) VALUES ($1, $2, $3, $4, $5)""",
                        user.get('user_id'), message_id, key["text"], key["answer"], str(key["number"])
                    )
                    await db.close()
            # если кнопок нет - отправляем просто текст сообщения
            else:
                await bot.send_message(chat_id=user.get('user_id'), text=message_text)

            await logger.info(f"Уведомление для @{user.get('user')} доставлено!")



            # сохраняем сообщение в jivo_integration_queue
            user_id = int(user.get('user_id'))
            text = f"Пользователь получил автоматическое уведомление с PokerHUB с текстом: \n{message_text}"
            await funcs.add_msg_to_jivo_integration_queue(user_id=user_id, text=text)




            # # Отправляем в JIVO сообщение, чтобы сапорты не теряли контекст
            # username = user.get("user")
            # user_id = user.get('user_id')
            # result = await funcs.send_to_jivo(text=",
            #     user_id=user_id,
            #     intent="Обращение из телеграм" + (f" https://t.me/{username}" if username else "") if username else None,
            #     invite=f"Для просмотра истории переписки с пользователем, можете посетить: https://telegram.pokerhub.pro/profile/{user_id}",
            #     url=f"https://telegram.pokerhub.pro/profile/{user_id}"
            # )
            # if not result:
            #     await logger.error(f"Сообщение от юзера {user_id} не доставлено в JIVO!")















            # записываем в историю сообщений уведомление
            await funcs.add_msg_to_history(chat_id=user.get('user_id'), author_id="system", content=f"Пользователь получил автоматизированное сообщение с ПХ с текстом: {message_text}")

            # записываем в БД в историю юзера
            db = await create_connect()
            await db.execute(
                """INSERT INTO user_history (user_id, text) VALUES ($1, LEFT($2, $3))""", int(user.get('user_id')),
                f"Получил уведомление с PokerHUB: {message_text}", MAX_CHARS_USERS_HISTORY
            )
            await db.close()

            # сохраняем в список, кому удалось отправить
            alerts.append(user['id'])

            # await bot.send_message(chat_id="5762455571", text=message_text) # отправляю себе (для теста)

        # если не удалось отправить, сохраняем его в словарь
        except aiogram.exceptions.TelegramNetworkError as error:
            # Ошибка соединения с телеграм
            continue

        except aiogram.exceptions.TelegramRetryAfter as error:
            # сработала антиспам система телеги. Пропускаем
            continue

        except aiogram.exceptions.TelegramNotFound as error:
            err_text = f"Пользователь с таким id не найден в телеграм\n{error}"
            await logger.error(err_text)
            # Закроем это уведомление чтобы не пытаться его обработать повторно
            alerts.append(user['id'])
            users_errors, conf_data = await add_error(user=user, users_errors=users_errors, conf_data=conf_data, err_text=err_text)

        except aiogram.exceptions.TelegramBadRequest as error:
            if "chat not found" in str(error):
                err_text = f"Пользователь не запускал бота! Сообщите ему, чтобы запустил @{bot_info.get_username()}\n{error}"
            elif "can't parse entities" in str(error):
                err_text = f"В тексте уведомления содержатся неподдерживаемые теги/символы!\n{error}"
            elif "BUTTON_DATA_INVALID" in str(error):
                err_text = f"Ошибка в создании inline-клавиатуры!\n{error}"
            else:
                err_text = f"Некорректный запрос к telegram: {error}"
            await logger.error(f"Уведомление для @{user.get('user')} не доставлено! {err_text}")
            users_errors, conf_data = await add_error(user=user, users_errors=users_errors, conf_data=conf_data, err_text=err_text)

        except aiogram.exceptions.TelegramForbiddenError as error:
            err_text = f"Пользователь заблокировал бота! Уведомите, чтобы перезапустил его: @{bot_info.get_username()}\n{error}"
            await logger.error(err_text)
            # Закроем это уведомление чтобы не пытаться его обработать повторно
            alerts.append(user['id'])
            users_errors, conf_data = await add_error(user=user, users_errors=users_errors, conf_data=conf_data, err_text=err_text)

        except aiogram.exceptions.TelegramServerError as error:
            # Ошибка на стороне сервера телеграм
            await logger.error(f"Ошибка на стороне сервера ТГ: {err_text}")
            continue

        except aiogram.exceptions.ClientDecodeError as error:
            err_text = f"Не удалось декодировать данные\n{error}"
            await logger.error(err_text)
            users_errors, conf_data = await add_error(user=user, users_errors=users_errors, conf_data=conf_data, err_text=err_text)

        except aiogram.exceptions.TelegramAPIError as error:
            # Ошибка при обращении к телеграм АПИ
            await logger.error(f"Ошибка при обращении к телеграм АПИ {error}")
            continue


        except Exception as error:
            err_text = f"Необработанная ошибка: {error}"
            await logger.error(f"Ошибка при отправке уведомления в ТГ: {error}")
            users_errors, conf_data = await add_error(user=user, users_errors=users_errors, conf_data=conf_data, err_text=err_text)


    # закрываем тех, кому удалось отправить
    await close_notifications(alerts)

    # отправляем в ДС таски
    await discord_alert(users_errors=users_errors, conf_data=conf_data)

    # обновляем дату отправки
    if not conf_data['last_send']:
        await update_date(errors=conf_data['errors'])
    # если прошло более суток
    elif (datetime.now() - datetime.strptime(conf_data['last_send'], '%Y-%m-%d %H:%M:%S')) > timedelta(days=1):
        await update_date()
    else:
        # сохраняем данные обратно в файл
        await save_config(conf_data)

async def get_notifications():
    # запрашиваем уведомления с покерхаб
    async with ClientSession() as session:
        async with session.get('https://pokerhub.pro/api/getnotifications') as response:
        # async with session.get('http://localhost:2288/api/getnotifications') as response: # для локального теста
            try:
                resp = await response.json()
                # получаем текущее время по МСК
                msk_tz = pytz.timezone('Europe/Moscow')
                now_msk = datetime.now(msk_tz).replace(tzinfo=None)
                # await logger.debug(f"Будут отобраны уведомления чьё время < {str(now_msk)}")
                # фильтруем только те, которые уже нужно отправить
                need_send = list(filter(lambda data: now_msk > datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S'), resp))

                # await logger.debug(f"Нужно отправить уведомления: {need_send}")
            except Exception as error:
                await logger.error(f'Ошибка PokerHub: {error}')
                return None
    # возвращаем фильтрованный JSON
    return need_send

async def close_notifications(users: list):
    await logger.debug(f"Будут закрыты уведомления с id: {users}")
    if not users:
        return
    # отправляем запрос на закрытие уведомлений, которые удалось отправить
    async with ClientSession() as session:
        url = f'https://pokerhub.pro/api/updatenotifications?ids={",".join([str(user) for user in users])}'
        async with session.get(url) as response:
            await logger.debug(f"Ответ от {url}: {response.status}")

async def discord_alert(users_errors: dict, conf_data: dict):
    # для формирования в ДС уведомления, кому не удалось отправить сообщения
    if not users_errors:
        return

    errors_msg = [f"!task **@{bot_info.get_username()}**: У данных пользователей возникли ошибки при рассылке им уведомлений с *PokerHub*:\n\n"]

    for alert in users_errors:
        errors_msg.append(f"```@{users_errors[alert][0].get('username')} (id{users_errors[alert][0].get('id')})\n")
        for data in users_errors[alert]:
            # добавляем текст для уведомлений
            errors_msg[-1] += f"id уведомления: {data.get('notification_id')}\nОшибка: {data.get('error')}\nУведомление должно было быть доставлено {data.get('date')}\n\n"
        errors_msg[-1] += "```"

    # если ошибок меньше или равно step, отправляем разом
    step = 4 # по сколько сообщений об ошибках будет в одном месседже в дискорд
    if len(errors_msg) <= step:
        response = await send_msg("".join(errors_msg))
    else:
        # Нарезаем список на группы по step элементов
        sliced = [errors_msg[i:i + step] for i in range(0, len(errors_msg), step)]

        # отправляем, склеивая строки
        for msg in [''.join(group) for group in sliced]:
            response = await send_msg(msg)

    # проверяем статускод ответа от дискорда после попытки отправить сообщение
    if response and response.status == 200:
        await logger.info(f"Уведомление об ошибках отправлено в дискорд!")
    else:
        await logger.error(f"Не удалось отправить сообщение в канал: {ds_channel}. Ошибка: {response.status}")

async def send_msg(msg: str):
    # для отправки сообщения в ДС
    try:
        # Url АПИ
        url = f"https://discord.com/api/v9/channels/{ds_channel}/messages"
        # сообщение
        payload = {"content": msg}
        async with ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                return response
    except Exception as error:
        await logger.error(f"При отправке уведомления в дискорд возникла ошибка: {error}")
        return None


async def update_date(errors=None):
    # обновляет в JSON дату последней отправки
    data = await load_config()
    data['last_send'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['errors'] = {} # обнуляем ошибки
    # если передан словарь ошибок - записываем его тоже
    if errors:
        data['errors'] = errors
    await save_config(data=data)

async def load_config():
    # загружает данные из JSON-файла
    """
    {
        "last_send":"дата отправки в ДС таска",
        "errors": {
            "tg_id": [["id","date"],["id", "date"]],
            "tg_id": [["id","date"],["id", "date"]]
        }
    }
    """
    try:
        with open(conf_file, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        with open(conf_file, 'w') as file:
            json.dump(DEFAULT_DATA, file, indent=4)
        data = DEFAULT_DATA
    except json.JSONDecodeError:
        await logger.error(f"Ошибка при чтении файла {conf_file}. Файл не является допустимым JSON.")
        data = DEFAULT_DATA
    return data

async def save_config(data):
    try:
        with open(conf_file, 'w') as file:
            json.dump(data, file, indent=4)
    except json.JSONDecodeError:
        await logger.error(f"Ошибка при записи данных в файл {conf_file}.")
    except Exception as e:
        await logger.error(f"Произошла ошибка: {e}")


async def main():
    # главная функция. Запускает алгоритм рассыльщика
    await logger.info("Модуль рассыльщика уведомлений с ПокерХаб успешно запущен!")
    while True:
        try:
            # считываем json
            conf_data = await load_config()
            # если есть уведомления
            if (notifications := await get_notifications()):
                await send_alerts(notifications=notifications, conf_data=conf_data)
        except Exception as error:
            await logger.error(f"Возникла ошибка в главном цикле: {error}")
        finally:
            await asyncio.sleep(180)


if __name__ == "__main__":
    asyncio.run(main())
