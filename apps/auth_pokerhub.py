import os
import aiohttp
from aiogram import Router
#
import apps.logger as logger
from modules import MAP
from apps.funcs import send_message, add_history, touch_user_activity
router = Router()

# 401 - нет токена
# 402 - нет тг id
# 403 - Токен не найден
# 404 - Токен просрочен
# 201 - Успешно зареегистрирован
# 202 - Успешно авторизован

keycodes_answers = {
    201: "<b>Вы успешно зарегистрировались на сайте </b><a href=\"https://pokerhub.pro\">PokerHUB</a><b>, теперь можно вернуться обратно на сайт</b>",
    202: "<b>Вы успешно авторизовались на сайте </b><a href=\"https://pokerhub.pro\">PokerHUB</a><b>, теперь можно вернуться обратно на сайт</b>",
    404: "<b>Прошло слишком много времени, отведенного на авторизацию!</b>\n\nПожалуйста, перейдите на <a href=\"https://pokerhub.pro\">сайт</a>, и попробуйте пройти авторизацию ещё раз!",
    403: "<b>Ошибка в ссылке для авторизации!</b>\n\nПожалуйста, перейдите на <a href=\"https://pokerhub.pro\">сайт</a>, и попробуйте пройти авторизацию ещё раз!",
    402: "<b>Ошибка в ссылке для авторизации!</b>\n\nПожалуйста, перейдите на <a href=\"https://pokerhub.pro\">сайт</a>, и попробуйте пройти авторизацию ещё раз!",
    401: "<b>Ошибка в ссылке для авторизации!</b>\n\nПожалуйста, перейдите на <a href=\"https://pokerhub.pro\">сайт</a>, и попробуйте пройти авторизацию ещё раз!"
}

# для запроса с ПХ данных по юзеру по его auth-коду
async def get_user_data_auth(auth_code):
    if not auth_code:
        return {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://pokerhub.pro/api/tg/get-tokens",
                json={"tokens": [auth_code]},
                headers={'Content-Type': 'application/json; charset=utf-8'},
                timeout=30
                ) as response:
                user_data = await response.json()
                return user_data[0] if user_data else {}
    except Exception as error:
        await logger.error(f"Не удалось запросить данные о юзере с ПХ: {error}")
        return {}

# для авторизации в ПХ
async def start_auth(bot, message, auth_code):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    nickname = f"{first_name} {last_name}" if last_name else f"{first_name}"

    msg_data = {"text": f"<b>Вы входите на сайт </b><a href=\"https://pokerhub.pro\">PokerHUB.pro</a> <b>под учетной записью \"{nickname}\".</b>\n\nЧтобы продолжить авторизацию на сайте, нажмите на кнопку <b>\"Авторизоваться\"</b>.\n\nЕсли вы не совершали никаких действий на сайте, или попали сюда в результате действий третьих лиц, нажмите на кнопку <b>\"Отмена\"</b>.",
                "buttons": [
                    [{"title": "Авторизоваться", "callback": f"auth_ph={auth_code}"}],
                    [{"title": "Отмена", "callback": f"abort_ph={auth_code}"}]
                    ]
                }

    await send_message(bot=bot,
        user_id=user_id,
        msg_data=msg_data
        )

# подтверждение авторизации
@router.callback_query(lambda call: call.data.startswith('auth_ph='))
async def auth_pokerhub(call, bot):
    await call.answer()
    auth_code = call.data.replace('auth_ph=', '')
    user_id = call.from_user.id
    await touch_user_activity(user_id)
    username = call.from_user.username or None
    first_name = call.from_user.first_name
    last_name = call.from_user.last_name
    avatar = f"https://telegram.pokerhub.pro/api/static/img/avatars/avatar_{user_id}.jpg"
    url = "https://pokerhub.pro/api/tg/authbybot"
    payload = {
        "auth": True,
        "telegram_id": user_id,
        "username": username,
        "token": auth_code,
        "nickname": f"{first_name} {last_name}" if last_name else f'{first_name}',
        "avatar": avatar if os.path.exists(os.path.join(os.getenv('static_folder'), 'img', 'avatars', f'avatar_{user_id}.jpg')) else None
    }

    # print(payload)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                data = await response.json()
                status = data.get("status", None)
                if status and (answer_msg:=keycodes_answers.get(status, None)):
                    try:
                        await bot.delete_message(call.from_user.id, call.message.message_id)
                    except:
                        pass
                    await send_message(bot=bot,
                        user_id=user_id,
                        msg_data={"text": answer_msg}
                        )
                else:
                    await logger.error(f"Неучтённый статускод ответа ПХ: {response.status}. Ответ json={await response.json()}")
    except aiohttp.ClientError as e:
        await logger.error(f"Ошибка при отправке запроса к ПХ: {e}")
        await send_message(
            bot=bot,
            user_id=user_id,
            msg_data={"text": "Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте ещё раз."}
        )
    except Exception as error:
        await logger.error(f"Необработанная ошибка при подтверждении авторизации: {e}")

# отмена авторизации
@router.callback_query(lambda call: call.data.startswith('abort_ph='))
async def auth_pokerhub(call, bot):
    await call.answer()
    auth_code = call.data.replace('auth_ph=', '')
    user_id = call.from_user.id
    await touch_user_activity(user_id)

    url = "https://pokerhub.pro/api/tg/authbybot"
    payload = {
        "auth": False,
        "telegram_id": user_id,
        "token": auth_code
    }

    # print(payload)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                try:
                    await bot.delete_message(call.from_user.id, call.message.message_id)
                except:
                    pass
                await send_message(bot=bot,
                    user_id=user_id,
                    msg_data={"text": "Авторизация успешно отменена"}
                    )
                await logger.debug(f"Пользователь {user_id} отменил авторизацию на ПХ. Статускод ответа: {response.status}")
    except aiohttp.ClientError as e:
        await logger.error(f"Ошибка при отправке запроса к ПХ: {e}")
        await send_message(
            bot=bot,
            user_id=user_id,
            msg_data={"text": "Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте ещё раз."}
        )
    except Exception as error:
        await logger.error(f"Необработанная ошибка при подтверждении авторизации: {e}")
