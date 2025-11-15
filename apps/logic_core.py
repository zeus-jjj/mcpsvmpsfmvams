"""
Обработчик для личностей бота
"""
import asyncio
#
from aiogram import Router
from collections import OrderedDict
#
import apps.logger as logger
#
from apps.funcs import run_action, send_message, add_history, save_event, close_old_notifications, touch_user_activity
from apps.notifier import notificator
from modules import MAP, FSMStates
#
from modules import message_manager


router = Router()
# Ограничение на количество блокировок (не много, чтобы не забивать ОЗУ, и не мало,
# чтобы не было ошибки при большом кол-ве одновременно запущеных FSM)
LOCK_LIMIT = 100

# Создаем глобальный словарь для хранения блокировок пользователей
user_locks = OrderedDict()

# для получения/создания объекта блокировки
async def get_user_lock(user_id):
    # Удаляем старейшие блокировки, если превышен лимит
    while len(user_locks) >= LOCK_LIMIT:
        user_locks.popitem(last=False)
    # Создаем блокировку для нового пользователя
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]

# при нажатии /start
async def start(bot, message, persona, msg):
    await logger.debug(f"Пользователь @{message.from_user.username or 'Unknown'} запустил бота [{persona}]")
    # проверяем, какая персона должна запуститься
    if (persona:=persona or "default") not in MAP['start']:
        # если передана непонятная метка - скидываем на дефолт
        persona = "default"
    # текущий маршрут
    route = "start"
    user_id = message.from_user.id
    await touch_user_activity(user_id)
    current_map = MAP['start'] if not msg else MAP['callback']

    # если для этого сообщения есть данные в json
    if (msg_data:=current_map.get(msg or persona, None)):



        # проверяем, есть-ли действие для этого сообщения
        result = False
        if (act:=msg_data.get("action")) or (act:=msg_data.get("actions")):
            if type(act) == list:
                for action in act:
                    result = await run_action(action=action, user_id=user_id, bot=bot)
            elif type(act) == dict:
                result = await run_action(action=act, user_id=user_id, bot=bot)
            else:
                await logger.error(f"Ошибка в типе данных action(s)! Должен быть list/dict, а передан {type(act)}!")
        # Если результат выполнения action(s) положительный, то закроем уведомление и подменим данные сообщения
        if result:
            # если True, то подменяем данные для ответа и маршрут
            route = act.get('is_ok', None)
            msg_data = MAP['callback'].get(msg or route, None)



        # проверяем, есть-ли ивент, который нужно записать
        if (event:=msg_data.get("event", None)):
            await save_event(user_id=user_id, event=event)
    user_data = {"username": message.from_user.username or "unknown", "first_name": message.from_user.first_name or "друг", "last_name": message.from_user.last_name or ""}
    result = await send_message(bot=bot, user_id=user_id, msg_data=msg_data, persona=persona, route=msg or route, user_data=user_data)

    # добавляем отложенное уведомление (не добавится, если уже было добавлено
    # ранее, либо если для этой личности нет уведомлений)
    if result:
        await message_manager.delete_messages(bot=bot, user_id=user_id)
    if result and (notifications:=msg_data.get('notifications', None)):
        await notificator.add_notifications(user_id=user_id,
            notifications=notifications)
    # возвращаем результат отправки (True/False)
    return result

# если ни один из колбеков выше не подошёл
# одна функция для всех (почти) колбеков персон. Для упрощения редактирования поведения
# личностей бота (для этого достаточно отредактировать json-файл, а код менять не нужно!)
@router.callback_query()
async def handle_persona_callback(call, state, bot):
    callback_data = call.data
    user_id = call.from_user.id
    await touch_user_activity(user_id)
    await call.answer()





    # # ЭКСПЕРИМЕНТАЛЬНЫЙ БЛОК ДЛЯ ЗАКРЫТИЯ ОТЛОЖЕННЫХ УВЕДОМЛЕНИЙ ПРИ УНИКАЛЬНОМ ДЕЙСТВИИ
    # # пока этот блок отключаю (13.10.25), т.к. я буду создавать отложенные уведомления на определенную дату. Если эту часть не закомментить,
    # # то юзеру скорее всего не дойдёт важное отложенное уведомление. С другой стороны, эта часть кода полезна, так как при движениее по
    # # нелинейной воронке, юзер будет получать уведомления только с той ветки, по которой сейчас идёт
    # await close_old_notifications(user_id=user_id, callback=callback_data)




    # если для этого колбека есть данные в json
    if (msg_data:=MAP['callback'].get(callback_data, None)):
        # если это сообщение нужно удалить (на котором сработала кнопка)
        if msg_data.get('delete', False):
            try:
                await bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)
            except Exception as error:
                await logger.error(f"Не удалось удалить сообщение у юзера с id={user_id}. Ошибка: {error}")

        prev = callback_data
        result = False
        if (act:=msg_data.get("action")) or (act:=msg_data.get("actions")):
            if type(act) == list:
                for action in act:
                    result = await run_action(action=action, user_id=user_id, bot=bot)
            elif type(act) == dict:
                result = await run_action(action=act, user_id=user_id, bot=bot)
            else:
                await logger.error(f"Ошибка в типе данных action(s)! Должен быть list/dict, а передан {type(act)}!")
        # Если результат выполнения action(s) положительный, то закроем уведомление и подменим данные сообщения
        if result:
            # если True, то подменяем данные для ответа и маршрут
            prev = act.get('is_ok', None)
            msg_data = MAP['callback'].get(act['is_ok'], None)

        # проверяем, есть-ли ивент, который нужно записать
        if (event:=msg_data.get("event", None)):
            await save_event(user_id=user_id, event=event)

        user_data = {"username": call.from_user.username or "unknown", "first_name": call.from_user.first_name or "друг", "last_name": call.from_user.last_name or ""}
        result = await send_message(bot=bot, user_id=user_id, msg_data=msg_data, route=callback_data, user_data=user_data)

        # добавляем отложенное уведомление (не добавится, если уже было добавлено
        # ранее, либо если для этой личности нет уведомлений)
        if result:
            await message_manager.delete_messages(bot=bot, user_id=user_id)
        if result and (notifications:=msg_data.get('notifications', None)):
            await notificator.add_notifications(user_id=user_id,
                notifications=notifications)
        if prev:
            # закрываем предыдущее уведомление
            await notificator.close_notification(user_id=user_id,
                label=prev)

    # если не найдено поведение для этого колбека
    else:
        await logger.error(f"Для callback={callback_data} нет поведения в json-файле!")


# Обработчик для сообщений с запущеной машиной состояний
@router.message(FSMStates.fsm_context)
async def process_fsm(message, state, bot):
    user_id = message.from_user.id
    await touch_user_activity(user_id)

    async with await get_user_lock(user_id):
        data = await state.get_data()
        collected_items = 0 # кол-во собанных данных

        for idx, item in enumerate(data.get("collect", [])):
            # если найдены данные, для которых юзер ещё не отправил ответ
            if not item.get('value', None):
                expected_data = item.get("expected_data")
                answer = item.get("is_ok_msg")
                if expected_data == "text" and message.text:
                    item['value'] = message.text
                elif expected_data == "contact" and message.contact and message.contact.phone_number:
                    item['value'] = message.contact.phone_number
                else:
                    # отправляем ответное сообщение в случае неправильных данных от юзера
                    await message.answer(item.get("is_not_ok_msg"))
                    # выходим из функции
                    return
                # записываем историю
                await add_history(user_id=user_id, text=f"Оставил данные: {item['name']} - {item['value']}")
                # отправляем ответное сообщение если данные корректные
                await send_message(bot=bot, user_id=user_id, msg_data={"text": answer, "remove_keyboard": True})
                # обновляем счётчик данных
                collected_items = idx
                # выходим из цикла
                break

        # если доп. данные ещё не были записаны - записываем их
        if "addition_data" not in data:
            data["addition_data"] = {"username": message.from_user.username, "user_id": user_id, "profile_link": f"https://telegram.pokerhub.pro/profile/{user_id}"}
        # записываем в машину состояний обновлённые данные
        await state.update_data(data=data)

        # если FSM работает
        if await state.get_state():
            # выполняем действие каждый раз после добавления новых данных
            await run_action(action={"func": data.get("if_collected")}, user_id=user_id, bot=bot)
            # если собраны все данные
            if collected_items == len(data.get("collect")) - 1:
                # останавливаем FSM
                await state.clear()

"""

"""
