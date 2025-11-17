import asyncio
#
from aiogram import Router
from collections import OrderedDict
#
import apps.logger as logger
#
from apps.funcs import (
    run_action,
    send_message,
    add_history,
    save_event,
    close_old_notifications,
    touch_user_activity,
)
from apps.notifier import notificator
from modules import (
    DEFAULT_FUNNEL,
    FSMStates,
    get_funnel,
    get_user_funnel,
    set_user_funnel,
)
from modules import message_manager


def _extract_next_route(action_data):
    """Возвращает целевой маршрут из action(s), если он указан."""
    if isinstance(action_data, dict):
        return action_data.get("is_ok")
    if isinstance(action_data, list):
        for action in reversed(action_data):
            if isinstance(action, dict) and action.get("is_ok"):
                return action["is_ok"]
    return None


router = Router()

# Ограничение на количество блокировок
LOCK_LIMIT = 100
user_locks = OrderedDict()


async def get_user_lock(user_id):
    """Получить/создать asyncio.Lock для пользователя."""
    while len(user_locks) >= LOCK_LIMIT:
        user_locks.popitem(last=False)
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]


# -----------------------------
#         /start
# -----------------------------
async def start(bot, message, persona, msg, funnel_name: str = DEFAULT_FUNNEL):
    await logger.debug(
        f"Пользователь @{message.from_user.username or 'Unknown'} запустил бота [{persona}]"
    )

    # сохраняем активную воронку за пользователем
    funnel_name = set_user_funnel(message.from_user.id, funnel_name)

    funnel_map = get_funnel(funnel_name)

    # определяем персонажа (ветку старта)
    if (persona := persona or "default") not in funnel_map["start"]:
        persona = "default"

    route = "start"
    user_id = message.from_user.id
    await touch_user_activity(user_id)

    current_map = funnel_map["start"] if not msg else funnel_map["callback"]

    # ищем сообщение
    msg_data = current_map.get(msg or persona)

    if msg_data:
        # выполняем actions / action
        result = False
        act = msg_data.get("action") or msg_data.get("actions")

        if act:
            if isinstance(act, list):
                for action in act:
                    result = await run_action(
                        action=action, user_id=user_id, bot=bot
                    )
            elif isinstance(act, dict):
                result = await run_action(
                    action=act, user_id=user_id, bot=bot
                )
            else:
                await logger.error(
                    f"action(s) must be list/dict, got {type(act)}"
                )

        # если действие выполнено положительно
        if result:
            next_route = _extract_next_route(act)
            if next_route:
                route = next_route
                msg_data = funnel_map["callback"].get(route)

        # записываем event
        if event := msg_data.get("event"):
            await save_event(user_id=user_id, event=event)

    # отправляем сообщение
    user_data = {
        "username": message.from_user.username or "unknown",
        "first_name": message.from_user.first_name or "друг",
        "last_name": message.from_user.last_name or "",
    }

    result = await send_message(
        bot=bot,
        user_id=user_id,
        msg_data=msg_data,
        persona=persona,
        route=msg or route,
        user_data=user_data,
        funnel_name=funnel_name,
    )

    # удаляем старые сообщения
    if result:
        await message_manager.delete_messages(bot=bot, user_id=user_id)

    # добавляем отложенные уведомления
    if result and (notifications := msg_data.get("notifications")):
        await notificator.add_notifications(
            user_id=user_id,
            notifications=notifications,
            funnel_name=funnel_name,
        )

    return result


# -----------------------------
#        CALLBACK
# -----------------------------
@router.callback_query()
async def handle_persona_callback(call, state, bot):
    callback_data = call.data
    user_id = call.from_user.id

    await touch_user_activity(user_id)
    await call.answer()

    # определяем воронку
    message_obj = getattr(call, "message", None)

    funnel_name = None
    if message_obj and hasattr(message_obj, "conf"):
        funnel_name = message_obj.conf.get("funnel")

    if not funnel_name:
        funnel_name = get_user_funnel(user_id)

    if hasattr(call, "conf"):
        call.conf["funnel"] = funnel_name

    funnel_map = get_funnel(funnel_name)

    msg_data = funnel_map["callback"].get(callback_data)

    if not msg_data:
        await logger.error(f"Для callback={callback_data} нет поведения!")
        return

    # удаляем сообщение, на котором нажали кнопку
    if msg_data.get("delete", False):
        try:
            await bot.delete_message(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
            )
        except Exception as error:
            await logger.error(
                f"Не удалось удалить сообщение у юзера={user_id}: {error}"
            )

    prev = callback_data
    result = False

    act = msg_data.get("action") or msg_data.get("actions")

    if act:
        if isinstance(act, list):
            for action in act:
                result = await run_action(
                    action=action, user_id=user_id, bot=bot
                )
        elif isinstance(act, dict):
            result = await run_action(
                action=act, user_id=user_id, bot=bot
            )
        else:
            await logger.error(
                f"action(s) must be list/dict, got {type(act)}"
            )

    # если действие успешно — подменяем маршрут
    if result:
        next_route = _extract_next_route(act)
        if next_route:
            prev = next_route
            msg_data = funnel_map["callback"].get(next_route)

    # записываем event
    if event := msg_data.get("event"):
        await save_event(user_id=user_id, event=event)

    # отправка сообщения
    user_data = {
        "username": call.from_user.username or "unknown",
        "first_name": call.from_user.first_name or "друг",
        "last_name": call.from_user.last_name or "",
    }

    result = await send_message(
        bot=bot,
        user_id=user_id,
        msg_data=msg_data,
        route=callback_data,
        user_data=user_data,
        funnel_name=funnel_name,
    )

    if result:
        await message_manager.delete_messages(bot=bot, user_id=user_id)

    if result and (notifications := msg_data.get("notifications")):
        await notificator.add_notifications(
            user_id=user_id,
            notifications=notifications,
            funnel_name=funnel_name,
        )

    if prev:
        await notificator.close_notification(
            user_id=user_id,
            label=prev,
            funnel_name=funnel_name,
        )


# -----------------------------
#          FSM
# -----------------------------
@router.message(FSMStates.fsm_context)
async def process_fsm(message, state, bot):
    user_id = message.from_user.id
    await touch_user_activity(user_id)

    async with await get_user_lock(user_id):
        data = await state.get_data()
        collected_items = 0

        for idx, item in enumerate(data.get("collect", [])):
            # если данных ещё нет
            if not item.get("value"):
                expected = item.get("expected_data")
                ok_msg = item.get("is_ok_msg")

                # ожидаем текст
                if expected == "text" and message.text:
                    item["value"] = message.text

                # ожидаем контакт
                elif (
                    expected == "contact"
                    and message.contact
                    and message.contact.phone_number
                ):
                    item["value"] = message.contact.phone_number

                else:
                    await message.answer(item.get("is_not_ok_msg"))
                    return

                await add_history(
                    user_id=user_id,
                    text=f"Оставил данные: {item['name']} - {item['value']}",
                )

                await send_message(
                    bot=bot,
                    user_id=user_id,
                    msg_data={"text": ok_msg, "remove_keyboard": True},
                )

                collected_items = idx
                break

        # добавляем доп. данные
        if "addition_data" not in data:
            data["addition_data"] = {
                "username": message.from_user.username,
                "user_id": user_id,
                "profile_link": f"https://telegram.pokerhub.pro/profile/{user_id}",
            }

        await state.update_data(data=data)

        # FSM работает?
        if await state.get_state():
            await run_action(
                action={"func": data.get("if_collected")},
                user_id=user_id,
                bot=bot,
            )

            # последний элемент
            if collected_items == len(data.get("collect", [])) - 1:
                await state.clear()
