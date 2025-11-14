from aiogram import Router
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton

import aiohttp
from json import loads, JSONDecodeError
from modules import headers, get_host, check_ticket, create_connect, TicketState
import apps.logger as logger
import apps.funcs as funcs

router = Router()

# для кнопки создания тикета
@router.callback_query(lambda call: call.data == 'create-ticket')
async def ticket_handler(call, state, bot):

    user_id = call.from_user.id
    await funcs.touch_user_activity(user_id)

    current_state = await state.get_state()
    ticket = await check_ticket(user_id)

    if current_state == 'TicketState:ACTIVE' and ticket is not None:
        await bot.send_message(
            chat_id=user_id,
            text='У вас уже есть активный тикет.'
        )
        return

    await state.set_state(TicketState.CREATE)

    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text='❌', callback_data='not-create-ticket'))

    await bot.send_message(
        chat_id=user_id,
        text="Введите сообщение, чтобы операторы могли вам помочь",
        reply_markup=keyboard.as_markup()
    )


@router.callback_query(lambda call: call.data == 'not-create-ticket' and TicketState.CREATE)
async def remove_ticket_create(call, state):
    await funcs.touch_user_activity(call.from_user.id)
    await state.clear()
    await call.message.delete()


@router.message(TicketState.CREATE)
async def create_ticket_handler(message, state, bot):
    await funcs.touch_user_activity(message.from_user.id)
    await state.set_state(TicketState.ACTIVE)
    body = {
        'user_id': message.from_user.id,
        'username': message.from_user.username,
        'question': message.text
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(get_host()+'/tickets/add', json=body) as response:
            data = loads(await response.text())

    await state.update_data(id=data.get("id", "0"))

    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text=f"""Закрыть обращение #{data.get("id", "0")}""",
                                      callback_data=f'close_ticket-{data.get("id")}'))

    await bot.send_message(
        chat_id=message.from_user.id,
        text="<code>Ожидайте, скоро вам ответят!</code>",
        reply_markup=keyboard.as_markup()
    )

# для создания тикета пользователем
@router.message(lambda message: TicketState.ACTIVE(message))
async def create_ticket_message_handler(message, state, bot):
    await funcs.touch_user_activity(message.from_user.id)
    ticket_id = await state.get_data()
    body = {
        'id': ticket_id['id'],
        'text': message.text,
        'is_messanger':True,
        'user': {
            'id': message.from_user.id,
            'first_name': message.from_user.first_name or '',
            'last_name': message.from_user.last_name or '',
            'photo_code': get_host()+f'/static/img/avatars/avatar_{message.from_user.id}.jpg',
        }
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(get_host()+'/tickets/add-message', json=body) as response:
            text = await response.text()
            try:
                r = loads(text)
            except JSONDecodeError:
                print('ERROR SEND',text)
                await bot.send_message(message.from_user.id, 'Сообщение не доставлено из-за ошибки на сервере #034')
            if r.get('resultCode') == 2:
                await state.clear()


@router.callback_query(lambda c: c.data and c.data.startswith('close_ticket-') and TicketState.ACTIVE)
async def close_ticket(call, state, bot):

    await funcs.touch_user_activity(call.from_user.id)
    await call.answer(text='Закрываю...')
    ticket_id = call.data.replace('close_ticket-','')

    if not ticket_id.isdigit():
        return

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(get_host()+f'/tickets/close-ticket/{ticket_id}') as response:
            data = loads(await response.text())

    if data['status'] != 'ok':
        return
    else:
        db = await create_connect()
        await db.execute(
            """INSERT INTO user_history (user_id, text) VALUES ($1, $2)""", call.from_user.id,
            f"Пользователь закрыл тикет #{ticket_id}"
        )
        await db.close()

    try:
        await bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)
    except Exception as error:
        await logger.error(f"[tickets] Не удалось удалить сообщение. Ошибка: {error}")

    await bot.send_message(
        chat_id=call.from_user.id,
        text="Вы закрыли обращение"
    )

    await state.clear()

