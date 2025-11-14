from os import getenv
from os.path import join, exists

import asyncio
from aiogram import types, Router
from aiogram.filters.command import Command
from aiogram.filters import ChatMemberUpdatedFilter, KICKED, MEMBER
from modules import get_data, create_connect, bot, dp, MAP
#
from json import loads, JSONDecodeError

# модуль для рассылки уведомлений с покерхаб
import apps.ph_notifier as notifier
# RAG-система
# from apps import ragflow

# импортируем ядро логики бота
import apps.logic_core as logic_core 
import apps.auth_pokerhub as auth_pokerhub
import apps.funcs as funcs
import apps.logger as logger
from apps.bot_info import bot_info
from apps.iq_quiz import quiz_results
from apps.vk_iq_quiz import vk_quiz_results
from apps.select_quiz import quiz_results as select_quiz_results

# уведомления от разных личностей
from apps.notifier import notificator



router = Router()


# вызывается на событие блокировки бота юзером
@dp.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def process_user_blocked_bot(event: types.ChatMemberUpdated):
    # print(f'Пользователь {event.from_user.id} заблокировал бота')
    db = await create_connect()
    await db.execute(
        """INSERT INTO funnel_history (user_id, label)
            VALUES ($1, $2)""",
        event.from_user.id, "Заблокировал бота"
        )
    await db.close()
    await notificator.blocked(user_id=event.from_user.id, is_blocked=True)
    await funcs.touch_user_activity(event.from_user.id)

# вызывается, когда юзер разблокировал бота
@dp.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def user_unblocked_bot(event: types.ChatMemberUpdated):
    # print(f'Пользователь {event.from_user.id} разблокировал бота')
    db = await create_connect()
    await db.execute(
        """INSERT INTO funnel_history (user_id, label)
            VALUES ($1, $2)""",
        event.from_user.id, "Разблокировал бота"
        )
    await db.close()
    await notificator.blocked(user_id=event.from_user.id, is_blocked=False)
    await funcs.touch_user_activity(event.from_user.id)



# для теста авторизации на ПХ
# https://t.me/firestorm_test_bot?start=auth=generated_code

@router.message(Command('start'))
async def process_start_command(message: types.Message, bot):
    # print(message.text.replace('/start', ''))
    raw_data = message.text.replace('/start', '').lstrip()
    try:
        data = get_data(raw_data)  
    except Exception as error:
        await logger.error(f"Не удалось извлечь метки из ссылки: {raw_data} у пользователя с id={message.from_user.id}!")
        data = {}
    



    # ВРЕМЕННЫЙ КОСТЫЛЬ ДЛЯ СОВМЕСТИМОСТИ СО СТАРЫМИ МЕТКАМИ, УДАЛИТ 13.10.2025 ЭТОТ БЛОК И ОТПИСАТЬ В КОНФУ
    platform = data.get("platform")
    company = data.get("company")
    content = data.get("content")
    # platform=source, company=campaign, content=content
    if platform:
        # source
        data['s'] = platform
    if company:
        # campaign
        data['ca'] = company
    if content:
        # content
        data['co'] = content




    # Если есть, значит бота запустили с целью авторизации на ПХ
    auth_code = data.get('auth', None)
    # quiz_string (если есть, значит это переход из айкью-теста, нужно расшифровать)
    quiz_string = data.get('iq', None)
    # если есть - значит это запрос результатов квиза по ВК воронке
    vk_quiz_string = data.get('vkiq')
    # квиз для определения направления
    select_quiz_string = data.get('sel_quiz')
    # чтобы перейти к сообщению с таким идентификатором
    msg = data.get("msg", None)
    await logger.info(f"Бот запущен пользователем {message.from_user.username}, id={message.from_user.id}, data={data}")

    db = await create_connect()
    # проверяем, есть-ли такой юзер в БД
    user_id = await db.fetchrow(
        """SELECT id FROM users WHERE id = $1""", message.from_user.id
    )
    # По дефолту ставим unknown, далее если у него есть аватарка - это значение поменяется
    avatar = f'unknown_user.jpg'
    # Проверяем, есть-ли на диске аватарка юзера
    if not exists(join(getenv('static_folder'), 'img',
                    'avatars', f'avatar_{message.from_user.id}.jpg')
                    ):
        profile_pictures = await bot.get_user_profile_photos(message.from_user.id, limit=1)
        avatars = profile_pictures.photos
        # Если у юзера есть фото профиля
        if len(avatars) > 0:
            file = await bot.get_file(avatars[0][-1].file_id)
            # Скачиваем его
            await bot.download_file(file.file_path,
                join(getenv('static_folder'), 'img',
                    'avatars', f'avatar_{message.from_user.id}.jpg'))

            avatar = f'avatar_{message.from_user.id}.jpg'

        await db.execute(
            """INSERT INTO users (id, username, last_name, first_name, photo_code)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id)
            DO UPDATE SET
                username = EXCLUDED.username,
                last_name = EXCLUDED.last_name,
                first_name = EXCLUDED.first_name,
                photo_code = EXCLUDED.photo_code""",
            message.from_user.id, message.from_user.username or "Unknown",
            message.from_user.last_name, message.from_user.first_name, avatar
        )


    # если он есть, значит запускал бота
    if user_id:
        # print(f"Этот юзер уже запускал бота: {user_id.get('id', None)}")

        # # временной решение для отладки (ЗАКОММЕНТИТЬ ИЛИ УДАЛИТЬ ПРИ ФИНАЛЬНОМ ДЕПЛОЕ)
        # await db.execute(
        #     """UPDATE lead_resources 
        #     SET campaign = $2, source = $3, medium = $4, term = $5, content = $6, direction_id = (SELECT id FROM directions WHERE code=$7 LIMIT 1) 
        #     WHERE user_id = $1""",
        #     message.from_user.id, data.get('campaign'), data.get('source'), data.get('medium'), data.get('term'), data.get('content'), data.get('land').upper() if data.get('land') else None
        # )

        # получаем из БД utm-метки юзера, которые были при первом его запуске бота
        user_utm = await db.fetchrow(
            """SELECT campaign, source, medium, term, content, direction_id FROM lead_resources WHERE user_id = $1""", message.from_user.id
        )
        # Преобразуем объект Record в словарь
        user_utm = dict(user_utm.items()) if user_utm else {}

        # если метки из БД не Null
        if (user_utm.get("campaign", None)) or (user_utm.get("source", None)):
            # присваиваем метки из БД в data, если это не авторизация на ПХ
            if not auth_code:
                data = user_utm
        # print(f"UTM-метки из БД: {user_utm}")
    else:
        # print("Это новый юзер")
        # Сохраняем в БД запись в users

        if auth_code:
            # platform заменил на source
            data["s"] = "auth_pokerhub"      

        await db.execute(
            """INSERT INTO users (id, username, last_name, first_name, photo_code) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING""",
            message.from_user.id, message.from_user.username or "Unknown",
            message.from_user.last_name, message.from_user.first_name, avatar
        )
        
        await db.execute(
            """INSERT INTO user_history (user_id, text) VALUES ($1, $2)""", message.from_user.id,
            f"Начал пользоваться ботом." + "" if data == {} else f"\nСсылка перехода в бота: {raw_data}"
        )

        auth_user_data = await auth_pokerhub.get_user_data_auth(auth_code=auth_code)
        referer_url = auth_user_data.get('referer', None)
        
        # записываем в БД utm-метки (если они уже там есть - запрос будет проигнорирован)
        # Было: platform, company, content
        # Стало: campaign (ca), source (s), medium (m), term (t), content (co)
        await db.execute(
            """INSERT INTO lead_resources (user_id, campaign, source, medium, term, content, direction_id, referer_url, raw_link) 
            VALUES ($1, $2, $3, $4, $6, $7, (SELECT id FROM directions WHERE code=$5 LIMIT 1), $8, $9) ON CONFLICT (user_id) DO NOTHING""",
            message.from_user.id, data.get('ca'), data.get('s'), data.get('m'), data.get('t'), data.get('co'), data.get('land').upper() if data.get('land') else None, referer_url, raw_data
        )

    await db.close()
    await funcs.touch_user_activity(message.from_user.id)

    # если был передан параметр для авторизации на покерхаб
    if auth_code:
        await auth_pokerhub.start_auth(bot=bot, message=message, auth_code=auth_code)
    elif quiz_string:
        await quiz_results(bot=bot, message=message, quiz_string=quiz_string)
    elif vk_quiz_string:
        await vk_quiz_results(bot=bot, message=message, quiz_string=vk_quiz_string)
    elif select_quiz_string:
        await select_quiz_results(bot=bot, message=message, quiz_string=select_quiz_string)

    # Если просто старт - сбрасываем сессию РАГ и пишем приветственное сообщение
    # else:
    #     assistant = ragflow.client.list_assistants(name=ragflow.assistant_name)
    #     if assistant:
    #         sessions = ragflow.client.list_sessions(assistant=assistant[0], name=f"{ragflow.session_base_name}_{message.from_user.id}")
    #         if sessions:
    #             ragflow.client.clear_chat_history(assistant[0], ids=[session.id for session in sessions])
                
    # Это раньше тут подключалась воронка по файлу map.json с персонами
    else:
        # пробуем подключить нужную персону, если переданы нужные метки
        await logic_core.start(bot=bot, message=message, persona=data.get("ca", None), msg=msg)




# обработчик всех сообщений (для создания или дополнения тикета)
@router.message()
async def message_handler(message, state, bot):
    message_text = message.text
    user_full_name = message.from_user.full_name
    username = message.from_user.username or None
    user_id = message.from_user.id
    
    # добавляем в историю event отправку юзером сообщения
    await funcs.save_event(user_id=user_id, event="send_msg", rewrite=True)
    await funcs.touch_user_activity(user_id)

    # Айди юзеров, которые могут общаться с ИИ
    # Пока-что хардкод, со списком айдишек юзеров. Потом автоматика должна определять, куда слать сообщения
    ai_access = [] # [542149705, 5762455571]
    if user_id in ai_access:
        dialogue_type = "ai"
    else:
        dialogue_type = "support"


    # Если сообщение должно идти в ИИ-ассистента
    if dialogue_type == "ai":
        sent_message = await bot.send_message(user_id, "🤔")
        message_id = sent_message.message_id
        await logger.debug(f"Пользователь {user_full_name} отправил сообщение с текстом: {message_text}")
        rag_answer = await ragflow.send_msg_to_rag(bot=bot, user_id=user_id,message_text=message_text)
        if rag_answer:
            answer_text, keyboard = rag_answer
            await logger.debug(f"Ответ ИИ: [{answer_text}], кнопки: [{keyboard}]")
            try:
                await bot.delete_message(chat_id=user_id, message_id=message_id)
            except Exception as e:
                await logger.error(f"Не удалось удалить сообщение: {e}")

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=answer_text.replace("**", "*"),
                    reply_markup=keyboard,
                    parse_mode="Markdown"
                )
            except Exception as e:
                # Удаляем сомволы форматирования текста, т.к. без parse_mode будем отправлять в этом случае
                answer_text = answer_text.replace("*", "")
                await bot.send_message(
                    chat_id=user_id,
                    reply_markup=keyboard,
                    text=answer_text
                )
            await logger.debug(f"Сообщение успешно доставлено пользователю {user_full_name}")
            # Сохраняем сообщение в историю сообщений
            saved = await funcs.add_msg_to_history(content=message_text, chat_id=user_id, author_id=user_id)
            if not saved:
                await logger.error(f'Сообщение от user_id={message.from_user.id} не записан в БД в историю сообщений!')
            # делаем небольшую задержку
            await asyncio.sleep(0.5)
            # Сохраняем сообщение в историю сообщений
            saved = await funcs.add_msg_to_history(content=f"Ответ от ИИ: {answer_text}", chat_id=user_id, author_id="-1")
            if not saved:
                await logger.error('Ответ от ИИ-ассистента не записан в БД в историю сообщений!')
        # Если ответа от RAG не вернулся - отправляем сообщение об этом
        else:
            await bot.send_message(
                    chat_id=user_id,
                    text="*Не могу ответить...*\nПопробуйте задать вопрос ещё раз",
                    parse_mode="Markdown"
                )
            await logger.error(f"Не удалось отправить ответ пользователю {user_full_name}")

    # Если сообщение должно идти в живую поддержку
    elif dialogue_type == "support":

        # Пытаемся извлечь сообщения из таблицы jivo_integration_queue, и сначала отправить их
        queue_msgs = await funcs.get_msgs_to_jivo_integration_queue(user_id=user_id)
        # Получаем дату последнего сообщения в jivo_integration_queue
        last_create_at = queue_msgs[-1]['create_at'] if queue_msgs else None
        # Добавляем \n\n\n в конец, чтобы визуально проще различать сообщения
        for item in queue_msgs:
            item['text'] += '\n\n\n'
        # Если есть текстовое сообщение от юзера
        if message_text:
            # Добавляем ласт сообщение (которое от юзера)
            queue_msgs += [{'text': message_text, 'create_at': None}]

        # Проверяем наличие вложений
        attachments = []

        if message.photo:
            # for photo in message.photo:
            photo = message.photo[-1]
            file = await bot.get_file(photo.file_id)
            file_name = "photo.jpg"
            attachments.append({"file_type": "photo", "file_path": file.file_path, "file_name": file_name})
        if message.video:
            file = await bot.get_file(message.video.file_id)
            file_name = message.video.file_name or "video.mp4"
            attachments.append({"file_type": "video", "file_path": file.file_path, "file_name": file_name})
        if message.document:
            file = await bot.get_file(message.document.file_id)
            file_name = message.document.file_name or "document"
            attachments.append({"file_type": "document", "file_path": file.file_path, "file_name": file_name})
        if message.voice:
            file = await bot.get_file(message.voice.file_id)
            file_name = "voice.ogg"
            attachments.append({"file_type": "audio", "file_path": file.file_path, "file_name": file_name})


        # Отправляем сначала файлы, если они есть
        for attachment in attachments:
            # Отправляем в JIVO
            result = await funcs.send_to_jivo(
                user_id=user_id, 
                # thumb=attachment.get("url") if attachment.get("type") == "photo" else None,
                # file=attachment.get("url") if attachment.get("type") == "document" or attachment.get("type") == "photo" else None,
                # video=attachment.get("url") if attachment.get("type") == "video" else None,
                file_path=attachment.get("file_path", None),
                file_type=attachment.get("file_type", None),
                file_name=attachment.get("file_name", None),
                name=f"{user_full_name}",
                intent=f"Обращение из телеграм @{bot_info.get_username()}" + (f" https://t.me/{username}" if username else ""),
                invite=f"Для просмотра истории переписки с пользователем, можете посетить: https://telegram.pokerhub.pro/profile/{user_id}",
                photo=f"https://telegram.pokerhub.pro/api/static/img/avatars/avatar_{user_id}.jpg"
            )
            if not result:
                await logger.error(f"Сообщение от юзера {user_full_name} ({user_id}) не доставлено в JIVO!")
            else:
                # Сохраняем сообщение в историю сообщений
                file_name = result.get('file_name', None)
                original_file_name = result.get('original_file_name', None)
                if file_name and original_file_name:
                    saved = await funcs.add_msg_to_history(content=original_file_name, 
                                                           name=file_name, 
                                                           type=attachment.get('file_type'), 
                                                           chat_id=user_id, 
                                                           author_id=user_id)
                    if not saved:
                        await logger.error(f'Сообщение от user_id={message.from_user.id} не записан в БД в историю сообщений!')
                else:
                    await logger.error(f"Не удалось получить имя файла, сохраненного на сервере! result={result}")
        for msg in queue_msgs:
            # Отправляем в JIVO
            result = await funcs.send_to_jivo(
                text=msg.get('text'), 
                user_id=user_id, 
                name=f"{user_full_name}",
                intent=f"Обращение из телеграм @{bot_info.get_username()}" + (f" https://t.me/{username}" if username else ""),
                invite=f"Для просмотра истории переписки с пользователем, можете посетить: https://telegram.pokerhub.pro/profile/{user_id}",
                photo=f"https://telegram.pokerhub.pro/api/static/img/avatars/avatar_{user_id}.jpg"
            )
            if not result:
                await logger.error(f"Сообщение от юзера {user_full_name} ({user_id}) не доставлено в JIVO!")
            else:
                # Сохраняем сообщение в историю сообщений
                saved = await funcs.add_msg_to_history(content=message_text, chat_id=user_id, author_id=user_id)
                if not saved:
                    await logger.error(f'Сообщение от user_id={message.from_user.id} не записан в БД в историю сообщений!')
        
        # Деактивируем сообщения в jivo_integration_queue если отправили их
        if last_create_at:
            await funcs.deactivate_msgs_for_user(user_id=user_id, end_date=last_create_at)




async def start_notifier():
    # запускаем цикл уведомлений с покерхаба
    asyncio.create_task(notifier.main())
    # запускаем цикл отложенных уведомлений бота
    asyncio.create_task(notificator.main())

async def main():
    dp.include_routers(
        notifier.router, # уведомления с покерхаб
        auth_pokerhub.router, # авторизация в ПХ
        logic_core.router, # "личности" бота
        router
    )

    # запускаем модуль уведомлений после запуска бота
    dp.startup.register(start_notifier)
    
    # Получаем юзернейм бота
    bot_information = await bot.get_me()
    bot_info.set_id(id=bot_information.id)
    bot_info.set_username(username=bot_information.username)
    await logger.info(f"Bot started, ID: {bot_info.get_id()}, name: @{bot_info.get_username()}")
    await funcs.update_funnel_db()
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())

