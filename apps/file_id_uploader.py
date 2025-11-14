import asyncio
import os
import hashlib
from aiogram import types
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo, InputMediaDocument, ReplyKeyboardRemove
from modules import bot, dp
import apps.logger as logger
from apps.bot_info import bot_info
from modules import create_connect
# 
from modules import message_manager

async def send_file_by_label(bot, chat_id: int, label: str, filepath: str = None, content_type: str = 'document', text=None, filename=None, files_group=None, thumbnail_path=None, reply_markup=None):
    """Отправляем файл или медиагруппу по label, используя file_id из БД, если возможно"""
    
    # Если передана медиагруппа (список файлов)
    if files_group:
        media_group = []
        files_to_upload = []
        file_ids_to_save = []

        # Логируем порядок файлов для отладки
        await logger.info(f"Processing files_group for user {chat_id}, label={label}: {[(f['file_path'], f['tg_filename']) for f in files_group]}")

        for index, file in enumerate(files_group):
            file_path = file.get("file_path", None)
            filename = file.get("tg_filename", None)
            thumbnail_path = file.get("thumbnail", None)
            file_content_type = file.get("content_type", 'document')
            file_label = f"{label}_{file_path}"  # Уникальный label для каждого файла

            # Проверяем, существует ли файл
            if not file_path or not os.path.exists(file_path):
                await logger.error(f"Ошибка при отправке файла юзеру {chat_id} с label={file_label}\nОшибка: файл по пути {file_path} не существует!")
                return False

            # Проверяем превью
            thumbnail = None
            if thumbnail_path and os.path.exists(thumbnail_path):
                thumbnail = FSInputFile(thumbnail_path, filename=filename)
            elif thumbnail_path:
                await logger.error(f"Ошибка при отправке файла юзеру {chat_id} с label={file_label}\nОшибка: thumbnail-файл по пути {thumbnail_path} не существует! Отправка без thumbnail.")

            # Проверяем file_id в БД
            db_file = await get_file_id(label=file_label)
            real_hash = calculate_file_hash(filepath=file_path)

            if db_file:
                file_id = db_file.get('file_id', None)
                db_content_type = db_file.get('content_type', 'document')
                file_hash = db_file.get('hash', None)

                # Проверяем хеш и тип контента
                if file_hash and file_id and real_hash == file_hash and file_content_type == db_content_type:
                    media = create_media_object(file_id, file_content_type, text if index == 0 else None, thumbnail)
                    media_group.append(media)
                    file_ids_to_save.append((file_label, file_id, file_content_type, file_hash))
                    await logger.info(f"Added file_id {file_id} for {file_label} to media_group at index {index}")
                    continue

            # Если file_id не найден или файл изменился, добавляем для загрузки
            files_to_upload.append((file_path, filename, file_content_type, thumbnail, file_label, real_hash))
            await logger.info(f"Added file {file_path} to files_to_upload at index {index}")

        # Если все файлы имеют валидные file_id
        if not files_to_upload and media_group:
            try:
                await logger.info(f"Sending media_group with {len(media_group)} items: {[m.media for m in media_group]}")
                messages = await bot.send_media_group(chat_id=chat_id, media=media_group)
                for message in messages:
                    await message_manager.add_message(user_id=chat_id, message_id=message.message_id)
                # Отправляем клавиатуру отдельным сообщением, если она есть
                if reply_markup and text:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode='HTML',
                        reply_markup=reply_markup
                    )
                return True
            except Exception as error:
                await logger.error(f"Ошибка при отправке медиагруппы юзеру {chat_id} по file_id. label={label}\nОшибка: {error}")
                return False

        # Загружаем файлы, если есть что загружать
        if files_to_upload:
            for file_path, filename, file_content_type, thumbnail, file_label, real_hash in files_to_upload:
                file = FSInputFile(file_path, filename=filename)
                media = create_media_object(file, file_content_type, text if len(media_group) == 0 else None, thumbnail)
                media_group.append(media)
                await logger.info(f"Added file {file_path} to media_group at index {len(media_group)-1}")

            try:
                await logger.info(f"Sending media_group with {len(media_group)} items: {[m.media for m in media_group]}")
                messages = await bot.send_media_group(chat_id=chat_id, media=media_group)
                # Сохраняем file_id для загруженных файлов
                for (file_path, filename, file_content_type, thumbnail, file_label, real_hash), message in zip(files_to_upload, messages):
                    file_id = get_file_id_from_message(message, file_content_type)
                    await save_file_id(label=file_label, file_id=file_id, content_type=file_content_type, file_hash=real_hash)
                    await message_manager.add_message(user_id=chat_id, message_id=message.message_id)
                    await logger.info(f"Saved file_id {file_id} for {file_label}")
                # Отправляем клавиатуру отдельным сообщением, если она есть
                if reply_markup and text:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode='HTML',
                        reply_markup=reply_markup
                    )
                return True
            except Exception as error:
                await logger.error(f"Ошибка при отправке медиагруппы юзеру {chat_id} через file upload. label={label}\nОшибка: {error}")
                return False

    # Если передан одиночный файл
    elif filepath:
        # Проверяем, существует ли файл
        if not os.path.exists(filepath):
            await logger.error(f"Ошибка при отправке файла юзеру {chat_id} с label={label}\nОшибка: файл по пути {filepath} не существует!")
            return False

        # Проверяем превью
        if thumbnail_path and os.path.exists(thumbnail_path):
            thumbnail = FSInputFile(thumbnail_path, filename=filename)
        else:
            if thumbnail_path:
                await logger.error(f"Ошибка при отправке файла юзеру {chat_id} с label={label}\nОшибка: thumbnail-файл по пути {thumbnail_path} не существует! Отправка без thumbnail.")
            thumbnail = None

        # Проверяем file_id в БД
        file = await get_file_id(label=f"{label}_{filepath}")
        real_hash = calculate_file_hash(filepath=filepath)

        if file:
            file_id = file.get('file_id', None)
            db_content_type = file.get('content_type', 'document')
            file_hash = file.get('hash', None)

            # Проверяем хеш и тип контента
            if file_hash and file_id and real_hash == file_hash and content_type == db_content_type:
                result, answer = await send_and_save_file(
                    bot=bot,
                    chat_id=chat_id,
                    file=file_id,
                    text=text,
                    thumbnail=thumbnail,
                    content_type=content_type,
                    reply_markup=reply_markup
                )
                if bool(result):
                    await message_manager.add_message(user_id=chat_id, message_id=result)
                    await logger.info(f"Sent file with file_id {file_id} for {label}")
                    return True
                else:
                    await logger.error(f"Ошибка при отправке файла юзеру {chat_id} по file_id. label={label}\nОшибка: {answer}")

        # Если file_id не найден или файл изменился
        file = FSInputFile(filepath, filename=filename)
        result, answer = await send_and_save_file(
            bot=bot,
            chat_id=chat_id,
            file=file,
            text=text,
            thumbnail=thumbnail,
            content_type=content_type,
            reply_markup=reply_markup
        )

        if bool(result):
            await save_file_id(label=f"{label}_{filepath}", file_id=answer, content_type=content_type, file_hash=real_hash)
            await message_manager.add_message(user_id=chat_id, message_id=result)
            await logger.info(f"Saved and sent file_id {answer} for {label}")
            return True
        await logger.error(f"Ошибка при отправке файла юзеру {chat_id} через file upload. label={label}\nОшибка: {answer}")
        return False

    return False

def create_media_object(file, content_type: str, caption=None, thumbnail=None):
    """Создаёт объект медиа для медиагруппы"""
    match content_type:
        case "video":
            return InputMediaVideo(media=file, caption=caption, parse_mode='HTML', thumbnail=thumbnail)
        case "image":
            return InputMediaPhoto(media=file, caption=caption, parse_mode='HTML')
        case _:
            return InputMediaDocument(media=file, caption=caption, parse_mode='HTML', thumbnail=thumbnail)

def get_file_id_from_message(message, content_type: str):
    """Извлекает file_id из сообщения"""
    match content_type:
        case "video":
            return message.video.file_id
        case "image":
            return message.photo[-1].file_id
        case _:
            return message.document.file_id

async def get_file_id(label: str):
    """Ищем в базе данных file_id по label"""
    query = """SELECT file_id, content_type, hash FROM uploaded_files WHERE label = $1;"""
    db = await create_connect()
    row = await db.fetchrow(query, label)
    await db.close()
    return row if row else None

async def save_file_id(label: str, file_id: str, content_type: str, file_hash: str):
    """Сохраняем file_id в базу данных"""
    query = """INSERT INTO uploaded_files (label, file_id, content_type, hash) VALUES ($1, $2, $3, $4) ON CONFLICT (label) DO UPDATE SET file_id = $2, content_type = $3, hash = $4;"""
    db = await create_connect()
    await db.execute(query, label, file_id, content_type, file_hash)
    await db.close()

async def send_and_save_file(bot, chat_id: int, file: str, content_type: str, text, thumbnail, reply_markup):
    """Отправляем одиночный файл"""
    try:
        match content_type:
            case "video":
                message = await bot.send_video(
                    chat_id=chat_id,
                    video=file,
                    caption=text,
                    thumbnail=thumbnail,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                return message.message_id, message.video.file_id
            case "image":
                message = await bot.send_photo(
                    chat_id=chat_id,
                    photo=file,
                    caption=text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                return message.message_id, message.photo[-1].file_id
            case _:
                message = await bot.send_document(
                    chat_id=chat_id,
                    document=file,
                    caption=text,
                    thumbnail=thumbnail,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                return message.message_id, message.document.file_id
    except Exception as error:
        return False, str(error)

def calculate_file_hash(filepath: str) -> str:
    """Вычисляет MD5 хеш файла"""
    with open(filepath, 'rb') as file:
        md5_hash = hashlib.md5(file.read())
    return md5_hash.hexdigest()