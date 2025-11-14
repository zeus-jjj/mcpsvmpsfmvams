import json
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import binascii
import mysql.connector
# 
from modules import ENCRYPTION_KEY, ENCRYPTION_IV, MYSQL_CONFIG
import apps.logger as logger
from apps.funcs import send_message

async def quiz_results(bot, message, quiz_string):
    try:
        # Расшифровка hex-строки
        encrypted_bytes = binascii.unhexlify(quiz_string)  # Конвертируем hex в байты
        cipher = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, ENCRYPTION_IV)
        decrypted_bytes = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)
        decrypted_data = decrypted_bytes.decode('utf-8')

        # Парсинг JSON
        db_id = json.loads(decrypted_data)
        telegram_id = int(message.from_user.id)
        telegram_username = message.from_user.username or None

        # Подключение к MySQL
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Обновление записи в quiz_sessions по id
        query = """
        UPDATE quiz_select_sessions
        SET telegram_id = %s, telegram_username = %s
        WHERE id = %s
        """
        cursor.execute(query, (telegram_id, telegram_username, db_id))
        if cursor.rowcount == 0:
            await logger.debug("Запись не найдена или не обновлена")
        conn.commit()

        # Извлечение iq_score
        select_query = """
        SELECT best_game, spin_score, cash_score, mtt_score 
        FROM quiz_select_sessions 
        WHERE id = %s
        """
        cursor.execute(select_query, (db_id,))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            raise Exception("Не удалось извлечь данные")
        # Распаковка результатов в переменные
        best_game, spin_score, cash_score, mtt_score = result
        persent_spin_score = round((spin_score/(spin_score+mtt_score+cash_score))*100)
        persent_cash_score = round((cash_score/(spin_score+mtt_score+cash_score))*100)
        persent_mtt_score = 100 - persent_spin_score - persent_cash_score
        cursor.close()
        conn.close()

        # делаем чтобы КЭШ не был топ1
        if best_game.lower() == "cash":
            if persent_spin_score > persent_mtt_score:
                best_game = "spin"
                tmp = persent_cash_score
                persent_cash_score = persent_spin_score
                persent_spin_score = tmp
            else:
                best_game = "mtt"
                tmp = persent_cash_score
                persent_cash_score = persent_mtt_score
                persent_mtt_score = tmp

        if best_game.lower() == "mtt":
            msg_data = {
                "text": f"<b>Ваше приоритетное направление — MTT</> 🏆\n\nВы — марафонец, боец за трофеи. Вы любите глубину, стадийность, потенциальные заносы и вкус победы.\n\nMTT подходит вам на {persent_mtt_score}%, Spin & Go на {persent_spin_score}%, Cash на {persent_cash_score}%.\n\nТеперь вы знаете свои предрасположенности и можете попробовать себя в деле. Выбирайте один из наших бесплатных курсов по трём дисциплинам и получите доступ к урокам.\n\nС нас — чёткая методология, тренеры с многолетним опытом и финансирование для лучших студентов. Всё, чтобы сделать покер прибыльным делом.\n\n<b>Кликайте по кнопке ниже и выбирайте дисциплину ↓</b>",
                "buttons": [
                    [{"title": "Бесплатное обучение", "callback": "free_learning"}]
                    ],
                "file": {
                    "content_type": "image",
                    "file_path": "media/select_mtt.png",
                    "tg_filename": "select_mtt.png"
                }
            }
        elif best_game.lower() == "spin":
            msg_data = {
                "text": f"<b>Ваше приоритетное направление — Spin & Go</b> 🏆\n\nВам ближе скорость, динамика, адреналин. Хотите играть быстро, в удобное время и прогнозировать прибыль.\n\nSpin & Go подходит вам на {persent_spin_score}%, MTT на {persent_mtt_score}%, Cash на {persent_cash_score}%.\n\nТеперь вы знаете свои предрасположенности и можете попробовать себя в деле. Выбирайте один из наших бесплатных курсов по трём дисциплинам и получите доступ к урокам.\n\nС нас — чёткая методология, тренеры с многолетним опытом и финансирование для лучших студентов. Всё, чтобы сделать покер прибыльным делом.\n\n<b>Кликайте по кнопке ниже и выбирайте дисциплину ↓</b>",
                "buttons": [
                    [{"title": "Бесплатное обучение", "callback": "free_learning"}]
                    ],
                "file": {
                    "content_type": "image",
                    "file_path": "media/select_spin.png",
                    "tg_filename": "select_spin.png"
                }
            }
        else:
            msg_data = {
                "text": f"<b>Ваше приоритетное направление — Cash</b> 🏆\n\nЛюбите разбираться в спотах, строить стратегии, контролировать процесс и банкролл.\n\nCash подходит вам на {persent_cash_score}%, MTT на {persent_mtt_score}%, Spin & Go на {persent_spin_score}%.\n\nТеперь вы знаете свои предрасположенности и можете попробовать себя в деле. Выбирайте один из наших бесплатных курсов по трём дисциплинам и получите доступ к урокам.\n\nС нас — чёткая методология, тренеры с многолетним опытом и финансирование для лучших студентов. Всё, чтобы сделать покер прибыльным делом.\n\n<b>Кликайте по кнопке ниже и выбирайте дисциплину ↓</b>",
                "buttons": [
                    [{"title": "Бесплатное обучение", "callback": "free_learning"}]
                    ],
                "file": {
                    "content_type": "image",
                    "file_path": "media/select_cash.png",
                    "tg_filename": "select_cash.png"
                }
            }
        # Добавляем уведомление
        msg_data["notifications"] = [
                {
                    "message": "motivation_1",
                    "at_time": {"time": "10:00", "delta_days": 2}
                }
            ]
        
        await send_message(bot=bot,
            user_id=telegram_id,
            msg_data=msg_data,
            route=f"select_quiz_results-{best_game}",
            notification=True
            )
                
        
    except Exception as error:
        await logger.error(f"error: {error}")