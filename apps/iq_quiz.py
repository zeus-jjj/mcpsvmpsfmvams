import json
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import binascii
import mysql.connector
# 
from modules import ENCRYPTION_KEY, ENCRYPTION_IV, MYSQL_CONFIG
import apps.logger as logger
from apps.funcs import send_message

results = [
            { 'score' : 64, 'level': 'Донатор - «погружаюсь в атмосферу»', 'text': 'IQ 50-64 — Настоящая магия покера раскрывается с первым сыгранным и разобранным банком. У нас есть интерактивный тренажёр, где вы ничем не рескуете, а еще есть сотни ребят, которые только начинают свой путь в покере. Присоединяйтесь!'},
            { 'score' : 84, 'level': 'Фиш - лёгкая мишень за столом', 'text': 'IQ 65-84 — Каждый профи когда-то был фишом. Вступайте в наше сообщество, пройдите наши курсы и перепройдите тест. Вы увидите, насколько это просто и интересно изучать покер!'},
            { 'score' : 94, 'level': 'Новичок - базовые концепты усвоены', 'text': 'IQ 85-94 — Вы уже знаете правила, осталось закрепить матчасть. Наши продвинутые курсы помогут изучить слепые зоны в покере, и начать стабильно расти по игровым лимитам!'},
            { 'score' : 104, 'level': 'Хобби-Игрок', 'text': 'IQ 95-104 — Игра ради удовольствия может быть плюсовой! Наши продвинутые курсы помогут изучить слепые зоны в покере, и начать стабильно расти по игровым лимитам!'},
            { 'score' : 114, 'level': 'Полу-Рег - плюсуете, но ещё учитесь', 'text': 'IQ 105-114 — Пот-оддсы и позиция уже ваши друзья. Наши продвинутые курсы помогут изучить слепые зоны в покере, и начать стабильно расти по игровым лимитам!'},
            { 'score' : 129, 'level': 'Сильный Регуляр - mid-stakes', 'text': 'IQ 115-129 — База прочна — пора прокачать эксплойт. Оттачивайте навыки в нашем тренажере и обсуждайте раздачи в телеграм-сообществе, впереди интересный путь к вершинам покера!'},
            { 'score' : 139, 'level': 'Покерный Профи - устойчивый плюсовый рег', 'text': 'IQ 130-139 — Длинная дистанция и стрики для вас не страшны. В нашем телеграм сообществе вы найдете новые вызовы, решение которых, обязательно приведут вас к успеху!'},
            { 'score' : 149, 'level': 'High-Roller', 'text': 'IQ 140-149 — Вы остановились в шаге от ТОПа! Вы уже умеете читать диапазоны оппонентов на всех улицах, но нет пределу совершенству - есть куда расти! Присмотритесь к нашему онлайн сообществу, уверен, что нам будет чем вас заинтересовать!'},
            { 'score' : 160, 'level': 'GTO/Exploit-БОГ играете «как солвер»', 'text': 'Ваш IQ попадает в диапазон 150-160 — Это лучший результат из возможных! Вы видите EV-линию ещё до того, как дилер подумает достать ривер-карту. Лучшим вариантом дальнейшего развития для вас будет помощь более слабым игрокам. Присоединяйтесь к нашему Телеграм сообществу, предлагайте новые идеи и форматы, мы будем рады развиваться сообща с таким разбирающимся игроком!'},
        ]


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
        UPDATE quiz_sessions
        SET telegram_id = %s, telegram_username = %s
        WHERE id = %s
        """
        cursor.execute(query, (telegram_id, telegram_username, db_id))
        if cursor.rowcount == 0:
            await logger.debug("Запись не найдена или не обновлена")
        conn.commit()



        # Извлечение iq_score
        select_query = """
        SELECT iq_score FROM quiz_sessions WHERE id = %s
        """
        cursor.execute(select_query, (db_id,))
        iq_score = cursor.fetchone()
        if not iq_score:
            raise Exception("Не удалось извлечь iq_score")
        iq_score = int(iq_score[0])
        cursor.close()
        conn.close()

        for res_data in results:
            if res_data['score'] >= iq_score:

                msg_data = {"text": f"Привет! Ваш IQ Score: {iq_score}\n\nУровень: {res_data['level']}\n\n{res_data['text']}",
                "buttons": [
                    [{"title": "Таблица лидеров", "web_app": "https://pokerhub.app/iq-leaderboard?{user_id}"}],
                    [{"title": "Бесплатное обучение", "callback": "quiz_free_learning"}],
                    [{"title": "Наш тг-канал", "link": "https://t.me/+YyeoKwNJvIoyYjUy"}]
                    ]
                }

                msg_data = {
                    "text": f"Привет! Ваш IQ Score: {iq_score}\n\nУровень: {res_data['level']}\n\n{res_data['text']}",
                    "buttons": [
                        [{"title": "Таблица лидеров", "web_app": "https://pokerhub.app/iq-leaderboard?{user_id}"}],
                        [{"title": "Бесплатное обучение", "callback": "quiz_free_learning"}]
                        ],
                        "notifications": [
                        {
                            "message": "notif_iq_quiz",
                            "at_time": {"time": "10:00", "delta_days": 1}
                        }
                    ]
                }
                await send_message(bot=bot,
                    user_id=telegram_id,
                    msg_data=msg_data,
                    route="iq_quiz_results",
                    notification=True
                    )
                break
        
    except Exception as error:
        await logger.error(f"error: {error}")