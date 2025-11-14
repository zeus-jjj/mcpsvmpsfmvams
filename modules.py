import re
from os.path import join
from os import getenv
from aiogram.types import FSInputFile
from json import loads
import asyncpg
import base64
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import Dispatcher
# 
from aiogram import Bot
# 
from apps.msg_deleter import MessageManager

# Это для iq-квиза
ENCRYPTION_KEY = b'jkb342j3b98u32hrh98ewhfroi3u98r0'  # 32 байта для AES-256
ENCRYPTION_IV = b'sdkjg09843j092jf'  # 16 байт
# Конфигурация MySQL
MYSQL_CONFIG = {
            'host': 'realifod.beget.tech',
            'user': 'realifod_iq_hub',
            'password': 'Mvh54g2y',
            'database': 'realifod_iq_hub'
}


# грузим данные из файла шаблонов текста для сообщений
MAP = loads(open('map.json', 'r', encoding='utf-8').read())

# Токен доступа к локальной ragflow
ragflow_token = getenv("ragflow_token")
# Базовый путь к ragflow
ragflow_url = getenv("ragflow_url")

# JIVO-backend базовый адрес моего бекенда-интегратора с JIVO
JIVO_INTEGRATOR_URL = getenv("JIVO_INTEGRATOR_URL", "http://localhost:9200")

# токен доступа к АМО
AMO_TOKEN = getenv("AMO_TOKEN")
# ссылка на АМО
AMO_DOMAIN = getenv("AMO_DOMAIN")
# для авторизации в бекенде
auth = getenv('auth')
# auth - это шифрованный токен бота, чтобы бекенд проверял, от кого приходит запросы
headers = {'Authorization': auth}

# ограничение на кол-вол символов в столбце text аблицы user_history. Это ограничение задаётся в самой БД,
# чтобы не перегружать её тонной повторяющейся информации, но чтобы можно было понять что за текст получил юзер.
# При записи в бд текст обрезается до этого кол-ва символов SQL-запросом, чтобы те кто будет смотреть историю
# юзера в веб-панели понимали +-, о чем речь вообще. В __init__.py в бекенде прописано тоже самое, т.к. там создаются записи ответов на тикеты
MAX_CHARS_USERS_HISTORY = 255

bot = Bot(getenv('BOT_TOKEN'))
storage = MemoryStorage()
# создаём диспетчер
dp = Dispatcher(storage=storage)

# Инициализируем менеджер сообщений
message_manager = MessageManager()

def get_host() -> str:
    return "http://localhost:3095/api"
    # return 'http://94.130.236.66:3095/api' if name == 'nt' else 'https://telegram.pokerhub.pro/api'

def get_key_b64() -> str:
    # ключ для шифрования id юзера для покерхаб
    key_b64 = base64.urlsafe_b64encode(getenv('CRYPT_KEY').encode())
    return key_b64

def get_static(file_name: str) -> FSInputFile:
    return FSInputFile(path=join(getenv('static_folder'), 'messages', file_name))


def get_data(payload: str) -> dict:
    if not payload or len(payload) < 3:
        return {}

    data = {}

    # Разбиваем по разделителям ___ и -
    # Можно использовать регулярное выражение для разделения по нескольким разделителям
    parts = re.split(r'___|-', payload)

    for param in parts:
        # Ищем разделитель ключ-значение: либо '=', либо '_'
        if '=' in param:
            key_value = param.split('=', 1)
        elif '_' in param:
            key_value = param.split('_', 1)
        else:
            # Неизвестный формат, пропускаем
            continue

        if len(key_value) == 2:
            key, value = key_value
            key = key.strip()
            value = value.strip()
            if key and value:
                data[key] = value

    return data

# для соединения с БД
async def create_connect():
    return await asyncpg.connect(
        host=getenv('db_host'),
        port=getenv('db_port'),
        database=getenv('db_name'),
        user=getenv('db_user'),
        password=getenv('db_password')
    )

# регистрируем универсальное состояние для сообщений
class FSMStates(StatesGroup):
    fsm_context = State()