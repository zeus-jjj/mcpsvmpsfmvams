import aiohttp
import asyncio
from os import getenv
# 
from . import logger
from modules import create_connect, get_host, auth, AMO_TOKEN, AMO_DOMAIN
from apps.bot_info import bot_info


# ID воронки ("Новые клиенты")
pipeline_id = 1976686
# ID этапа в воронке ("Получена заявка")
status_id = 29327686

# # Шаблон данных для заявки
# custom_fields = [
#     {"field_id": 610331, "values": [{"value": "Женя"}]}, # имя
#     {"field_id": 1070945, "values": [{"value": "89788912666"}]}, # номер
#     {"field_id": 616985, "values": [{"value": "@tracenull"}]}, # мессенджер (ссылка на ТГ или ТГ id)
#     {"field_id": 574715, "values": [{"value": "Хочет проконсультироваться по развитию в команде"}]} # причина подачи заявки в команду

# ]

fields_template = {
    "phone": 1070945, # номер
    "first_name": 610331, # имя
    "messenger": 616985, # мессенджер (ссылка на ТГ или ТГ id)
    "reason": 574715, # причина подачи заявки в команду
    "profile_link": 1080611 # ссылка на профиль в веб-панели
}


def get_headers():
    return {
        'Authorization': f'Bearer {AMO_TOKEN}',
        'Content-Type': 'application/json'
    }

# создаём пустую заявку
async def create_lead(session):
    url = f'{AMO_DOMAIN}/api/v4/leads'
    payload = {
        "data": None
    }
    async with session.post(url, json=payload, headers=get_headers(), ssl=False) as response:
        server_answer = await response.json()
        if response.status == 200:
            lead_id = server_answer['_embedded']['leads'][0]['id']
            await logger.info(f"Успешно создана пустая заявка {lead_id}!")
            return lead_id
        else:
            await logger.error(f"Ошибка создания пустой заявки. Статус-код: {response.status}. Ошибка: {server_answer}")
            return None

# обновляем созданную заявку
async def update_lead(session, lead_id, name, data):
    url = f'{AMO_DOMAIN}/api/v4/leads/{lead_id}'

    # если не передан мессенджер - собираем его сами
    if "messenger" not in data:
        data.update({"messenger": f'https://t.me/{data.get("username")}'} if data.get("username", None) else f'telegram_id={data.get("user_id", "unknown")}')

    payload = {
        'name': name,
        'status_id': status_id,
        'pipeline_id': pipeline_id,
        'custom_fields_values': [{"field_id": fields_template[key], "values": [{"value": value}]} for key, value in data.items() if key in fields_template.keys()]
    }
    

    async with session.patch(url, json=payload, headers=get_headers(), ssl=False) as response:
        server_answer = await response.json()
        if response.status == 200:
            await logger.info(f"Заявка {lead_id} успешно обновлена!")
            return server_answer
        else:
            await logger.error(f"Ошибка редактирования заявки. Статус-код: {response.status}. Ошибка: {server_answer}")
            return None

# добавляет/обновляет заявку
async def process_lead(data, lead_id=None):
    async with aiohttp.ClientSession() as session:
        # если не передай id заявки
        if lead_id is None:
            # пытаемся добавить пустую заявку. Если не получилось
            if (lead_id := await create_lead(session=session)) is None:
                # возвращаем None
                return None
            # если получилось
            else:
                # добавляем лид в БД
                await add_lead(user_id=data.get("user_id"), lead_id=lead_id, status=status_id)
        # обновляем данные в заявке
        update_response = await update_lead(session=session,
            lead_id=lead_id, 
            name=f"Заявка #{lead_id} на консультацию в команду из ТГ-бота @{bot_info.get_username()}", 
            data=data)

        return lead_id if update_response is not None else None



# для добавления лида в БД
async def add_lead(user_id, lead_id, status):
    input_data = {
        "key": auth,
        "user_id": user_id,
        "lead_id": lead_id,
        "status": status
        }
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{get_host()}/amo_leads/add_lead", json=input_data) as response:
            # Проверка успешности запроса
            if response.status == 200:
                return True
            else:
                await logger.error(f'Ошибка при создании заявки в БД. Ответ сервера: statuscode={response.status}')
                return False

# отправка сообщения от юзера для занесения в БД
async def send_amo_msg(lead_id, msg, is_user=True):
    input_data = {
        "key": auth,
        "message": msg,
        "lead_id": lead_id,
        "is_user": is_user
        }
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{get_host()}/amo_leads/send_message", json=input_data) as response:
            # Проверка успешности запроса
            if response.status == 200:
                return True
            else:
                await logger.error(f'Ошибка при отправке сообщения от юзера в бекенд. Ответ сервера: statuscode={response.status}')
                return False

# проверка на активную заявку в АМО
async def check_active_lead(user_id):
    input_data = {
        "key": auth,
        "user_id": user_id
        }
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{get_host()}/amo_leads/is_have_amo_lead", json=input_data) as response:
            # Проверка успешности запроса
            if response.status == 200:
                server_answer = await response.json()
                return server_answer.get("lead_id")
            else:
                await logger.error(f"Не удалось сделать запрос на сервер для определения активной заявки в АМО для юзера {user_id}")
                return None


if __name__ == "__main__":
    asyncio.run(process_lead(name="тестовая заявка"))
