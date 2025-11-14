import time
import json
import logging
from ragflow_sdk import RAGFlow, Agent
# 
from modules import ragflow_token, ragflow_url
import apps.logger as logger
# 
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton



# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
sync_logger = logging.getLogger(__name__)

class RagFlowClient:
    """
    Класс-обёртка для работы с RagFlow API.
    Он инкапсулирует основные операции для создания датасетов, загрузки документов,
    управления чат-ассистентами и общения с ними.
    """
    def __init__(self, api_key: str, base_url: str):
        """
        Инициализация клиента RagFlow.
        :param api_key: API-ключ RagFlow.
        :param base_url: Базовый URL RagFlow (например, "http://localhost:9380").
        """
        try:
            self.client = RAGFlow(api_key=api_key, base_url=base_url)
            sync_logger.info("RagFlow клиент успешно инициализирован.")
        except Exception as e:
            sync_logger.error(f"Ошибка инициализации RagFlow клиента: {e}")
            raise e

        # Выбираем в качестве основного датасета первый датасет
        self.dataset = self.choose_dataset()

    # -------------
    # Работа с датасетами
    # -------------
    def list_datasets(self, **kwargs):
        """
        Получает список датасетов.
        Допустимые параметры: name, id, page, page_size и т.д.
        :return: Список объектов Dataset.
        """
        try:
            datasets = self.client.list_datasets(**kwargs)
            sync_logger.info(f"Найдено {len(datasets)} датасетов.")
            return datasets
        except Exception as e:
            sync_logger.error(f"Ошибка получения датасетов: {e}")
            return []

    def choose_dataset(self):
        # Получаем список датасетов
        datasets = self.client.list_datasets()
        if datasets:
            dataset = datasets[0]
            sync_logger.info(f"Датасетов всего: {len(datasets)}")
            sync_logger.info(f"Выбран первый датасет: name: {dataset.name}, description: {dataset.description}, id: {dataset.id}")
        else:
            # Создаем новый датасет, если нет существующих
            dataset = self.client.create_dataset(name="main_dataset")
            sync_logger.info("Датасеты не найдены. Создан новый!")
        return dataset

    def create_dataset(self, name: str, avatar: str = "", description: str = "",
                       embedding_model: str = None, language: str = "English",
                       permission: str = "me", chunk_method: str = "naive", parser_config: dict = None):
        """
        Создает датасет.
        :param name: Уникальное имя датасета.
        :param avatar: Base64 код аватара (по умолчанию пустая строка).
        :param description: Описание датасета.
        :param embedding_model: Имя модели эмбеддингов (если None, используется значение по умолчанию).
        :param language: Язык датасета.
        :param permission: Уровень доступа ("me" или "team").
        :param chunk_method: Метод разбиения документа.
        :param parser_config: Конфигурация парсера (зависит от chunk_method).
        :return: Объект Dataset.
        """
        try:
            ds = self.client.create_dataset(
                name=name,
                avatar=avatar,
                description=description,
                embedding_model=embedding_model if embedding_model else "",
                language=language,
                permission=permission,
                chunk_method=chunk_method,
                parser_config=parser_config
            )
            sync_logger.info(f"Датасет '{name}' создан: ID={ds.id}")
            return ds
        except Exception as e:
            sync_logger.error(f"Ошибка создания датасета '{name}': {e}")
            raise e

    def upload_document(self, dataset, file_path: str):
        """
        Загружает документ в указанный датасет.
        :param dataset: Объект Dataset.
        :param file_path: Путь к файлу для загрузки.
        """
        try:
            with open(file_path, "rb") as f:
                blob = f.read()
            dataset.upload_documents([{"display_name": file_path, "blob": blob}])
            sync_logger.info(f"Документ '{file_path}' успешно загружен в датасет '{dataset.name}'.")
        except Exception as e:
            sync_logger.error(f"Ошибка загрузки документа '{file_path}' в датасет '{dataset.name}': {e}")
            raise e

    # -------------
    # Работа с чат-ассистентами
    # -------------
    def list_assistants(self, **kwargs):
        """
        Возвращает список чат-ассистентов.
        Допустимые параметры: page, page_size, orderby, id, name и т.д.
        :return: Список объектов Chat (ассистентов).
        """
        try:
            chats = self.client.list_chats(**kwargs)
            sync_logger.info(f"Найдено {len(chats)} чат-ассистентов.")
            return chats
        except Exception as e:
            sync_logger.error(f"Ошибка получения списка чат-ассистентов: {e}")
            return []


    def create_assistant(self, name: str, dataset_ids: list = []):
        """
        Создает чат-ассистента.
        :param name: Имя чат-ассистента.
        :param dataset_ids: Список ID датасетов, которые будут связаны с чатом.
        :return: Объект Chat.
        """
        try:
            chat = self.client.create_chat(name, dataset_ids=dataset_ids)
            sync_logger.info(f"Чат-ассистент '{name}' создан: ID={chat.id}")
            return chat
        except Exception as e:
            sync_logger.error(f"Ошибка создания чат-ассистента '{name}': {e}")
            raise e


    def list_sessions(self, assistant, **kwargs):
        """
        Получает список сессий (чатовых историй) для указанного чат-ассистента.
        Допустимые параметры: page, page_size, orderby, id, name и т.д.
        :return: Список объектов Session.
        """
        try:
            sessions = assistant.list_sessions(**kwargs)
            sync_logger.info(f"Найдено {len(sessions)} чатов для ассистента '{assistant.name}'.")
            return sessions
        except Exception as e:
            sync_logger.error(f"Ошибка получения сессий для чата '{assistant.name}': {e}")
            return []

    # -------------
    # Работа с сообщениями и сессиями чата
    # -------------
    def create_chat(self, assistant, session_name: str = "New session"):
        """
        Создает сессию с указанным чат-ассистентом.
        :param assistant: Объект Chat.
        :param session_name: Имя сессии.
        :return: Объект Session.
        """
        try:
            session = assistant.create_session(name=session_name)
            sync_logger.info(f"Сессия '{session_name}' создана для чата '{assistant.name}': ID={session.id}")
            return session
        except Exception as e:
            sync_logger.error(f"Ошибка создания сессии для чата '{assistant.name}': {e}")
            raise e

    def clear_chat_history(self, assistant, ids=None):
        try:
            assistant.delete_sessions(ids)
            sync_logger.info(f"Чаты с айди: {ids} очищены.")
        except Exception as e:
            sync_logger.error(f"Ошибка очистки чатов {ids}: {e}")
            raise e











async def parse_answer(text):
    """
    Функция парсит из ответа ИИ текст ответа и клавиатуру (answer и buttons).
    Используется для вывода ответов от RAGFlow-системы (ИИ-помощник)
    """
    try:
        data = json.loads(text, strict=False)
    except Exception as error:
        await logger.error(f"Не удалось преобразовать весь текст в словарь: {error}")
        try:
            data = json.loads(text[text.find("{"):text.find("}")+1], strict=False)
        except Exception as error:
            await logger.error(f"Не удалось преобразовать извлеченный текст в словарь: {error}")
            data = text
    keyboard = None
    answer = text 
    if isinstance(data, dict):
        try:
            buttons = ((KeyboardButton(text=btn_text), ) for btn_text in data.get("buttons", []))
            # Создаём клавиатуру
            keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
        except Exception as e:
            await logger.error(f'Ошибка при формировании клавиатуры: {e}')
        answer = data.get('answer', '')
    return answer, keyboard

async def send_msg_to_rag(bot, user_id, message_text):
    session_name = f"{session_base_name}_{user_id}"
    assistant = client.list_assistants(name=assistant_name)
    if not assistant:
        await logger.error(f"Не удалось найти ассистента с названием {assistant_name}, создаём нового.")
        assistant = client.create_assistant(name=f"", dataset_ids=[self.dataset.id])
        session = client.create_chat(assistant=assistant, session_name=session_name)
    else:
        # Берём первое совпадение
        assistant = assistant[0]
        session = client.list_sessions(assistant=assistant, name=session_name)
        if not session:
            await logger.debug(f"Сессия {session_name} не найдена, создаём новую")
            session = client.create_chat(assistant=assistant, session_name=session_name)
        else: 
            # Берём первую совпавшую сессию
            session = session[0]
            await logger.info(f"Выбрана сессия: ID={session.id}, Name={session.name}")
    
    response = session.ask(question=message_text, stream=True)
    
    full_text = ""
    for chunk in response:
        full_text = chunk.content

    if full_text:
        # Извлекаем из ответа ИИ текст и кнопки
        return (await parse_answer(text=full_text))
    else:
        await logger.error(f"В response нет текста для извлечения! response={response}")
        return None







# Название ИИ-помощника
assistant_name = "BaseAssistant"
# Базовое имя нового чата (добавляем айди юзера)
session_base_name = f"Session"

# Объект для работы с РАГ
client = RagFlowClient(api_key=ragflow_token, base_url=ragflow_url)

client = None




# переделать часть функций (или все) на http-запросы через aiohttp, либо функции текущие обернуть в
# неблокирующие потоки


if __name__ == "__main__":
    pass
    # # Конфигурация клиента RagFlow
    # API_KEY = "ragflow-c5YzU1Y2Y0ZjgwZDExZWZiNDEwYjJiYj"
    # BASE_URL = "https://telegram.pokerhub.pro/rag"  # Например, http://localhost:9380

    # client = RagFlowClient(api_key=API_KEY, base_url=BASE_URL)

    # # Получаем список датасетов
    # datasets = client.list_datasets()
    # if datasets:
    #     dataset = datasets[0]
    #     logger.info(f"Датасетов всего: {len(datasets)}")
    #     logger.info(f"Выбран первый датасет: name: {dataset.name}, description: {dataset.description}, id: {dataset.id}")
    # else:
    #     # Создаем новый датасет, если нет существующих
    #     dataset = client.create_dataset(name="kb_demo")
    #     logger.info("Датасеты не найдены. Создан новый!")


    # # Получение списка ассистентов
    # assistants = client.list_assistants()
    # if assistants:
    #     logger.info(f"Ассистентов всего: {len(assistants)}")
    #     for assistant in assistants:
    #         logger.info(f"Ассистент: ID={assistant.id}, Name={assistant.name}")
    # else:
    #     logger.info("Чат-ассистенты отсутствуют.")


    # # Пример: присоединение к существующему чату по ID
    # CHAT_ID = "c4c905b6f80d11efba95b2bb93949cfe"
    # chat = client.join_chat_by_id(CHAT_ID)
    # if chat is None:
    #     logger.error("Не удалось найти чат по заданному ID, создаём новый.")
    #     chat = client.create_assistant(name="Demo Assistant", dataset_ids=[dataset.id])
    #     session = client.create_session(assistant, session_name="Demo Session")
    # else:
    #     # Получение списка сессий для выбранного чата
    #     sessions = client.list_sessions(chat)
    #     if sessions:
    #         session = sessions[0]
    #         logger.info(f"Выбрана сессия: ID={session.id}, Name={session.name}")
    #     else:
    #         logger.info("Сессий не найдено, создаем новую сессию.")
    #         session = client.create_session(chat, session_name="Demo Session")

    # # Отправка сообщения и получение ответа (не потоковый режим)
    # question = "Привет, как дела?"
    # logger.info(f"Обращение: {question}")

    # response = session.ask(question, False)
    # for chunk in response:
    #     full_message = chunk.content

    # logger.info(f"Ответ ассистента: {full_message}")



    # # Загружаем документ в датасет
    # file_path = "example.txt"  # путь к файлу
    # client.upload_document(dataset, file_path)

    # # Создаем чат-ассистента и сессию
    # assistant = client.create_assistant(name="Demo Assistant", dataset_ids=[dataset.id])
    # session = client.create_session(assistant, session_name="Demo Session")

    # # Отправляем сообщение и получаем ответ (не потоковый режим)
    # response = client.send_message(session, "Привет, как дела?", stream=False)
    # print("Ответ ассистента:", response)

    # Если требуется потоковый вывод
    # for chunk in client.send_message(session, "Расскажи что-нибудь интересное", stream=True):
    #     print(chunk.content, end="", flush=True)

    # Очистка истории чата (если требуется)
    # client.clear_chat_history(assistant)
