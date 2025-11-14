from collections import defaultdict
import apps.logger as logger
from typing import Dict, List

class MessageManager:
    def __init__(self):
        # Формат: {user_id: [{'message_id': int}]}
        self.messages: Dict[int, List[dict]] = defaultdict(list)

    async def add_message(self, user_id: int, message_id: int):
        """Добавляет сообщение для возможного удаления"""
        self.messages[user_id].append({
            'message_id': message_id,
            'count': 0
        })

    async def delete_messages(self, bot, user_id: int):
        """Удаляет сообщения для пользователя, опционально по маршруту"""
        if user_id not in self.messages:
            return
        messages_id = []
        for msg in self.messages[user_id][:]:  # Копируем список, чтобы избежать модификации во время итерации
            try:
                if msg.get('message_id', None) and msg['count'] > 0:
                    messages_id.append(msg['message_id'])
                    self.messages[user_id].remove(msg)  # Удаляем из списка
                elif msg['message_id']:
                    self.messages[user_id][self.messages[user_id].index(msg)]['count'] += 1
                if messages_id:
                    await bot.delete_messages(chat_id=user_id, message_ids=messages_id)
            except Exception as e:
                await logger.error(f"Ошибка при удалении сообщения {msg['message_id']} для юзера {user_id}: {e}")

    def clear_messages(self, user_id: int):
        """Очищает все сообщения для пользователя"""
        if user_id in self.messages:
            self.messages[user_id].clear()
