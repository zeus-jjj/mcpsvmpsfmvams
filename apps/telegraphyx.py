import aiohttp
# 
import apps.logger as logger

async def send_to_telegraphyx(start: str) -> bool:
    url = f"https://app.telegraphyx.ru/api/bot/start?start={start}"
    await logger.debug(f"Отправляем GET-запрос по адресу: {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return True
            else:
                await logger.error(f"Ответ от telegraphyx: {await response.text()}")
                return False