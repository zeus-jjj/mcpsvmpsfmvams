import inspect
# 
from datetime import datetime
from functools import wraps
# 
from colorama import Fore, Style, init
import aioconsole



# Инициализация colorama
init(autoreset=True)



# Декоратор для автоматического добавления имени модуля и функции
def log_with_context(log_func):
    @wraps(log_func)
    async def wrapper(text, *args, **kwargs):
        frame = inspect.currentframe().f_back  # Берём предыдущий стек вызова
        module_name = inspect.getmodule(frame).__name__  # Получаем имя модуля
        function_name = frame.f_code.co_name  # Получаем имя функции
        
        # Формируем сообщение с модулем и функцией
        log_text = f"[{module_name}.{function_name}] {text}"
        await log_func(log_text, *args, **kwargs)  # Передаём дальше в логгер
    return wrapper


# Асинхронные функции логирования
@log_with_context
async def debug(text):
    await aioconsole.aprint(
        Style.BRIGHT + datetime.now().strftime("%d-%m-%Y : %H:%M:%S"),
        ">",
        Fore.CYAN + "[DEBUG]",
        ">",
        Style.BRIGHT + Fore.CYAN + text
    )


@log_with_context
async def info(text):
    await aioconsole.aprint(
        Style.BRIGHT + datetime.now().strftime("%d-%m-%Y : %H:%M:%S"),
        ">",
        Fore.GREEN + "[INFO ]",
        ">",
        Style.BRIGHT + Fore.GREEN + text
    )


@log_with_context
async def error(text):
    await aioconsole.aprint(
        Style.BRIGHT + datetime.now().strftime("%d-%m-%Y : %H:%M:%S"),
        ">",
        Fore.RED + "[ERROR]",
        ">",
        Style.BRIGHT + Fore.RED + text
    )
