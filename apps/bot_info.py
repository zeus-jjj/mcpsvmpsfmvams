"""
класс для хранения данных о боте. Данные попадают сюда из bot.py -> on_startup
"""

class BOT_INFO():
	def __init__(self):
		self.id = 0
		self.username = None

	def set_id(self, id):
		self.id = id

	def set_username(self, username):
		self.username = username

	def get_id(self):
		return self.id

	def get_username(self):
		return self.username

bot_info = BOT_INFO()
