import mysql.connector
from mysql.connector import Error

class MySQLDatabase:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                print("Conectado ao MySQL Server versão ", db_info)
                return self.connection
        except Error as e:
            print("Erro ao conectar ao MySQL", e)

    def close(self):
        if self.connection.is_connected():
            self.connection.close()
            print("Conexão ao MySQL está fechada")

    def execute_query(self, query, params=None):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)  # Alteração aqui para aceitar parâmetros
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"Erro: {e}")

