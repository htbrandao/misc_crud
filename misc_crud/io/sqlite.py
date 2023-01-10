import sqlite3


class __ModSQLite:
    """
    Classe para modelar conexão e interação com banco de dados SQLite.
    """

    def __init__(self, database: str):
        self.__conn = sqlite3.connect(database=database, timeout=3)
        self.__cursor = self.__conn.cursor()

    def __commit(self):
        self.__conn.commit()

    def __rowcount(self) -> int:
        return self.__cursor.rowcount

    def select(self, *, query: str, valores: dict) -> tuple:
        self.__cursor.execute(query, valores)
        _ = self.__cursor.fetchall()
        return (len(_), _)

    def select_one(self, *, query: str, valores: dict) -> tuple:
        self.__cursor.execute(query, valores)
        return self.__cursor.fetchone()

    def execute(self, *, query: str, valores: dict) -> int:
        self.__cursor.execute(query, valores)
        self.__commit()
        return self.__rowcount()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__del__()

    def __del__(self):
        self.__cursor.close()
        self.__conn.close()


class DatabaseSQLite(__ModSQLite):
    def __init__(self, database: str):
        super().__init__(database=database)


from sqlite.sqlite import DatabaseSQLite


class Queries:
    """
    Classe para abstrair as queries para lidar com dados no SQLite.

    :meta public:
    """

    TABELA = 'HISTORICO'

    COLUNAS = ['ID', 'TIMESTAMP', 'FORMATO', 'PROCESSADO']

    COUNT = """SELECT * FROM "HISTORICO;"""

    PUT_REG = """
              INSERT INTO "HISTORICO" ("ID", "TIMESTAMP") VALUES (:identificador, :timestamp);
              """

    UPDATE_REG = """
                 UPDATE "HISTORICO"
                    SET
                        ("FORMATO", "PROCESSADO") = (:formato, :timestamp)
                 WHERE "ID" = :identificador;
                 """

    GET_REG = """
              SELECT * FROM "HISTORICO" WHERE "ID" = :identificador;
              """

    DELETE_REG = """
                 DELETE FROM "HISTORICO" WHERE "ID" = :identificador;
                 """


class Historico:
    """
    Classe para abstrair os métodos para lidar com dados no SQLite.

    :meta public:
    """

    @staticmethod
    def put(conn: DatabaseSQLite, identificador: str, timestamp: int) -> int:
        """
        Realiza o insert de um registro.

        :param DatabaseSQLite conn: conexão com o banco de dados
        :param str identificador: id do registro
        :param int timestamp: timestam do registro
        :return: quantidade de registros escritos
        :rtype: int
        """
        return conn.execute(query=Queries.PUT_REG,
                            valores={'identificador': identificador, 'timestamp': timestamp})

    @staticmethod
    def update(conn: DatabaseSQLite, identificador: str, formato: str, timestamp: int) -> int:
        """
        Realiza o update de um registro.

        :param DatabaseSQLite conn: conexão com o banco de dados
        :param str identificador: id do registro
        :param int timestamp: timestam do registro
        :return: quantidade de registros escritos
        :rtype: int
        """
        return conn.execute(query=Queries.UPDATE_REG,
                            valores={'identificador': identificador, 'formato': formato, 'timestamp': timestamp})

    @staticmethod
    def get(conn: DatabaseSQLite, identificador: str) -> tuple:
        """
        Realiza um select de acordo com o id.

        :param DatabaseSQLite conn: conexão com o banco de dados
        :param str identificador: id do registro
        :return: registro em forma de tupla (id, timestamp) -> (str, float)
        :rtype: tuple
        """
        return conn.select_one(query=Queries.GET_REG, valores={'identificador': identificador})

    @staticmethod
    def delete(conn: DatabaseSQLite, identificador: str) -> int:
        """
        Realiza um delete de acordo com o id.

        :param DatabaseSQLite conn: conexão com o banco de dados
        :param str identificador: id do registro
        :return: quantidade de registros apagados
        :rtype: int
        """
        return conn.execute(query=Queries.DELETE_REG, valores={'identificador': identificador})

    @staticmethod
    def show(conn: DatabaseSQLite) -> tuple:
        """
        Recupera todos os registro (full scan).

        :param DatabaseSQLite conn: conexão com o banco de dados
        :return: todos os registros, retornando uma tuple[int, list[tuple]]
        :rtype: tuple
        """
        *_, todos_registros = conn.select(query=Queries.COUNT, valores={})
        return todos_registros
