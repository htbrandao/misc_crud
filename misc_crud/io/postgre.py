import psycopg2

from psycopg2 import OperationalError
from psycopg2.extras import DictCursor

import logging as logger


class __ModPostgre:
    """
    Classe para modelar conexão e interação com banco de dados Postgre.
    """

    def __init__(self, host: str, porta: int, database: str, usuario: str, senha: str):
        try:
            self.__conexao = psycopg2.connect(database=database, user=usuario, password=senha, host=host, port=porta)
            self.__cursor = self.__conexao.cursor(cursor_factory=DictCursor)
        except OperationalError as e:
            logger.critical(f'Falha de conexao com o banco de dados: {e}')

    def __commit(self):
        self.__conexao.commit()

    def __rowcount(self) -> int:
        return self.__cursor.rowcount

    def select(self, *, query: str, values: dict) -> tuple:
        self.__cursor.execute(query, values)
        _ = self.__cursor.fetchall()
        return (len(_), _)

    def select_one(self, *, query: str, values: dict) -> tuple:
        self.__cursor.execute(query, values)
        return self.__cursor.fetchone()

    def execute(self, *, query: str, values: dict) -> int:
        self.__cursor.execute(query, values)
        self.__commit()
        return self.__rowcount()

    def __enter__(self):
        return self

    def __del__(self):
        self.__cursor.close()
        self.__conexao.close()

    def __exit__(self, type, value, traceback):
        self.__del__()


class DatabasePostgre(__ModPostgre):
    def __init__(self,
                 host: str,
                 porta: str,
                 database: str,
                 usuario: str,
                 senha: str):
        super().__init__(host=host, porta=porta, database=database, usuario=usuario, senha=senha)


from postgre import DatabasePostgre


class Queries:
    """
    Classe para abstrair as queries para lidar com dados no Postgre.

    :meta public:
    """

    TABELA = 'HISTORICO'

    COLUNAS = ['ID', 'TIMESTAMP', 'FORMATO', 'PROCESSADO']

    COUNT = """SELECT * FROM "HISTORICO";"""

    PUT_REG = """
              INSERT INTO "HISTORICO" ("ID", "TIMESTAMP") VALUES (%(identificador)s, %(timestamp)s);
              """

    UPDATE_REG = """
                 UPDATE "HISTORICO"
                    SET
                        ("FORMATO", "PROCESSADO") = (%(formato)s, %(timestamp)s)
                 WHERE "ID" = %(identificador)s;
                 """

    GET_REG = """
              SELECT * FROM "HISTORICO" WHERE "ID" = %(identificador)s;
              """

    DELETE_REG = """
                 DELETE FROM "HISTORICO" WHERE "ID" = %(identificador)s;
                 """


class Historico:
    """
    Classe para abstrair os métodos para lidar com dados no Postgre.

    :meta public:
    """

    @staticmethod
    def put(conn: DatabasePostgre, identificador: str, timestamp: int) -> int:
        """
        Realiza o insert de um registro.

        :param DatabasePostgre conn: conexão com o banco de dados
        :param str identificador: id do registro
        :param int timestamp: timestam do registro
        :return: quantidade de registros escritos
        :rtype: int
        """
        return conn.execute(query=Queries.PUT_REG,
                            valores={'identificador': identificador, 'timestamp': timestamp})

    @staticmethod
    def update(conn: DatabasePostgre, identificador: str, formato: str, timestamp: int) -> int:
        """
        Realiza o update de um registro.

        :param DatabasePostgre conn: conexão com o banco de dados
        :param str identificador: id do registro
        :param int timestamp: timestam do registro
        :return: quantidade de registros escritos
        :rtype: int
        """
        return conn.execute(query=Queries.UPDATE_REG,
                            valores={'identificador': identificador, 'formato': formato, 'timestamp': timestamp})

    @staticmethod
    def get(conn: DatabasePostgre, identificador: str) -> tuple:
        """
        Realiza um select de acordo com o id.

        :param DatabasePostgre conn: conexão com o banco de dados
        :param str identificador: id do registro
        :return: registro em forma de tupla (id, timestamp) -> (str, float)
        :rtype: tuple
        """
        return conn.select_one(query=Queries.GET_REG, valores={'identificador': identificador})

    @staticmethod
    def delete(conn: DatabasePostgre, identificador: str) -> int:
        """
        Realiza um delete de acordo com o id.

        :param DatabasePostgre conn: conexão com o banco de dados
        :param str identificador: id do registro
        :return: quantidade de registros apagados
        :rtype: int
        """
        return conn.execute(query=Queries.DELETE_REG, valores={'identificador': identificador})

    @staticmethod
    def show(conn: DatabasePostgre) -> tuple:
        """
        Recupera todos os registro (full scan).

        :param DatabasePostgre conn: conexão com o banco de dados
        :return: todos os registros, retornando uma tuple[int, list[tuple]]
        :rtype: tuple
        """
        *_, todos_registros = conn.select(query=Queries.COUNT, valores={})
        return todos_registros
