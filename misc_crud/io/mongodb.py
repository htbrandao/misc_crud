from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import logging as logger


class PacotaoMongoException(DiyNlpException):
    """
    Exceção para problema com MongoDB.

    :param status_code int: status_code
    :param mensagem str: mensagem de exceção
    :return: PacotaoMongoException
    :rtype: PacotaoMongoException
    """
    def __init__(self, mensagem: str, excecao=None):
        self.mensagem = mensagem
        self.excecao = excecao


class __ModMongo(metaclass=SingletonMeta):
    def __init__(self, host: str, porta: int, database: str, collection: str, usuario: str, senha: str):
        try:
            self.__CLIENTE = MongoClient(f'mongodb://{usuario}:{senha}@{host}:{porta}/{database}')
            self.__DATABASE = self.__CLIENTE[database]
            self.__COLLECTION = self.__DATABASE[collection]
        except ConnectionFailure as e:
            logger.critical(e)
            raise PacotaoMongoException(404, f'Não foi possível conectar ao MongoDB: {host}:{porta}')

    def exec_find_one(self, filtro: dict) -> dict:
        return self.__COLLECTION.find_one(filter=filtro)

    def exec_insert_one(self, documento: dict) -> str:
        _ = self.__COLLECTION.insert_one(document=documento)
        return _.inserted_id

    def exec_insert_many(self, documentos: list) -> list:
        _ = self.__COLLECTION.insert_many(documents=documentos)
        return _.insert_ids

    def exec_delete_one(self, filtro: dict) -> int:
        _ = self.__COLLECTION.delete_one(filter=filtro)
        return _.deleted_count

    def exec_find(self, filtro: dict, projecao: list) -> tuple:
        _ = self.__COLLECTION.find(filter=filtro, projection=projecao)
        return tuple([x for  x in _])


class __ModMongoSingleton(__ModMongo, metaclass=SingletonMeta):
    pass


class MultiDatabaseMongo(__ModMongo):
    """
    Classe para modelar conexão e interação com banco de dados MongoDB.
    """
    pass


class DatabaseMongo(__ModMongoSingleton):
    """
    Classe (Singleton) para modelar conexão e interação com banco de dados MongoDB.
    """
    def __init__(self, host: str, porta: str, database: str, collection: str, usuario: str, senha: str):
        super().__init__(host=host, porta=porta, database=database, collection=collection, usuario=usuario, senha=senha)
