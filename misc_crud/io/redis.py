import functools

from datetime import datetime

from redis import StrictRedis
from redis.exceptions import ConnectionError

import logging as logger


class PacotaoRedisException(Exception):
    """
    Exceção base para lidar com S3.

    :param status_code int: status_code
    :param mensagem str: mensagem de exceção
    :return: S3BaseException
    :rtype: S3BaseException
    """
    def __init__(self, mensagem: str, excecao=None):
        self.mensagem = mensagem
        self.excecao = excecao


# TODO: implementar?
def retry_read(func):
    """
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)


class __ModRedis:
    """
    Classe para modelar conexão e interação com cache Redis.

    Por padrão, a lib redis já utiliza *connection pools*, portanto podemos utilizar o conector de alto nível diretamente.

    A lib também já realiza a gestão das conexões.
    """

    def __init__(self, master: str, slaves: list, porta: int, senha: str):

        self.__MASTER = StrictRedis(host=master, port=porta, password=senha, decode_responses=True, encoding='utf-8')
        self.__SLAVES = [StrictRedis(host=s, port=porta, password=senha, decode_responses=True, encoding='utf-8') for s in slaves]

        try:
            for h in [self.__MASTER, *self.__SLAVES]:
                h.ping()
        except ConnectionError:
            msg = f'Não foi possível conectar ao Redis: {h.get_connection_kwargs()["hosts"]}'
            logger.error(msg)
            raise PacotaoRedisException('Não foi possível conectar ao Redis')

    def listar_regs(self, i=0) -> tuple:
        """
        Recupera todos os registros (chaves) do cache.

        :param int i: índice da lista hosts slaves
        :returns: todos os registros
        :rtype: tuple
        """
        return tuple(self.__SLAVES[i].keys())

    def listar_key_val(self, i=0) -> tuple:
        """
        Recupera todos os registros (chaves, valor) do cache.

        :param int i: índice da lista hosts slaves
        :returns: todos os registros
        :rtype: tuple
        """
        k = self.__SLAVES[i].keys()
        return tuple([*zip(k, [self.get_reg(_) for _ in k])])

    def set_reg(self, identificador: str) -> bool:
        """
        Grava um novo registro no cache.

        :param str identificador: chave do cache
        :returns: True/False de acordo com o resultado da escrita
        :rtype: bool
        """
        _ = self.__MASTER.set(name=identificador, value=datetime.now().strftime('%s'), nx=True)
        return True if _ else False

    def get_mais_antigo(self, i=0) -> str:
        """
        Recupera o i-ésimo registro mais antigo no cache, uma vez que o registro (chave) é gravado com um valor de timestamp.

        :returns: chave mais antiga
        :rtype: str
        """
        _ = sorted(self.listar_key_val(), key=lambda x: x[-1], reverse=False)
        return _[i][0] if _ else ''

    def get_reg(self, identificador: str, i=0) -> str:
        """
        Recupera o valor a partir de um identificador.

        :param str identificador: chave do cache
        :param int i: índice da lista hosts slaves
        :returns: valor da chave no cache
        :rtype: str
        """
        try:
            return self.__SLAVES[i].get(name=identificador)
        except AttributeError as e:
            logger.error(e)
            return ''

    def delete_reg(self, identificador: str) -> int:
        """
        Remove um registro do cache a partir do identificador.

        :param str identificador: chave do cache
        :returns: 1/0 de acordo com o resultado da remoção
        :rtype: int
        """
        _ = self.__MASTER.delete(identificador)
        return 1 if _ else 0


class CacheRedis(__ModRedis):
    def __init__(self,
                 master: str,
                 slaves: list,
                 porta: str,
                 senha: str):
        super().__init__(master=master, slaves=slaves, porta=porta, senha=senha)
