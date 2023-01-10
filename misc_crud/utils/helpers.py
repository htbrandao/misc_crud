from hashlib import md5  # nosec
from datetime import datetime
from base64 import b64encode, b64decode


def parse_timestamp(timestamp: float, formato='%Y-%m-%dT%H:%M:%S'):
    """
    Helper. Faz o parse do timestamp para o formato desejado.

    :param float timestamp: epoch
    :param str formato: formato para saída do timestamp
    :return: timestamp formatado
    :rtype: str
    """
    return datetime.fromtimestamp(timestamp).strftime(formato)


def bytes_para_base64str(arquivo: bytes, enc='utf-8'):
    """
    Helper. Converte um bytearray para uma string b64.

    :param bytes arquivo: bytearray
    :param str enc: encoding
    :return: str
    :rtype: str
    """
    return b64encode(arquivo).decode(enc)


def base64str_para_bytes(arquivo: str, enc='utf-8'):
    """
    Helper. Converte uma string b64 para bytearray.

    :param str arquivo: base64 string
    :param str enc: encoding
    :return: bytes do arquivo
    :rtype: bytes
    """
    return b64decode(arquivo.encode(enc), validate=True)


def calcula_md5(base64str: str, enc='utf-8'):
    """
    Helper. Calcula a soma md5.

    :param str base64str: base64 string
    :param str enc: encoding
    :return: md5
    :rtype: str
    """
    return md5(base64str.encode(enc)).hexdigest()  # nosec
