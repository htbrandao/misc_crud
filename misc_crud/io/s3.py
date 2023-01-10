import io
import boto3
import functools

from botocore.config import Config
from botocore.exceptions import ClientError

from .utils.meta import SingletonMeta
from .helpers import bytes_para_base64str, base64str_para_bytes

ERRMSG_LEITURA = 'Não foi possível ler o objeto'
ERRMSG_ESCRITA = 'Não foi possível gravar o objeto'


class PacotaoS3Exception(Exception):
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


class ObjetoNaoEncontradoException(PacotaoS3Exception):
    """
    Exceção para não encontrar objeto no bucket S3.

    :param status_code int: status_code
    :param mensagem str: mensagem de exceção boto3
    :return: ObjetoNaoEncontradoException
    :rtype: ObjetoNaoEncontradoException
    """
    pass


def objeto_existe(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """
        Decorator para verificar se objeto existe no bucket.

        :param list args: lista de args
        :param dict kwargs: dict de kwargs
        :return: funcção decorada
        :rtype: func
        :raises: ObjetoNaoEncontradoException
        """
        if kwargs:
            s3, path_obj = args[0], kwargs.get('path_objeto', '')
        elif args:
            s3, path_obj = args[0], args[1]
        else:
            raise PacotaoS3Exception('Algo errado no decorator')
        if path_obj in s3.listar_objetos().get('objetos'):
            return func(*args, **kwargs)
        else:
            raise ObjetoNaoEncontradoException(mensagem=f'Objeto {path_obj} não encontrado')
    return wrapper


class __ModS3():
    """
    Classe para modelar conexão e interação com solução de armazenamento S3.
    """

    def __init__(self, access_key: str, secret_key: str, endpoint: str, region: str, volume: str, **kwargs):
        self.__PARAMS = {
            'service_name': 's3',
            'use_ssl': kwargs.get('use_ssl', False),
            'aws_access_key_id': kwargs.get('access_key', access_key),
            'aws_secret_access_key': kwargs.get('secret_key', secret_key),
            'endpoint_url': kwargs.get('endpoint', endpoint),
            'config': Config(region_name=kwargs.get('region', region),
                             s3={'addressing_style': 'path'},
                             retries={
                                'max_attempts': kwargs.get('max_attempts', 1),
                                'mode': 'standard'})}
        self.__CLIENT = boto3.client(**self.__PARAMS)
        self.__RESOURCE = boto3.resource(**self.__PARAMS)
        self.__VOLUME = volume
        self.__BUCKET = self.__RESOURCE.Bucket(self.__VOLUME)

    @property
    def get_client(self):
        return self.__CLIENT

    @property
    def get_resource(self):
        return self.__RESOURCE

    @property
    def get_volume(self):
        return self.__VOLUME

    @property
    def get_bucket(self):
        return self.__BUCKET

    def listar_objetos(self):
        """
        Lista objetos no bucket.

        :return: dict {objetos, total}
        :rtype: dict
        """
        try:
            objetos = [o.key for o in self.__BUCKET.objects.all()]
            return {'objetos': objetos, 'total': len(objetos)}
        except ClientError as e:
            raise PacotaoS3Exception('Não foi possível remover o objeto', e)

    def pesquisar_objeto(self, path_objeto: str) -> dict:
        """
        Efetua uma pesquisa dentre os objetos do bucket.

        :param str path_objeto: path_objeto
        :return: dict {objetos, total}
        :rtype: dict
        """
        objetos = [o for o in self.listar_objetos().get('objetos') if path_objeto in o]
        return {'objetos': objetos, 'total': len(objetos)}

    @objeto_existe
    def apagar_objeto(self, path_objeto: str) -> dict:
        """
        Apaga um objeto do bucket.

        :param str path_objeto: path_objeto
        :return: dict {objeto, status}
        :rtype: dict
        :raises: ObjetoNaoEncontradoException
        """
        try:
            retorno = self.__CLIENT.delete_object(Bucket=self.__VOLUME, Key=path_objeto)
            return {'objeto': path_objeto, 'status': retorno.get('ResponseMetadata').get('HTTPStatusCode')}
        except ClientError as e:
            raise PacotaoS3Exception('Não foi possível remover o objeto', e)

    @objeto_existe
    def download_bytes(self, path_objeto: str) -> dict:
        """
        Retorna os bytes de um objeto.

        :param str path_objeto: path_objeto
        :return: dict {objeto, bytes}
        :rtype: dict
        :raises: ObjetoNaoEncontradoException
        """
        buffer = io.BytesIO()
        try:
            self.__CLIENT.download_fileobj(Bucket=self.__VOLUME, Key=path_objeto, Fileobj=buffer)
            buffer.flush()
            buffer.seek(0)
            return {'objeto': path_objeto, 'bytes': buffer.read()}
        except ClientError as e:
            raise PacotaoS3Exception(ERRMSG_LEITURA, e)

    @objeto_existe
    def download_b64str(self, path_objeto: str) -> dict:
        """
        Retona os bytes de um objeto do bucket como str base 64.

        :param str path_objeto: path_objeto
        :return: dict {objeto, b64str}
        :rtype: dict
        :raises: ObjetoNaoEncontradoException
        """
        try:
            objeto = self.__RESOURCE.Object(bucket_name=self.__VOLUME, key=path_objeto)
            bytes_objeto = objeto.get()['Body'].read()
            bytes_objeto = bytes_para_base64str(bytes_objeto)
            return {'objeto': path_objeto, 'b64str': bytes_objeto}
        except ClientError as e:
            raise PacotaoS3Exception(ERRMSG_LEITURA, e)

    @objeto_existe
    def download_arquivo(self, path_objeto: str, path_arquivo: str) -> dict:
        """
        Retona os bytes de um objeto do bucket para arquivo no file system.

        :param str path_objeto: path_objeto
        :param str path_arquivo: path_arquivo
        :param Client client: client s3
        :return: dict {objeto, arquivo}
        :rtype: dict
        :raises: ObjetoNaoEncontradoException
        """
        try:
            self.__CLIENT.download_file(self.__VOLUME, path_objeto, path_arquivo)
            return {'objeto': path_objeto, 'arquivo': path_arquivo}
        except ClientError as e:
            raise PacotaoS3Exception('Não foi possível gravar o arquivo localmente', e)

    def upload_arquivo(self, path_objeto: str, arquivo) -> dict:
        """
        Efetua o upload de um arquivo para o bucket.

        :param str path_objeto: path objeto no destino
        :param fastapi.File arquivo: arquivo
        :param boto3.Client client: client s3
        :return: dict {objeto, tamanho}, tamanho em bytes
        :rtype: dict
        :raises: NiaBBS3Exception
        """
        arquivo_bytes = arquivo.file._file
        tamanho_objeto = arquivo.file._max_size
        try:
            self.__CLIENT.upload_fileobj(arquivo_bytes, self.__VOLUME, path_objeto)
            return {'objeto': path_objeto, 'tamanho': tamanho_objeto}
        except ClientError as e:
            raise PacotaoS3Exception('Não foi possível efetuar o upload', e)

    def grava_bytes(self, path_objeto: str, bytes_objeto: bytes) -> dict:
        """
        Realiza a escrita de bytes no bucket.

        :param str path_objeto: path objeto no destino
        :param bytes bytes_objeto: bytes do objeto
        :return: dict {objeto, tamanho}, tamanho em bytes
        :rtype: dict
        :raises: NiaBBS3Exception
        """
        try:
            self.__CLIENT.upload_fileobj(io.BytesIO(bytes_objeto), self.__VOLUME, path_objeto)
            return {'objeto': path_objeto, 'tamanho': len(bytes_objeto)}
        except ClientError as e:
            raise PacotaoS3Exception(ERRMSG_ESCRITA, e)

    def grava_b64str(self, path_objeto: str, base64str_objeto: str) -> dict:
        """
        Converte str b64 realiza a escrita de bytes no bucket.

        :param str path_objeto: path_objeto
        :param str base64str_objeto: str base64 do objeto
        :return: dict {objeto, tamanho}, tamanho em bytes
        :rtype: dict
        :raises: NiaBBS3Exception
        """
        bytes_objeto = base64str_para_bytes(base64str_objeto)
        try:
            self.__CLIENT.upload_fileobj(io.BytesIO(bytes_objeto), self.__VOLUME, path_objeto)
            return {'objeto': path_objeto, 'tamanho': len(bytes_objeto)}
        except ClientError as e:
            raise PacotaoS3Exception(ERRMSG_ESCRITA, e)

    @staticmethod
    def __stream_chunks(body_bytes: bytes, chunk_size: int) -> bytes:
        """
        Iterator que realiza a leitura de um bytearray.

        :param bytes body_bytes: objeto byte-like com .read()
        :param int chunk_size: quantidade de bytes a serem lidos por iteração
        :returns: iterator
        :rtype: iterator(bytes)
        """
        while body_bytes:
            chunk = body_bytes.read(chunk_size)
            if chunk:
                yield chunk
            else:
                break

    @objeto_existe
    def stream_objeto(self, path_objeto: str, chunk_size=1048576):
        """
        Realiza o streamming de um objeto do bucket.

        :param str path_objeto: path_objeto
        :param int chunk_size: quantidade de bytes a serem lidos por iteração, 1 Mb (1 byte * 1024 Kb * 1024) por padrão
        :return: bytes chunk do arquivo
        return StreamingResponse
        :rtype: bytes
        :raises: ObjetoNaoEncontradoException
        """
        try:
            body_bytes = self.__RESOURCE.Object(bucket_name=self.__VOLUME, key=path_objeto).get()['Body']
            return self.__stream_chunks(body_bytes=body_bytes, chunk_size=chunk_size)
        except ClientError as e:
            raise PacotaoS3Exception(ERRMSG_LEITURA, e)


class __ModS3Singleton(__ModS3, metaclass=SingletonMeta):
    pass


class MultiArmazenamentoS3(__ModS3):
    def __init__(self, access_key: str, secret_key: str, endpoint: str, region: str, volume: str):
        super().__init__(access_key=access_key, secret_key=secret_key, endpoint=endpoint, region=region, volume=volume)


class ArmazenamentoS3(__ModS3Singleton):
    def __init__(self, access_key: str, secret_key: str, endpoint: str, region: str, volume: str):
        super().__init__(access_key=access_key, secret_key=secret_key, endpoint=endpoint, region=region, volume=volume)
