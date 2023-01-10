import socket

from pacotao import logger

from confluent_kafka import Producer, Consumer


class __ModKafka:
    """
    Classe para modelar conexão e interação com Apache Kafka.

        https://docs.confluent.io/kafka-clients/python/current/overview.html#python-demo-code
    """

    def __init__(self, topico: str, group_id: str, bootstrap_servers: str, porta: int):

        self.__BOOTSTRAP_SERVERS = ', '.join([f'{server}:{porta}' for server in bootstrap_servers])
        self.__TOPICO = topico
        self.__CONSUMER = Consumer({'bootstrap.servers': self.__BOOTSTRAP_SERVERS, 'group_id': group_id})
        self.__PRODUCER = Producer({'bootstrap.servers': self.__BOOTSTRAP_SERVERS, 'client_id': socket.gethostname()})


    @property
    def get_producer(self):
        return self.__PRODUCER

    @property
    def get_consumer(self):
        return self.__CONSUMER

    @staticmethod
    def on_send_success(record_metadata):
        logger.debug(record_metadata.topic)
        logger.debug(record_metadata.partition)
        logger.debug(record_metadata.offset)

    @staticmethod
    def on_send_error(excp):
        # handle exception
        logger.error('I am an errback: {excp}')

    def produzir_async(self, mensagem: str, enc='utf-8'):
        self.__PRODUCER.send(topic=self.__TOPICO, value=mensagem.encode(enc))\
                       .add_callback(self.on_send_success)\
                       .add_errback(self.on_send_error)

    def produzir_sync(self, mensagem: str, enc='utf-8'):
        self.produzir_async(mensagem=mensagem)
        self.get_producer.flush()

    def consumir(self, tamanho_poll=1, enc='utf-8'):
        """
        https://docs.confluent.io/kafka-clients/python/current/overview.html#python-code-examples
        """
        self.__CONSUMER.subscribe(self.__TOPICO)
        logger.debug('Consumindo mensagens...')
        for mensagem in self.__CONSUMER.poll(max_records=tamanho_poll):
            _ = [mensagem.topic,
                 mensagem.partition,
                 mensagem.offset,
                 mensagem.key.decode(enc),
                 mensagem.value.decode(enc)]
            logger.debug(_)
        self.__CONSUMER.close()
        return _

    def __del__(self):
        logger.debug('... apagando objeto ...')

    def __exit__(self, type, value, traceback):
        logger.debug('... saindo ...')
        self.__del__()


class MensageriaKafka(__ModKafka):
    def __init__(self,
                 topico: str,
                 group_id: str,
                 bootstrap_servers: str,
                 porta: str):
        super().__init__(topico=topico,
                        group_id=group_id,
                        bootstrap_servers=bootstrap_servers,
                        porta=porta)
