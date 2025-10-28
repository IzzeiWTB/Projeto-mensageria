
import pika
import time
import logging
import settings 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_rabbitmq(host, retries=10, delay=5):
    """Tenta conectar ao RabbitMQ com retentativas."""
    for i in range(retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            logger.info("Consumidor: Conexão com RabbitMQ estabelecida.")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Tentativa {i+1}/{retries} falhou. Conectando ao RabbitMQ em '{host}'... Erro: {e}")
            time.sleep(delay)
    logger.error("Não foi possível conectar ao RabbitMQ após várias tentativas.")
    return None

def callback(ch, method, properties, body):
    """
    Função callback para processar mensagens recebidas.
    Esta função será chamada toda vez que uma mensagem for consumida da fila.
    """
    try:
       
        mensagem_str = body.decode('utf-8')
        
       
        logger.info("\n==================== MENSAGEM RECEBIDA ====================")
        logger.info(f"\n {mensagem_str}")
        logger.info("\n===========================================================\n")

        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {e}")
        
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    """Função principal que inicia o consumidor."""
    
    
    connection = connect_to_rabbitmq(settings.RABBITMQ_HOST)
    if not connection:
        return

    channel = connection.channel()


    channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)

    
    channel.basic_qos(prefetch_count=1)

   
    channel.basic_consume(
        queue=settings.QUEUE_NAME,
        on_message_callback=callback
        
    )

    try:
        logger.info(' [*] Aguardando mensagens. Para sair, pressione CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Consumo interrompido pelo usuário.")
        channel.stop_consuming()
    except pika.exceptions.ConnectionClosedByBroker:
        logger.error("Conexão fechada pelo broker. Tentando reconectar...")
        main() 
    except Exception as e:
        logger.error(f"Erro inesperado no consumidor: {e}")
    finally:
        if connection.is_open:
            connection.close()
        logger.info("Conexão com RabbitMQ fechada.")

if __name__ == '__main__':
    main()