
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import pika
import json
import logging
import settings 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()


class Mensagem(BaseModel):
    nome: str
    texto: str

def get_rabbitmq_connection():
    """Cria e retorna uma conexão com o RabbitMQ."""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=settings.RABBITMQ_HOST)
        )
        logger.info(f"Conexão com RabbitMQ em '{settings.RABBITMQ_HOST}' estabelecida.")
        return connection
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Erro ao conectar com RabbitMQ: {e}")
        return None

@app.post("/enviar")
def enviar_mensagem(mensagem: Mensagem):
    """
    Endpoint para receber uma mensagem JSON e enviá-la para a fila do RabbitMQ.
    """
    connection = get_rabbitmq_connection()
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível conectar ao RabbitMQ."
        )

    try:
        channel = connection.channel()

        
        channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)

        
        message_body = mensagem.json()

       
        channel.basic_publish(
            exchange='',  
            routing_key=settings.QUEUE_NAME, 
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE 
            )
        )
        
        logger.info(f" [x] Mensagem enviada para a fila '{settings.QUEUE_NAME}': {message_body}")
        return {"status": "mensagem enviada", "dados": mensagem}

    except Exception as e:
        logger.error(f"Erro ao enviar mensagem: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Erro ao processar a mensagem: {e}"
        )
    finally:
        
        if connection and connection.is_open:
            connection.close()
            logger.info("Conexão com RabbitMQ fechada.")

@app.get("/")
def read_root():
    return {"status": "API FastAPI rodando. Use o endpoint /enviar para testar."}