from confluent_kafka import Consumer, KafkaError
import time
import random
from app.database import SessionLocal
from app import models
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka Consumer
kafka_config = {
    'bootstrap.servers': 'kafka:9092',  # Адрес Kafka
    'group.id': 'task-group',  # ID группы потребителей
    'auto.offset.reset': 'earliest'  # Начинать с самого раннего сообщения
}

consumer = Consumer(kafka_config)
consumer.subscribe(['tasks'])  # Подписываемся на топик "tasks"

def process_task(task_id: str):
    """
    Обрабатывает задачу.
    """
    db = SessionLocal()
    task = db.query(models.Task).filter(models.Task.id == task_id).first()
    if task:
        try:
            # Изменяем статус задачи на "В процессе работы"
            task.status = "В процессе работы"
            db.commit()

            # Эмуляция выполнения задачи (случайная задержка 5-10 секунд)
            time.sleep(random.randint(5, 10))

            # Изменяем статус задачи на "Завершено успешно" или "Ошибка"
            if random.random() < 0.8:  # Вероятность успешного завершения
                task.status = "Завершено успешно"
            else:
                task.status = "Ошибка"
            db.commit()
        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}")
        finally:
            db.close()

def main():
    """
    Основной цикл работы worker.
    """
    try:
        while True:
            msg = consumer.poll(1.0)  # Ожидаем сообщение из Kafka
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(msg.error())
                    break

            # Получаем задачу из Kafka
            task_id = msg.value().decode('utf-8')
            logger.info(f"Received task {task_id}")
            process_task(task_id)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
