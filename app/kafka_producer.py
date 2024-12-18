from confluent_kafka import Producer
import json

# Конфигурация Kafka Producer
kafka_config = {
    'bootstrap.servers': 'kafka:9092',  # Адрес Kafka
}

producer = Producer(kafka_config)

def send_task_to_kafka(task_id: str):
    """
    Отправляет задачу в Kafka.
    """
    topic = "tasks"
    try:
        # Отправляем задачу в Kafka
        producer.produce(topic, key=task_id, value=task_id)
        producer.flush()  # Ожидаем доставки сообщения
        print(f"Task {task_id} sent to Kafka topic {topic}")
    except Exception as e:
        print(f"Failed to send task to Kafka: {e}")
