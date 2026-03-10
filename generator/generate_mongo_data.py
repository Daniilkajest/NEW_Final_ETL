import logging
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker

# Настройка логирования для отладки
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

# Конфиг подключения к Mongo (внутри Docker сети)
MONGO_URI = "mongodb://admin:admin_password@localhost:27017/"
DB_NAME = "etl_source"

def get_db():
    """Инициализация подключения к БД."""
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

def generate_sessions(db, num_records=200):
    """Генерация сессий пользователей."""
    logger.info(f"Генерация {num_records} записей для UserSessions...")
    sessions = []
    
    for _ in range(num_records):
        # Генерируем даты так, чтобы они попадали в наши партиции в Postgres (Январь-Февраль 2024)
        start_date = datetime(2024, random.choice([1, 2]), random.randint(1, 28), random.randint(0, 23))
        end_date = start_date + timedelta(minutes=random.randint(5, 120))
        
        doc = {
            "session_id": f"sess_{fake.uuid4()[:8]}",
            "user_id": f"user_{random.randint(1, 100)}",
            "start_time": start_date.isoformat() + "Z",
            "end_time": end_date.isoformat() + "Z",
            "pages_visited": [fake.uri_path() for _ in range(random.randint(2, 10))],
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "actions": [random.choice(["login", "view_product", "add_to_cart", "logout"]) for _ in range(random.randint(2, 6))]
        }
        sessions.append(doc)
        
    db.UserSessions.insert_many(sessions)
    logger.info("Успешно вставлено.")

def generate_support_tickets(db, num_records=100):
    """Генерация обращений в поддержку."""
    logger.info(f"Генерация {num_records} записей для SupportTickets...")
    tickets = []
    
    for _ in range(num_records):
        created_at = datetime(2024, random.choice([1, 2]), random.randint(1, 28), random.randint(0, 15))
        updated_at = created_at + timedelta(hours=random.randint(1, 48))
        
        doc = {
            "ticket_id": f"ticket_{fake.uuid4()[:8]}",
            "user_id": f"user_{random.randint(1, 100)}",
            "status": random.choice(["open", "in_progress", "resolved", "closed"]),
            "issue_type": random.choice(["payment", "bug", "account_access", "delivery"]),
            "messages": [
                {"sender": "user", "message": fake.sentence(), "timestamp": created_at.isoformat() + "Z"}
            ],
            "created_at": created_at.isoformat() + "Z",
            "updated_at": updated_at.isoformat() + "Z"
        }
        tickets.append(doc)
        
    db.SupportTickets.insert_many(tickets)
    logger.info("Успешно вставлено.")

if __name__ == "__main__":
    db_conn = get_db()
    
    # Очищаем коллекции перед новой генерацией (идемпотентность)
    db_conn.UserSessions.drop()
    db_conn.SupportTickets.drop()
    
    generate_sessions(db_conn)
    generate_support_tickets(db_conn)
    
    logger.info("Генерация данных завершена!")
