from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app import schemas, models
from app.database import SessionLocal
from app.kafka_producer import send_task_to_kafka
import uuid

router = APIRouter()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/", response_model=schemas.Task)
def create_task(task: schemas.TaskCreate, db: Session = Depends(get_db)):
    db_task = models.Task(**task.dict())
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    # Отправляем задачу в Kafka
    send_task_to_kafka(str(db_task.id))
    return db_task

@router.get("/{task_id}", response_model=schemas.Task)
def read_task(task_id: uuid.UUID, db: Session = Depends(get_db)):
    db_task = db.query(models.Task).filter(models.Task.id == task_id).first()
    if db_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return db_task

@router.get("/", response_model=list[schemas.Task])
def read_tasks(status: str = None, db: Session = Depends(get_db)):
    query = db.query(models.Task)
    if status:
        query = query.filter(models.Task.status == status)
    return query.all()
