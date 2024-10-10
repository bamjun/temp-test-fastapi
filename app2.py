from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import requests
import psycopg2  # PostgreSQL 연결
import os
import configparser
from contextlib import asynccontextmanager
import pytz
kst = pytz.timezone('Asia/Seoul')



config = configparser.ConfigParser()
config.read("config.ini")



@asynccontextmanager
async def lifespan(app: FastAPI):
    print("애플리케이션이 시작됩니다.")
    yield
    scheduler.shutdown()
    print("애플리케이션이 종료됩니다.")


app = FastAPI(lifespan=lifespan)

# 스케줄러 초기화
scheduler = BackgroundScheduler(timezone=kst)

# # PostgreSQL 데이터베이스 연결 설정
# def get_db_connection():
#     return psycopg2.connect(
#         host=config['aws']['DB_HOST'],
#         database=config['aws']['DB_NAME'],
#         user=config['aws']['DB_USER'],
#         password=config['aws']['DB_PASSWORD'],
#         port=config['aws']['DB_PORT']
#     )

# # DB에서 데이터 가져오기
# def get_data_from_db():
#     conn = get_db_connection()
#     cursor = conn.cursor()
    
#     # 메시지 전송 시간을 확인하는 쿼리 (테이블 구조에 맞게 수정)
#     cursor.execute("SELECT content FROM public.a_board_comment ORDER BY id ASC LIMIT 1;")
#     data = cursor.fetchall()
    
#     conn.close()
#     return data
def get_data_from_db():
    return "**call get_data_from_db()"

# 메시지 전송 함수
def send_message():
    data = get_data_from_db()
    print(data)
    return
    for row in data:
        message_id, message, send_time = row
        
        # 현재 시간이 메시지 전송 시간과 같거나 늦으면 전송
        if datetime.now() >= send_time:
            try:
                response = requests.post("https://meme.com/sand", json={"message": message})
                print(f"Message {message_id} sent: {response.status_code}")
            except Exception as e:
                print(f"Error sending message {message_id}: {e}")

# 주기적으로 작업을 실행하는 스케줄러 등록 (1분마다 확인)
scheduler.add_job(send_message, "interval", seconds=60)
scheduler.add_job(
    send_message, 
    CronTrigger(hour='*', minute='0', second='0', timezone=kst),
    id="hourly_send_message_job",
    name="Send Messages Every Hour on the Hour (KST)"
)
scheduler.start()

@app.get("/")
async def root():
    return {"message": "Server is running 002"}

@app.get("/time")
async def get_first():
    # conn = get_db_connection()
    # cursor = conn.cursor()
    
    # # 메시지 전송 시간을 확인하는 쿼리 (테이블 구조에 맞게 수정)
    # cursor.execute("SELECT content FROM public.a_board_comment ORDER BY id ASC LIMIT 1;")
    # data = cursor.fetchall()
    
    # conn.close()
    return {"message": "get time data"}


@app.get("/schedule")
async def get_scheduled_jobs():
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "trigger": str(job.trigger),
            "next_run_time": str(job.next_run_time)
        })
    return {"scheduled_jobs": jobs}


from datetime import timedelta
from apscheduler.triggers.date import DateTrigger
def print_hello():
    print("하이용")

@app.get("/add")
async def add_scheduled_job():
    # 현재 시간에서 5분 후의 시간 계산
    run_time = datetime.now(kst) + timedelta(minutes=5)
    
    # 스케줄러에 작업 추가
    job = scheduler.add_job(
        print_hello,
        trigger=DateTrigger(run_date=run_time, timezone=kst),
        id=f"hello_job_{run_time.timestamp()}",
        name=f"Print Hello at {run_time}"
    )
    
    return {
        "message": "Job added successfully",
        "job_id": job.id,
        "scheduled_time": str(run_time)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
