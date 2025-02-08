import json
import pymysql
from kafka import KafkaConsumer
import time
import threading
from threading import Event
from datetime import datetime
import pytz

KAFKA_BROKER = '192.168.0.163:9092'
KAFKA_TOPIC_log = "logs"

stop_event = Event()

# 타임존 설정 (Kafka에서 받은 UTC timestamp를 KST로 변환)
tz = pytz.timezone('Asia/Seoul')

def get_db_connection():
    """SQL 설정"""
    return pymysql.connect(
        host='192.168.0.163',
        user='manufacture_user',
        password='andong1234',
        db='manufacture',
        charset='utf8mb4'
    )

def save_to_db_log(data):
    """Kafka 데이터를 log_info 테이블에 저장 (log_time = 분석된 시각 그대로)"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Kafka에서 받은 timestamp를 한국 시간(KST)으로 변환
            if 'timestamp' in data and data['timestamp']:
                try:
                    """ kafka timestamp 문자열을 파싱하여 datatime 객체로 변환 -> UTC 시간을 한국 표준시(KST)로 변경 -> 변환된 시간을 YYY-MM-DD- HH:MM:SS 포맷의 문자열로 저장"""
                    # ISO 8601 형식 파싱 (UTC 기준으로 KST 변환)
                    original_timestamp = datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    # UTC 타임존 설정 및 변환
                    original_timestamp = original_timestamp.replace(tzinfo=pytz.UTC).astimezone(tz)
                    # 시간 문자열 포맷 지정
                    log_time = original_timestamp.strftime('%Y-%m-%d %H:%M:%S')  # YYYY-MM-DD HH:MM:SS
                except ValueError:
                    print(f"Invalid timestamp format: {data['timestamp']}")
                    return
            else:
                log_time = None

            query = "INSERT INTO log_info (plt_number, log_time, ctgr) VALUES (%s, %s, %s)"
            cursor.execute(query, (data['pltNumber'], log_time, data['message']))
            conn.commit()

        print(f"log_info 저장 완료: {data['pltNumber']} | log_time: {log_time}")
    
    except Exception as e:
        print(f"log_info 저장 오류: {e}")
    
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()


def consume_messages():
    """Kafka 메시지 소비 및 DB 저장"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_log,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            enable_auto_commit=False,
            group_id='log_db_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print("Kafka Consumer started...")
        for message in consumer:
            if stop_event.is_set():
                break
            
            data = message.value
            save_to_db_log(data)
            
    except Exception as e:
        print(f"Consumer error: {e}. Reconnecting in 5 seconds...")
        time.sleep(5)  # 재연결 대기

def start_consumer():
    """Kafka Consumer 스레드 시작"""
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    print("Kafka Consumer thread running...")

if __name__ == "__main__":
    try:
        start_consumer()
        while not stop_event.is_set():
            time.sleep(1)  # 무한 대기를 적절한 sleep으로 유지
    except KeyboardInterrupt:
        print("Shutting down consumer...")
        stop_event.set()
        time.sleep(1)  # 종료 신호가 처리되도록 대기
