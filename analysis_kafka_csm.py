import json
import pymysql
from kafka import KafkaConsumer
import time
import threading
from threading import Event
from datetime import datetime
import pytz
import base64

KAFKA_BROKER = '192.168.0.163:9092'
KAFKA_TOPIC_IMAGES = 'kafka-ig'

stop_event = Event()

# 타임존 설정 (Kafka에서 받은 UTC timestamp를 KST로 변환)
tz = pytz.timezone('Asia/Seoul')

def get_db_connection():
    """SQL 설정"""
    return pymysql.connect(
        host='192.168.0.163',
        user='analysis_user',
        password='andong1234',
        db='analysis',
        charset='utf8mb4'
    )

def decode_image(encoded_image):
    """Base64로 인코딩된 이미지 데이터를 디코딩하여 원래 이미지 바이트로 복원"""
    decoded_image = base64.b64decode(encoded_image.encode('utf-8'))
    return decoded_image

def save_to_db_analysis(data):
    """Kafka 데이터를 analysis_info 테이블에 저장 (log_time = 분석된 시각 그대로)"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Kafka에서 받은 timestamp를 한국 시간(KST)으로 변환
            if 'timestamp' in data and data['timestamp']:
                try:
                    # ISO 8601 형식 파싱 (UTC 기준으로 KST 변환)
                    original_timestamp = datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    original_timestamp = original_timestamp.replace(tzinfo=pytz.UTC).astimezone(tz)
                    log_time = original_timestamp.strftime('%Y-%m-%d %H:%M:%S')  # YYYY-MM-DD HH:MM:SS
                except ValueError:
                    print(f"Invalid timestamp format: {data['timestamp']}")
                    return
            else:
                log_time = None

            # 디코드된 이미지 데이터를 BLOB으로 저장하기 위해 변환
            image_data = decode_image(data['image'])
           
           
            query = "INSERT INTO plt_image_analysis (plt_number, img, log_time, ctgr) VALUES (%s, %s, %s, %s)"
            cursor.execute(query, (data['pltNumber'],  pymysql.Binary(image_data), log_time, data['message']))
            conn.commit()

        print(f"plt_image_analysis 저장 완료: {data['pltNumber']} | log_time: {log_time}")
    
    except Exception as e:
        print(f"plt_image_analysis 저장 오류: {e}")
    
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

    
def consume_messages():
    """Kafka 메시지 소비 및 DB 저장"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_IMAGES,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='analysis_db_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print("Kafka Consumer started...")
        for message in consumer:
            if stop_event.is_set():
                break
            
            data = message.value 
            save_to_db_analysis(data)  # analysis 테이블 저장 (주석 처리)
            
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
