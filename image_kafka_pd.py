from kafka import KafkaProducer
import pymysql
import base64
import time
import json

# 전달하고자 하는  카프카 브로커의 주소 리스트
producer = KafkaProducer(
    bootstrap_servers=['192.168.0.163:9092'],
    compression_type='gzip',
    value_serializer = lambda x : json.dumps(x).encode('utf-8'))


# MySQL 연결 설정
def get_db_connection():
    """DB 연결 객체 반환"""
    return pymysql.connect(
        host='192.168.0.163',
        passwd='andong1234',
        user='manufacture_user',
        db='manufacture',
        charset='utf8mb4'
    )

# 이미지 인코딩
def encode_image(image_data):
    encoded_image = base64.b64encode(image_data).decode('utf-8')
    return encoded_image


# sql -> kafka producer
def fetch_and_send_images():
    """10초마다 새로운 데이터 하나씩 가져와 Kafka로 전송"""
    try:
        con = get_db_connection()
        last_img_id = None  # 마지막 처리된 이미지 시간
        while True:
            with con.cursor() as cur:
                # 최신 데이터 중 이전 데이터 이후 하나 가져오기
                if last_img_id:
                    query = "SELECT img, plt_number FROM plt_img WHERE plt_number > %s ORDER BY plt_number ASC LIMIT 1"
                    cur.execute(query, (last_img_id,))
                else:
                    query = "SELECT img, plt_number FROM plt_img ORDER BY plt_number ASC LIMIT 1"
                    cur.execute(query)

                data = cur.fetchone()
                if data:
                    img, plt_number = data
                    if img:  # 이미지 데이터가 있는 경우만 처리
                        encoded_image = encode_image(img)
                        payload = {"plt_number": plt_number, "encoded_image": encoded_image}
                        producer.send('aa_topic', value=payload)  # JSON 객체 전송
                        print(f"Image sent id: {plt_number}")
                        last_img_id = plt_number  # 처리된 이미지 아이디 업데이트
                else:
                    print("No new images found.")

            producer.flush()
            time.sleep(10)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()
        con.close()

if __name__ == "__main__":
    fetch_and_send_images()