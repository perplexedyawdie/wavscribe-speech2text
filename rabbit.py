import pika, sys, os, json
from transformers import pipeline
from mysql.connector import connect, Error
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

def main():
    credentials = pika.PlainCredentials(os.getenv('RABBIT_USER'), os.getenv('RABBIT_PASSWORD'))
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',5672,'/',credentials))
    channel = connection.channel()

    # save data to db
    def saveData(id, transcriptionData):
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        print(transcriptionData)
        saveScribeQuery = 'CALL save_scribe_data(%s, %s, %s)'
        try:
            with connect(
                host=os.getenv('DB_HOST'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB')
            ) as connection:
                print(connection)
                with connection.cursor() as cursor:
                    cursor.execute(saveScribeQuery, (transcriptionData, id, dt_string))
                    connection.commit()
        except Error as e:
            print(e)
    # handle speech recognition
    def transcribe(filePath):
        try:
            whisper = pipeline('automatic-speech-recognition', model='openai/whisper-base', chunk_length_s=60)
            # return whisper(filePath, return_timestamps=True)
            return whisper(filePath)
        except Exception as e:
            print(e)
            return None

    def callback(ch, method, properties, body):
       print(" [x] Received %r" % body.decode())
       data = json.loads(body.decode())
       try:
           transcription = f'{transcribe(data["url"])["text"]}'
           if transcription is not None:
            saveData(data['id'], transcription)
       except Exception as e:
           print(e)
           pass

    channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
