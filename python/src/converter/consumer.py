import pike, sys, os, time
from pymongo import MongoClient
import gridfs
from convert import to_mp3

def main():
    client = MongoClient('host.minikube.internal', 20717)
    db_videos = client.videos
    db_mp3s = client.mp3s

    # gridfs
    fs_videos = gridfs.GridFS(db_videos)
    fs_mp3s = gridfs.GridFS(db_mp3s)

    # rabbitmq connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq')
    )
    channel = connection.channel()

    def callback(channel, method, properties, body):
        err = to_mp3.start(body, fs_videos, fs_mp3s, channel)
        if err:
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else: 
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
    
    channel.basic_consume(
            queue=os.environ.get("VIDEO_QUEUE"),
            on_message_callback = callback
    )
    
    print("Waiting for messages. CTRL+C to Exit")

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except:
            os._exit(0)