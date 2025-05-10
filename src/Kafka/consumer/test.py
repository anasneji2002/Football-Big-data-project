from Kafka.consumer.models.LiveMatchConsumer import LiveMatchConsumer
import threading


liveMatchConsumer = LiveMatchConsumer()
liveMatchConsumerThread = threading.Thread(target=liveMatchConsumer.main())
liveMatchConsumerThread.start()
liveMatchConsumerThread.join()
