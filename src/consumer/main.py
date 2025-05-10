from models.StreamDataConsumer import StreamDataConsumer
import threading


streamDataConsumer = StreamDataConsumer()
streamDataConsumerThread = threading.Thread(target=streamDataConsumer.main())
streamDataConsumerThread.start()
streamDataConsumerThread.join()
