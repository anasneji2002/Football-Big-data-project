from models.StreamDataProducer import StreamDataProducer
import threading


streamDataProducer = StreamDataProducer(sport_event="sr:sport_event:51404467")
streamDataProducerThread = threading.Thread(target=streamDataProducer.main())
streamDataProducerThread.start()
streamDataProducerThread.join()