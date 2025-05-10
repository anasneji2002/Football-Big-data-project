from Kafka.producer.models.LiveMatchProducer import LiveMatchProducer
import threading


liveMatchProducer = LiveMatchProducer(sport_event="sr:sport_event:51404467")
liveMatchProducerThread = threading.Thread(target=liveMatchProducer.main())
liveMatchProducerThread.start()
liveMatchProducerThread.join()