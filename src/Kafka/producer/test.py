import threading

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from producer.models.LiveMatchProducer import LiveMatchProducer


liveMatchProducer = LiveMatchProducer(sport_event="sr:sport_event:51404467")
liveMatchProducerThread = threading.Thread(target=liveMatchProducer.main())
liveMatchProducerThread.start()
liveMatchProducerThread.join()