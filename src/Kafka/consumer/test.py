import threading

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from consumer.models.LiveMatchConsumer import LiveMatchConsumer

liveMatchConsumer = LiveMatchConsumer()
liveMatchConsumerThread = threading.Thread(target=liveMatchConsumer.main())
liveMatchConsumerThread.start()
liveMatchConsumerThread.join()
