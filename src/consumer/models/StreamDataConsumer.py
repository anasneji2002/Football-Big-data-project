from .IConsumer import IConsumer
import datetime

class StreamDataConsumer(IConsumer):
    def __init__(self):
        super().__init__()
    
    def _sub(self):
        super()._sub_with_topics(["stream"])
    
    def _on_new_message(self, message):
        print(f"timestamp:{datetime.date.today()}, message:{message}")

    def main(self):
        self._sub()
        while True:
            msg = self.consumer.poll(timeout_ms=6000)
            if msg:
                self._on_new_message(msg)
            else:
                print("No new messages")