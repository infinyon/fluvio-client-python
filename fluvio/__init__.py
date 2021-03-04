from .fluvio_rust import *

class PartitionConsumerStreamIterator:
    def __init__(self, stream):
        self.stream = stream

    def __iter__(self):
        return self

    def __next__(self):
        return self.stream.next()
