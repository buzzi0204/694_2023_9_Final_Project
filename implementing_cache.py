import pickle
import time

class Cache:
    def __init__(self, checkpoint_file=None, checkpoint_interval=None):
        self.max_size = 10
        self.cache = {}
        self.key_times = []
        self.checkpoint_file = checkpoint_file
        self.checkpoint_interval = checkpoint_interval
        if checkpoint_file:
            self.load_checkpoint()

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        else:
            return None

    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            # Remove the oldest key
            oldest_key = min(self.key_times, key=lambda x: x[1])[0]
            self.key_times = [x for x in self.key_times if x[0] != oldest_key]
            self.cache.pop(oldest_key)

        self.cache[key] = value
        self.key_times.append((key, time.time()))

        if self.checkpoint_file and self.checkpoint_interval and len(self.cache) % self.checkpoint_interval == 0:
            self.save_checkpoint()

    def save_checkpoint(self):
        with open(self.checkpoint_file, 'wb') as file:
            pickle.dump(self.cache, file)

    def load_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'rb') as file:
                self.cache = pickle.load(file)
                self.key_times = [(k, time.time()) for k in self.cache.keys()]
        except FileNotFoundError:
            pass