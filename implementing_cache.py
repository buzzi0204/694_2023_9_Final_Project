# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 19:43:13 2023

@author: athar
"""

import pickle

class Cache:
    def __init__(self, checkpoint_file=None, checkpoint_interval=None):
        self.max_size = 10
        self.cache = {}
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
            oldest_key = next(iter(self.cache))
            self.cache.pop(oldest_key)

        self.cache[key] = value

        if self.checkpoint_file and self.checkpoint_interval and len(self.cache) % self.checkpoint_interval == 0:
            self.save_checkpoint()

    def save_checkpoint(self):
        with open(self.checkpoint_file, 'wb') as file:
            pickle.dump(self.cache, file)

    def load_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'rb') as file:
                self.cache = pickle.load(file)
        except FileNotFoundError:
            pass
