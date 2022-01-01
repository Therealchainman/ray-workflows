from ray import workflow
import os
import ray
import time

'''
Virtual actors that have state durably logged to workflow storage.   
'''

def _get_root():
    return f"{os.path.dirname(os.path.abspath(__file__))}"

@workflow.virtual_actor
class Counter:
    def __init__(self):
        self.count = 0
    
    def incr(self):
        self.count += 1
        return self.count
    

if __name__ == "__main__":
    ray.init()
    workflow.init(storage=f"{_get_root()}/data")
    c1 = Counter.get_or_create('counter_1')
    assert c1.incr.run() == 1
    assert c1.incr.run() == 2

    time.sleep(30)
    ray.shutdown()
