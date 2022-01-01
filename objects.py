from ray import workflow
import os
import ray
from typing import List

'''
Storing large data objects in Ray Object store.  
'''

def _get_root():
    return f"{os.path.dirname(os.path.abspath(__file__))}"

@ray.remote
def hello():
    return "hello"

@workflow.step
def words() -> List[ray.ObjectRef]:
    return [hello.remote(), ray.put("world")]

@workflow.step
def concat(words: List[ray.ObjectRef]) -> str:
    return " ".join([ray.get(w) for w in words])

if __name__ == "__main__":
    workflow.init(storage=f"{_get_root()}/data")
    assert concat.step(words.step()).run() == "hello world"
