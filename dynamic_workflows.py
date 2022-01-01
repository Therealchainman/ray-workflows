from ray import workflow
import os
import ray
from typing import List
import time
from functools import lru_cache

'''
Storing large data objects in Ray Object store.  
'''

def _get_root():
    return f"{os.path.dirname(os.path.abspath(__file__))}"

@workflow.step
def add(a: int, b: int) -> int:
    return a + b

@workflow.step
def fib(n: int) -> int:
    if n < 2:
        return n
    return add.step(fib.step(n - 1), fib.step(n - 2))

# normal fibonnaci function
def fibo(n: int) -> int:
    if n < 2:
        return n
    return fibo(n-1) + fibo(n-2)

# add memoization
@lru_cache(maxsize=None)
def fibom(n: int) -> int:
    if n < 2:
        return n
    return fibom(n-1) + fibom(n-2)
    

if __name__ == "__main__":
    ray.init()
    workflow.init(storage=f"{_get_root()}/data")
    n = 10
    start = time.perf_counter()
    print(fib.step(n).run())
    stop = time.perf_counter()
    print(f"Time taken for the workflow: {stop - start}")
    start = time.perf_counter()
    print(fibo(n))
    stop = time.perf_counter()
    print(f"Time taken for the normal fibonnaci function: {stop - start}")

    start = time.perf_counter()
    print(fibom(n))
    stop = time.perf_counter()
    print(f"Time taken for the memoized fibonnaci function: {stop - start}")

    time.sleep(30)
    ray.shutdown()

'''
My observations here, I don't see a purpose to continue to analyze the comparison between these.  It appears that workflows is orders of magnitude slower
than the normal recursive function and the memoized function.  The overhead is just too much for ray.  This just relates to finding the purpose for ray.  
It would not be a good fit for this type of problem.  
'''
