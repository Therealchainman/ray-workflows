from ray import workflow
import os
import ray
from typing import List, Tuple
import time
import random

'''
Workflow basics 
'''

def _get_root():
    return f"{os.path.dirname(os.path.abspath(__file__))}"

@workflow.step(name='read')
def read_data(num: int) -> List[float]:
    return [i for i in range(num)]
    
@workflow.step(name='preprocess')
def preprocessing(data: List[float]) -> List[float]:
    return [i * 2 for i in data]

@workflow.step(name='sum')
def aggregate(data: List[float]) -> float:
    return sum(data)

@workflow.step(name='inner')
def double(v: int) -> int:
    return v * 2


@workflow.step
def faulty_function() -> str:
    if random.random() > 0.5:
        raise RuntimeError('oops')
    return 'OK'

@workflow.step
def handle_errors(result: Tuple[str, Exception]):
    # exception field is none on success
    err = result[1]
    if err:
        return f'There was an error {err}'
    else:
        return 'OK'

if __name__ == "__main__":
    # ray.init()
    # # initialize workflow storage
    workflow.init(storage=f"{_get_root()}/data")

    # setup the workflow
    # data = read_data.step(10)
    # preprocessed_data = preprocessing.step(data)
    # output = aggregate.step(preprocessed_data)
    # inner_step = double.options(name='inner').step(1)
    # outer_step = double.options(name='outer').step(inner_step)
    # result = outer_step.run_async('double')
    # inner = workflow.get_output('double', name='inner')
    # outer = workflow.get_output('double', name='outer')
    # assert ray.get(inner) == 2
    # assert ray.get(outer) == 4
    # assert ray.get(result) == 4

    # error handling

    r1 = faulty_function.options(max_retries=5).step()
    r1.run()
    r2 = faulty_function.options(max_retries=5).step()
    handle_errors.step(r2).run()


    # time.sleep(30)
    # ray.shutdown()
