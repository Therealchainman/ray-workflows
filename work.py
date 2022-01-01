from ray import workflow
import os
from typing import List, Tuple
import ray

'''
Workflow basics 
'''

def _get_root():
    return f"{os.path.dirname(os.path.abspath(__file__))}"


if __name__ == '__main__':
    workflow.init(f'{_get_root()}/data')
    print(ray.get(workflow.get_output(workflow_id='run_1')))