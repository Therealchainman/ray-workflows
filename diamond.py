from ray import workflow
from utilities.storage import _get_root
import time
import ray

'''
This is a conversion of the diamond workflow from argo to ray
'''

@workflow.step
def echo(msg: str, *deps) -> int:
    s = 0
    for d in deps:
        s += d
    print(f'{msg}+{s}', flush=True)
    time.sleep(1)
    return 2

if __name__ == '__main__':
    workflow.init(storage=f'{_get_root()}/data')
    # for i in range(17,20):
    #     A = echo.options(name='A').step('A')
    #     B = echo.options(name='B').step('B', A)
    #     C = echo.options(name='C').step('C', A)
    #     D = echo.options(name='D').step('D', B, C)
    #     D.run(workflow_id=f'diamond-{i}')
    print(workflow.list_all())

