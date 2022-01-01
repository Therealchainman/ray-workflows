from ray import workflow
import os

@workflow.step
def one() -> int:
    return 1

@workflow.step
def add(a: int, b: int) -> int:
    return a + b

output = add.step(100, one.step())

if __name__ == '__main__':
    path = os.path.dirname(os.path.abspath(__file__))
    workflow.init(storage=f'{path}/data')
    assert output.run(workflow_id='run_1') == 101
    assert workflow.get_status('run_1') == workflow.WorkflowStatus.SUCCESSFUL
    assert workflow.get_output('run_1') == 101
