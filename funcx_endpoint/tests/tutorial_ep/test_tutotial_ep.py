import time
import numpy
import logging
import argparse
import sys
import copy
from funcx.sdk.client import FuncXClient

# tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint


# Generate a random real orthogonal matrix of dimension (dim x dim)
def rvs(dim=3):
    import numpy as np
    random_state = np.random
    H = np.eye(dim)
    D = np.ones((dim,))
    for n in range(1, dim):
        x = random_state.normal(size=(dim - n + 1,))
        D[n - 1] = np.sign(x[0])
        x[0] -= D[n - 1] * np.sqrt((x * x).sum())
        # Householder transformation
        Hx = (np.eye(dim - n + 1) - 2. * np.outer(x, x) / (x * x).sum())
        mat = np.eye(dim)
        mat[n - 1:, n - 1:] = Hx
        H = np.dot(H, mat)
        # Fix the last sign such that the determinant is 1
    D[-1] = (-1)**(1 - (dim % 2)) * D.prod()
    # Equivalent to np.dot(np.diag(D), H) but faster, apparently
    H = (D * H.T).T
    return H


def check_determinant():
    import numpy as np
    mat = rvs(24)
    mat_inv = np.linalg.inv(mat)
    return np.linalg.det(mat) * np.linalg.det(mat_inv)


def identity(x):
    return x


class TestTutorial():

    def __init__(self, endpoint_id, func, expected, args=None, timeout=15, concurrency=5, tol=1e-5):
        self.endpoint_id = endpoint_id
        self.func = func
        self.expected = expected
        self.args = args
        self.timeout = timeout
        self.concurrency = concurrency
        self.tol = tol
        self.fxc = FuncXClient()
        self.func_uuid = self.fxc.register_function(self.func)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def run(self):
        try:
            submissions = []
            for _ in range(self.concurrency):
                task = self.fxc.run(self.args, endpoint_id=self.endpoint_id, function_id=self.func_uuid)
                submissions.append(task)

            time.sleep(self.timeout)

            unfinished = copy.deepcopy(submissions)
            while True:
                unfinished[:] = [task for task in unfinished if self.fxc.get_task(task)['pending']]
                if not unfinished:
                    break
                time.sleep(self.timeout)

            success = 0
            for task in submissions:
                result = self.fxc.get_result(task)
                if abs(result - self.expected) > self.tol:
                    self.logger.exception(f'Difference for task {task}. '
                                          f'Returned: {result}, Expected: {self.expected}')
                else:
                    success += 1

            self.logger.info(f'{success}/{self.concurrency} tasks completed successfully')
        except KeyboardInterrupt:
            self.logger.info('Cancelled by keyboard interruption')
        except Exception as e:
            self.logger.exception(f'Encountered exception: {e}')


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tutorial", required=True,
                        help="Tutorial Endpoint ID")
    args = parser.parse_args()

    rnd = numpy.random.randint(1000)
    tt = TestTutorial(args.tutorial, identity, rnd, args=rnd)
    tt.run()
