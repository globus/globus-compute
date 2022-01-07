import queue
import time

from funcx_endpoint.strategies import SimpleStrategy


class MockInterchange:
    def __init__(self, max_blocks=1, tasks=10):
        self.tasks_pending = tasks
        self.max_blocks = max_blocks
        self.managers = 0
        self.status = self.create_data()

    def get_outstanding_breakdown(self):
        item = self.status.get()

        if self.tasks_pending >= self.managers:
            this_round = self.tasks_pending - self.managers
            self.tasks_pending -= self.managers
        else:
            this_round = self.tasks_pending
            self.tasks_pending = 0

        current = [("interchange", this_round, this_round)]
        for i in range(self.managers):
            current.extend((f"manager_{i}", 1, 1))
        self.status.put(current)

        return item

    def scale_out(self):
        self.managers += 1

    def create_data(self):
        q = queue.Queue()
        items = [
            [("interchange", 0, 0)],
            [("interchange", 0, 0)],
            [("interchange", 0, 0)],
            [("interchange", self.tasks_pending, self.tasks_pending)],
            [("interchange", self.tasks_pending, self.tasks_pending)],
        ]
        [q.put(i) for i in items]

        return q


if __name__ == "__main__":
    print("Starting")
    mock = MockInterchange()
    # strategy = BaseStrategy(mock, threshold=2, interval=1)
    strategy = SimpleStrategy(mock, threshold=2, interval=1)
    print(strategy)
    for i in range(10):
        if i < 3:
            mock.scale_out()
        time.sleep(1)
    print("Exiting now")
