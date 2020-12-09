import threading
import time
from pytest import fixture

from funcx_endpoint.executors.high_throughput.interchange import Interchange
from funcx_endpoint.strategies.kube_simple import KubeSimpleStrategy


def no_op_worker():
    """
    This is used to trick the base strategy into not doing scheduled checks.
    The strategize calls are only triggered by explicit calls to make_callback in the
    tests.
    :return:
    """
    pass


class TestKubeSimple:

    @fixture
    def mock_interchange(self, mocker):
        mock_interchange = mocker.MagicMock(Interchange)
        mock_interchange.config = mocker.Mock()
        mock_interchange.config.max_workers_per_node = 1
        mock_interchange.config.provider = mocker.Mock()
        mock_interchange.config.provider = mocker.Mock()
        mock_interchange.config.provider.min_blocks = 1
        mock_interchange.config.provider.max_blocks = 4
        mock_interchange.config.provider.nodes_per_block = 1
        mock_interchange.config.provider.parallelism = 1.0
        mock_interchange.get_outstanding_breakdown = mocker.Mock(
            return_value=[('interchange', 16, True)])
        mock_interchange.get_total_tasks_outstanding = mocker.Mock(
            return_value={'RAW': 0})
        mock_interchange.provider_status = mocker.Mock(return_value={'RAW': 16})
        mock_interchange.get_total_live_workers = mocker.Mock(return_value=0)
        mock_interchange.scale_in = mocker.Mock()
        mock_interchange.scale_out = mocker.Mock()
        return mock_interchange

    @fixture
    def kube_strategy(self):
        """
        Create an instance of the KubeStrategy but disable the timer
        :return:
        """
        strat = KubeSimpleStrategy(threshold=10, interval=1, max_idletime=5)
        strat._thread = threading.Thread(target=no_op_worker())
        return strat

    def test_no_tasks_no_pods(self, mock_interchange, kube_strategy):
        mock_interchange.get_outstanding_breakdown.return_value = [
            ('interchange', 0, True)]
        mock_interchange.get_total_tasks_outstanding.return_value = []
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

    def test_scale_in_with_no_tasks(self, mock_interchange, kube_strategy):
        # First there is work to do and pods are scaled up
        mock_interchange.get_outstanding_breakdown.return_value = [('interchange', 16, True)]
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 16}
        mock_interchange.provider_status.return_value = {'RAW': 16}
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

        # Now tasks are all done, but pods are still running. Idle time has not yet
        # been reached, so the pods will still be running.
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 0}
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

        # Sleep til the idle time is reached and see that we scale in
        time.sleep(5)
        kube_strategy.make_callback(kind="timer")

        # There were 16 pods, and the minimum is 1
        mock_interchange.scale_in.assert_called_with(15, task_type="RAW")
        mock_interchange.scale_out.assert_not_called()

    def test_task_arrives_during_idle_time(self, mock_interchange, kube_strategy):
        # First there is work to do and pods are scaled up
        mock_interchange.get_outstanding_breakdown.return_value = [('interchange', 16, True)]
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 16}
        mock_interchange.provider_status.return_value = {'RAW': 16}
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

        # Now tasks are all done, but pods are still running. Idle time has not yet
        # been reached, so the pods will still be running.
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 0}
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

        # Now add a new task
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 1}
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

        # Verify that idle time is reset
        time.sleep(5)
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 0}
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

    def test_task_backlog_within_parallelism(self, mock_interchange, kube_strategy):
        # Aggressive scaling so new tasks will create new pods
        mock_interchange.config.provider.parallelism = 1.0
        mock_interchange.config.provider.max_blocks = 16
        mock_interchange.get_outstanding_breakdown.return_value = [('interchange', 1, True)]
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 16}
        mock_interchange.provider_status.return_value = {'RAW': 1}
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_called_with(15, task_type="RAW")

    def test_task_backlog_within_gated_by_parallelism(self, mock_interchange, kube_strategy):
        # Lazy scaling, so just a single new task won't spawn a new pod
        mock_interchange.config.provider.parallelism = 0.5
        mock_interchange.config.provider.max_blocks = 16
        mock_interchange.get_outstanding_breakdown.return_value = [('interchange', 1, True)]
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 16}
        mock_interchange.provider_status.return_value = {'RAW': 1}
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_called_with(7, task_type="RAW")

    def test_task_backlog_within_gated_by_max_blocks(self, mock_interchange,
                                                     kube_strategy):
        mock_interchange.config.provider.parallelism = 1.0
        mock_interchange.config.provider.max_blocks = 8
        mock_interchange.get_outstanding_breakdown.return_value = [('interchange', 8, True)]
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 16}
        mock_interchange.provider_status.return_value = {'RAW': 1}
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_called_with(7, task_type="RAW")

    def test_task_backlog_within_already_max_blocks(self, mock_interchange, kube_strategy):
        mock_interchange.config.provider.parallelism = 1.0
        mock_interchange.config.provider.max_blocks = 8
        mock_interchange.get_outstanding_breakdown.return_value = [('interchange', 8, True)]
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 16}
        mock_interchange.provider_status.return_value = {'RAW': 16}
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_not_called()

    def test_scale_when_no_pods(self, mock_interchange, kube_strategy):
        mock_interchange.config.provider.parallelism = 0.01  # Very lazy scaling
        mock_interchange.get_outstanding_breakdown.return_value = [('interchange', 8, True)]
        mock_interchange.provider_status.return_value = {'RAW': 0}
        mock_interchange.get_total_tasks_outstanding.return_value = {'RAW': 1}
        kube_strategy.start(mock_interchange)
        kube_strategy.make_callback(kind="timer")
        mock_interchange.scale_in.assert_not_called()
        mock_interchange.scale_out.assert_called_with(1, task_type="RAW")
