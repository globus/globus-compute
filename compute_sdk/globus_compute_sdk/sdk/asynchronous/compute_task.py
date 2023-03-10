import asyncio


class ComputeTask(asyncio.Future):
    """
    Represents a submitted Globus Compute task with an asychio wrapper
    """

    def __init__(self, task_id):
        """
        Parameters
        ----------
        task_id : uuid str
            The uuid of the Globus Compute task this instance is shadowing
        """
        super().__init__()
        self.task_id = task_id

    def __str__(self):
        return "Globus Compute Task ID " + self.task_id

    async def get_result(self):
        """
        Coroutine waiting for the Globus Compute task to complete and then
        return the result

        :return:
            result : Any
        """
        await self
        return self.result()
