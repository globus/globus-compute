from funcx_endpoint.endpoint.utils.config import Config


config = Config()


if __name__ == '__main__':

    import funcx
    import os
    import logging
    funcx.set_stream_logger()
    logger = logging.getLogger(__file__)

    endpoint_dir = "/home/yadu/.funcx/default"

    if config.working_dir is None:
        working_dir = "{}/{}".format(endpoint_dir, "worker_logs")
    # if self.worker_logdir_root is not None:
    #      worker_logdir = "{}/{}".format(self.worker_logdir_root, self.label)

    print("Loading : ", config)
    # Set script dir
    config.provider.script_dir = working_dir
    config.provider.channel.script_dir = os.path.join(working_dir, 'submit_scripts')
    config.provider.channel.makedirs(config.provider.channel.script_dir, exist_ok=True)
    os.makedirs(config.provider.script_dir, exist_ok=True)

    debug_opts = "--debug" if config.worker_debug else ""
    max_workers = "" if config.max_workers_per_node == float('inf') \
                  else "--max_workers={}".format(config.max_workers_per_node)

    worker_task_url = "tcp://127.0.0.1:54400"
    worker_result_url = "tcp://127.0.0.1:54401"

    launch_cmd = ("funcx-worker {debug} {max_workers} "
                  "-c {cores_per_worker} "
                  "--poll {poll_period} "
                  "--task_url={task_url} "
                  "--result_url={result_url} "
                  "--logdir={logdir} "
                  "--hb_period={heartbeat_period} "
                  "--hb_threshold={heartbeat_threshold} "
                  "--mode={worker_mode} "
                  "--container_image={container_image} ")

    l_cmd = launch_cmd.format(debug=debug_opts,
                              max_workers=max_workers,
                              cores_per_worker=config.cores_per_worker,
                              prefetch_capacity=config.prefetch_capacity,
                              task_url=worker_task_url,
                              result_url=worker_result_url,
                              nodes_per_block=config.provider.nodes_per_block,
                              heartbeat_period=config.heartbeat_period,
                              heartbeat_threshold=config.heartbeat_threshold,
                              poll_period=config.poll_period,
                              worker_mode=config.worker_mode,
                              container_image=None,
                              logdir=working_dir)
    config.launch_cmd = l_cmd
    print("Launch command: {}".format(config.launch_cmd))

    if config.scaling_enabled:
        print("About to scale things")
        config.provider.submit(config.launch_cmd, 1, 1)
