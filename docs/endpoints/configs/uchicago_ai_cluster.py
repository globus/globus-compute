# Launch 4 managers per node, each bound to 1 GPU
# Modify before use
NODES_PER_JOB = 2
GPUS_PER_NODE = 4
GPUS_PER_WORKER = 2

# DO NOT MODIFY
TOTAL_WORKERS = int((NODES_PER_JOB * GPUS_PER_NODE) / GPUS_PER_WORKER)
WORKERS_PER_NODE = int(GPUS_PER_NODE / GPUS_PER_WORKER)
GPU_MAP = ",".join([str(x) for x in range(1, TOTAL_WORKERS + 1)])
