from funcx_endpoint.version import VERSION

__author__ = "The funcX team"
__version__ = VERSION

import platform
import multiprocessing
if platform.system() == 'Darwin':
    multiprocessing.set_start_method('fork', force=True)
