from tblib import Traceback
import logging

logger = logging.getLogger(__name__)

class RemoteExceptionWrapper:
    def __init__(self, e_type, e_value, traceback):

        self.e_type = e_type
        self.e_value = e_value
        self.e_traceback = traceback

    def reraise(self):
        logger.debug("Reraising exception of type {}".format(self.e_type))
        reraise(self.e_type, self.e_value, self.e_traceback)



"""
Information that we want to be able to ship around:


* Function run information ->
    * Function id <---> Container id ? (is there a 1-1 mapping here?)
    * Container id
    * Endpoint id

* Function body
    * Potentially in a byte compiled form ?
* All parameters ->
    * Args  + Kwargs


From the client side. At function invocation we need to capture

"""


import pickle
import json

import concretes

class FuncXSerializer(object):


    def __init__():
        self.methods_map = {}

        for 

    def chomp_head(payload):
        pass

    def serialize():
        pass

    def deserialize():
        pass



