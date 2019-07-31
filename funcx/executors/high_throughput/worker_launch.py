import os
import random
import subprocess

from queue import PriorityQueue


class Manager(object):
    """ MOCK Manager
    """
    def __init__ (self):
        self.debug = True
        self.worker_port = 50055
        self.addresses = '127.0.0.1,'
        self.logdir = "."

    def launch_worker(self, worker_id=str(random.random()),
                      mode='no_container',
                      container_uri=None,
                      walltime=10):
        """ Launch the appropriate worker

        Parameters
        ----------
        worker_id : str
           Worker identifier string
        mode : str
           Valid options are no_container, singularity
        walltime : int
           Walltime in seconds before we check status

        """
        print("LAUNCH_WORKER is only partially baked")

        debug = ' -d' if self.debug else ''
        # TODO : This should assign some meaningful worker_id rather than random
        worker_id = ' -w {}'.format(5)

        cmd = (f'funcx-worker {debug}{worker_id} '
               f'-a {self.addresses} '
               f'-p {self.worker_port} '
               f'--logdir={self.logdir} ')

        print("Command string : ", cmd)
        if mode == 'no_container':
            modded_cmd = cmd
        elif mode == 'singularity':
            modded_cmd = 'singularity run --writable {container_uri} {cmd}'.format(self.container_uri)
        else:
            raise NameError("Invalid container launch mode.")

        stdout = 'STDOUT: READING FAILED'
        stderr = 'STDERR: READING FAILED'
        try:
            proc = subprocess.Popen(cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=True)

            proc.wait(timeout=walltime)
            retcode = proc.returncode
            try:
                stdout = proc.stdout.read().decode('utf-8')
                stderr = proc.stderr.read().decode('utf-8')
            except Exception as e:
                print("Failed to open STDOUT/STDERR streams due to {}".format(e))
                raise

        except Exception as e:
            print("TODO : Got an error in worker launch, got error {}".format(e))
            retcode = 100

        return (retcode, stdout, stderr)


if __name__ == '__main__':

    mgr = Manager()
    ec, stdout, stderr = mgr.launch_worker()
    print("From worker stdout: ", stdout)
    print("From worker stderr: ", stderr)

