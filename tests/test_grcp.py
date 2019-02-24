import os
import json
import tempfile
import unittest
import subprocess
import socket

class GRcpServerTestBase(unittest.TestCase):

    def setUp(self):
        self.start_grcp()

    def tearDown(self):
        self.grcp.kill()

    def start_grcp(self, port=4567):
        print(os.getcwd())
        self.grcp = subprocess.Popen(['python3', '-m', 'grcp.grcp', '--bind_port', str(port)],
                             stderr=subprocess.PIPE)
        # test if grcp has started successfully
        try:
            (stdout_data, stderr_data) = self.grcp.communicate(timeout=10)
            self.assertFalse(stderr_data)
        except:
            print(self.grcp.returncode)
            pass

    def test_start_stop(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10.0)
        s.connect(('localhost', 4567))
        msg = {'msg_type': 'router_up', 'routerid': '1.1.1.1'}
        s.send(json.dumps(msg).encode('utf-8'))
        s.close()
        self.assertTrue(True)
