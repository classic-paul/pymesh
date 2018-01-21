''' Command-and-control using a mesh network '''
try:
    from zyre_pyzmq import Zyre as Pyre
except Exception as e:
    from pyre import Pyre

# standard imports
import argparse
import json
import logging
import sys
import threading
from time import sleep
import uuid

# non-standard imports
from pyre import zhelper
from logformats import CustomJsonFormatter
from queue import Queue
import zmq

#--------------------------------------------------------
# Simple message handler processes messages by only
# logging messages. This is a class becase a concrete,
# useful handler would incorporate a range of functions
# for manipulating messages, depending on mesage/data.
#--------------------------------------------------------
class MessageHandlerSimple(object):
    def __init__(self):
        self.logger = logging.getLogger("pymesh")
    ''' Stub message handler '''
    def process_message(self, msg):
        ''' Send the message to stdout '''
        self.logger.debug("Message handler handled: " + msg.decode('utf-8'))
        msg_obj = json.loads(msg.decode('utf-8'))
        for key in msg_obj:
            print("key: " + key)
            print("value: " + str(msg_obj[key]))

#--------------------------------------------------------
# On run() a forked process polls the network for
# messages, Subscriber is blocked waiting on its queue
# for a message to appear. As soon as a message is placed
# on the queue Subscriber is unblocked and calls the
# handler to process the message
#--------------------------------------------------------
class Subscriber(threading.Thread):
    ''' Receives messages from mesh '''
    def __init__(self, node):
        super(Subscriber, self).__init__()
        self.queue = Queue()
        self.node = node
        self.run_loop = False
    def run(self):
        logger = logging.getLogger("pymesh")
        while self.run_loop:
            msg = self.queue.get()
            logger.debug("Subscriber caught msg: " + msg.decode('utf-8'))
            if not msg == b'$$DIE':
                self.node.message_handler.process_message(msg)
            logger.debug("Handled msg")
            self.queue.task_done()
        self.queue = None

#--------------------------------------------------------
# On run() Publisher waits for a message to be placed on
# its queue. As soon as a message is queued, Publisher
# becomes unblocked ad posts the message to the network
#--------------------------------------------------------
class Publisher(threading.Thread):
    ''' Sends messages to mesh '''
    def __init__(self, mesh_pipe):
        super(Publisher, self).__init__()
        self.queue = None
        self.mesh_pipe = mesh_pipe
        self.logger = logging.getLogger("pymesh")
        self.run_loop = False
    def run(self):
        self.queue = Queue()
        while self.run_loop:
            msg = self.queue.get()
            if not msg == b'$$DIE':
                self.mesh_pipe.send(msg.encode('utf_8'))
            self.queue.task_done()
        self.queue = None

#--------------------------------------------------------
class Node(object):
    ''' Handles message passing for a node in a mesh network '''
    def __init__(self, message_handler, log_level):
        self.message_handler = message_handler
        self.publisher = None
        self.subscriber = None
        self.ctx = None
        self.mesh_pipe = None
        self.logger = logging.getLogger("pymesh")
        self.logger.setLevel(log_level)
        f_handler = logging.FileHandler('./pymesh.log')
        f_formatter = CustomJsonFormatter('(timestamp) (level) (name) (message)')
        f_handler.setFormatter(f_formatter)
        self.logger.addHandler(f_handler)
        self.logger.propagate = False

    def connect(self):
        ''' Connect to the mesh network '''
        self.ctx = zmq.Context()
        self.mesh_pipe = zhelper.zthread_fork(self.ctx, self.mesh_task)
        if self.publisher is None:
            self.publisher = Publisher(self.mesh_pipe)
        if self.subscriber is None:
            self.subscriber = Subscriber(self)
        self.publisher.run_loop = True
        self.publisher.start()
        self.subscriber.run_loop = True
        self.subscriber.start()

    def disconnect(self):
        ''' Disconnect from mesh netwok '''
        self.mesh_pipe.send("$$STOP".encode('utf-8'))
        self.publisher.run_loop = False
        self.subscriber.run_loop = False
        self.publisher.queue.put("$$DIE".encode('UTF-8'))
        self.subscriber.queue.put("$$DIE".encode('UTF-8'))
        self.publisher.join()
        self.subscriber.join()
        self.ctx = None

    def send_message(self, msg):
        ''' Send a message to the mesh '''
        self.publisher.queue.put(msg)

    def mesh_task(self, ctx, pipe):
        n = Pyre("MESH")
        n.set_header("MESH", "subnet c and c")
        n.join("MESH")
        n.start()

        poller = zmq.Poller()
        poller.register(pipe, zmq.POLLIN)
        self.logger.debug(n.socket())
        poller.register(n.socket(), zmq.POLLIN)
        self.logger.debug(n.socket())
        while True:
            items = dict(poller.poll())
            self.logger.debug(n.socket(), items)
            if pipe in items and items[pipe] == zmq.POLLIN:
                message = pipe.recv()
                # message to quit
                if message.decode('utf-8') == "$$STOP":
                    break
                self.subscriber.queue.put(
                    json.dumps(
                        {"msg":message.decode('utf-8')}
                    ).encode('utf-8')
                )
                n.shouts("MESH", message.decode('utf-8'))
            else:
            #if n.socket() in items and items[n.socket()] == zmq.POLLIN:
                cmds = n.recv()
                msg_type = cmds.pop(0)
                msg_uuid = uuid.UUID(bytes=cmds.pop(0))
                msg_name = cmds.pop(0)
                msg_group = b''
                msg_headers = ''
                if msg_type.decode('utf-8') == "SHOUT":
                    msg_group = cmds.pop(0).decode('utf-8')
                elif msg_type.decode('utf-8') == "ENTER":
                    msg_headers = json.loads(cmds.pop(0).decode('utf-8'))

                msg_cont = []
                for i in cmds:
                    msg_cont.append(i.decode('utf-8'))
                try:
                    msg_json = json.dumps(
                        {
                            'msg_cont':msg_cont,
                            'msg_type':msg_type.decode('utf-8'),
                            'msg_uuid':str(msg_uuid),
                            'msg_name':msg_name.decode('utf-8'),
                            'msg_group':msg_group.decode('utf-8'),
                            'msg_headers':msg_headers
                        }
                    )
                    self.logger.debug(msg_json)
                    self.subscriber.queue.put(msg_json.encode('utf-8'))
                except Exception as e:
                    self.logger.debug(e)
        n.stop()

#--------------------------------------------------------
# Run from command line
#--------------------------------------------------------
def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store', dest='test', type=bool, help='Run a test')

    a = parser.parse_args(args)
    if a.test:
        node_a = Node(MessageHandlerSimple(), logging.DEBUG)
        node_a.connect()
        sleep(5)
        for i in range(10):
            node_a.send_message("Hello, World! {num}".format(num=str(i)))
            sleep(1)
        node_a.disconnect()

#------------------------------------------------------------------------------

if __name__ == "__main__":
    main(sys.argv[1:])
