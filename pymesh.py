"""
Command-and-control using a mesh network
"""

try:
    from zyre_pyzmq import Zyre as Pyre
except Exception as e:
    from pyre import Pyre

# standard imports
import argparse
import json
import coloredlogs, logging
import verboselogs
import sys
import threading
from time import sleep
import uuid
from pyre import zhelper
from logformats import CustomJsonFormatter
from queue import Queue
import zmq


class LogBuilder(object):
    def __init__(self, name, log_level):
        self.logger = verboselogs.VerboseLogger(name)
        self.logger.propagate = False
        if not self.logger.handlers: 
            f_handler = logging.FileHandler('./pymesh.log')
            f_formatter = CustomJsonFormatter('(timestamp) (level) (name) (message)')
            f_handler.setFormatter(f_formatter)
            c_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            ch = logging.StreamHandler()
            ch.setFormatter(c_formatter)
            self.logger.addHandler(ch)
            self.logger.addHandler(f_handler)
            coloredlogs.install(log_level, logger=self.logger)
        self.logger.spam('.__init__()')


# --------------------------------------------------------
# Simple message handler processes messages by only
# logging messages. This is a class because a concrete,
# useful handler would incorporate a range of functions
# for manipulating messages, depending on message/data.
# --------------------------------------------------------
class MessageHandlerSimple(LogBuilder):
    """
    Stub message handler
    """
    def __init__(self, log_level):
        super(MessageHandlerSimple, self).__init__("MessageHandlerSimple", log_level)

    def process_incoming(self, msg):
        """
        Send the message to logger
        """
        try:
            msg_obj = json.loads(msg.decode('utf-8'))
            for key in msg_obj:
                # only logging incoming messages
                if key == "msg_cont":
                    self.logger.success(
                        ".process_incoming() {key}, {value}".format(
                            key=key, value=str(msg_obj[key])
                        )
                    )
        except Exception as e:
            log_msg = (
                ".process_incoming() failed {key}, {value} with {xcptn}".format(
                    key=key, value=str(msg_obj[key]), xcptn=e
                )
            )
            self.logger.warning(log_msg)

    def process_outgoing(self, msg):
        return msg.encode('utf_8')


# --------------------------------------------------------
# On run() a forked process polls the network for
# messages, Subscriber is blocked waiting on its queue
# for a message to appear. As soon as a message is placed
# on the queue Subscriber is unblocked and calls the
# handler to process the message
# --------------------------------------------------------
class Subscriber(threading.Thread):
    """
    Receives messages from mesh
    """

    def __init__(self, node, log_level):
        super(Subscriber, self).__init__()
        self.hl = LogBuilder("Subscriber", log_level)
        self.logger = self.hl.logger
        self.queue = Queue()
        self.node = node
        self.run_loop = False

    def run(self):
        while self.run_loop:
            msg = self.queue.get()
            if msg == b'$$DIE':
                self.queue = None
                self.run_loop = False
            else:
                self.logger.debug("Subscriber thread got message: " + msg.decode('utf-8'))
                self.node.message_handler.process_incoming(msg)
                try:
                    self.queue.task_done()
                    self.logger.debug("Task done")
                except Exception as e:
                    self.logger.warning("Subscriber dequeue failed {xcptn}".format(xcptn=e))


# --------------------------------------------------------
# On run() Publisher waits for a message to be placed on
# its queue. As soon as a message is queued, Publisher
# becomes unblocked ad posts the message to the network
# --------------------------------------------------------
class Publisher(threading.Thread):
    """
    Sends messages to mesh
    """
    def __init__(self, mesh_pipe, message_handler, log_level):
        super(Publisher, self).__init__()
        self.hl = LogBuilder("Publisher", log_level)
        self.logger = self.hl.logger
        self.queue = None
        self.mesh_pipe = mesh_pipe
        self.run_loop = False
        self.message_handler = message_handler

    def run(self):
        self.queue = Queue()
        while self.run_loop:
            msg = self.queue.get()
            if msg == b'$$DIE':
                self.run_loop = False
                self.queue = None
            else:
                self.mesh_pipe.send(self.message_handler.process_outgoing(msg))
                self.queue.task_done()


# --------------------------------------------------------
class Node(LogBuilder):
    """
    Handles message passing for a node in a mesh network
    """
    def __init__(self, message_handler, name, log_level):
        super(Node, self).__init__(name, log_level)
        self.name = name
        self.message_handler = message_handler
        self.publisher = None
        self.subscriber = None
        self.ctx = None
        self.mesh_pipe = None
        self.log_level = log_level

    def connect(self):
        """
        Connect to the mesh network
        """
        self.ctx = zmq.Context()
        self.mesh_pipe = zhelper.zthread_fork(self.ctx, self.mesh_task)
        if self.publisher is None:
            self.publisher = Publisher(self.mesh_pipe, self.message_handler, self.log_level)
        if self.subscriber is None:
            self.subscriber = Subscriber(self, self.log_level)
        self.publisher.run_loop = True
        self.publisher.start()
        self.subscriber.run_loop = True
        self.subscriber.start()

    def disconnect(self):
        """
        Disconnect from mesh network
        """
        self.mesh_pipe.send("$$STOP".encode('utf-8'))
        # send poison pill to queues
        self.publisher.queue.put("$$DIE".encode('UTF-8'))
        self.subscriber.queue.put("$$DIE".encode('UTF-8'))
        # clear up
        self.publisher.join()
        self.subscriber.join()
        self.ctx = None

    def send_message(self, msg):
        ''' Send a message to the mesh '''
        self.publisher.queue.put(msg)

    def mesh_task(self, ctx, pipe):
        n = Pyre(self.name)
        n.join("MESH")

        n.set_header("MESH", "subnet c and c")
        n.start()

        poller = zmq.Poller()
        poller.register(pipe, zmq.POLLIN)
        self.logger.debug(n.socket())
        poller.register(n.socket(), zmq.POLLIN)
        self.logger.debug(n.socket())
        while True:
            items = dict(poller.poll())
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
                msg_group = ''
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
                            'msg_cont': msg_cont,
                            'msg_type': msg_type.decode('utf-8'),
                            'msg_uuid': str(msg_uuid),
                            'msg_name': msg_name.decode('utf-8'),
                            'msg_group': msg_group,
                            'msg_headers': msg_headers
                        }
                    )
                    self.subscriber.queue.put(msg_json.encode('utf-8'))
                except Exception as e:
                    self.logger.warning("json dumps error in mesh_task" % e)
        n.stop()


# --------------------------------------------------------
# Run from command line
# --------------------------------------------------------
def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store', dest='test', type=bool, help='Run a test')
    parser.add_argument(
        '--name',
        '-n',
        action='store',
        dest='name',
        type=str,
        help='The name of this node, used in logging'
    )
    parser.add_argument(
        '--log-level',
        '-v',
        action='store',
        dest='verbosity',
        type=int,
        help='Logging level: 5 (spam), 10, 15, 20, 25, 30, 35, 40, 50 (critical)')
    a = parser.parse_args(args)

    if a.test:
        msg_handler = MessageHandlerSimple(a.verbosity)
        node_a = Node(msg_handler, a.name, a.verbosity)
        node_a.connect()
        sleep(5)
        for i in range(10):
            node_a.send_message("Hello, World! {num}".format(num=str(i)))
            sleep(1)
        node_a.disconnect()

# ------------------------------------------------------------------------------


if __name__ == "__main__":
    main(sys.argv[1:])
