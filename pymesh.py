'''

pymesh

simple mesh network creation using zeroconf->zmq
pcp1976@gmail.com

'''
import socket
import threading
import struct
import sys
import logging
from time import sleep
import argparse
from zeroconf import ServiceBrowser, Zeroconf, ServiceInfo
from logformats import CustomJsonFormatter
import zmq

assert sys.version_info >= (3, 5)


#------------------------------------------------------------------------------

class MeshListener(object):
    '''
    Listens on zeroconf for node (dis)connections
    '''
    def __init__(self, name, bridge):
        self.bridge = bridge
        self.name = name
    def remove_service(self, _zeroconf, _type, name):
        ''' remove from our mesh connections '''
        self.bridge.on_disconnect(name)

    def add_service(self, _zeroconf, _type, name):
        ''' add node to our mesh connections '''
        info = _zeroconf.get_service_info(_type, name)
        _address = struct.unpack('BBBB', info.address)
        address = str(_address[0])+"."+str(_address[1])+"."+str(_address[2])+"."+str(_address[3])
        self.bridge.on_connect(name, address, str(info.port))

#------------------------------------------------------------------------------

class NetworkBrowser(threading.Thread):
    '''
    Listens for changes in advertised zeroconf services
    '''
    def __init__(self, node_name, bridge, lock):
        super(NetworkBrowser, self).__init__()
        self.bridge = bridge
        self.node_name = node_name
        self.run_loop = True
        self.my_zeroconf=Zeroconf()
        self.lock = lock
    def run(self):
        browser = ServiceBrowser(self.my_zeroconf, "_pymesh._tcp.local.", MeshListener(self.node_name, self.bridge))
        self.lock.acquire(blocking=True, timeout=-1)
        browser.join()
        self.my_zeroconf.close()
        browser = None
        self.lock.release()

#------------------------------------------------------------------------------

class NetworkAdvertiser(threading.Thread):
    ''' Advertises the node to the mesh '''
    def __init__(self, node_name, port, lock):
        super(NetworkAdvertiser, self).__init__()
        self.node_name = node_name
        self.ip_address = self.get_my_ip()
        self.desc = {'name': 'pymesh'}
        self.port = port
        self.info = None
        self.lock = lock
    def get_my_ip(self):
        ''' returns the ip address of the interface connected to the local subnet '''
        my_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            my_socket.connect(('10.255.255.255', 1))
            my_ip = my_socket.getsockname()[0]
        except:
            my_ip = '127.0.0.1'
        finally:
            my_socket.close()
        return my_ip
    def run(self):
        desc = {'type': 'pymesh'}
        info = ServiceInfo("_pymesh._tcp.local.", "{name}._pymesh._tcp.local.".format(
            name=self.node_name
            ), socket.inet_aton(self.get_my_ip()), self.port, 0, 0, desc, "{name}.local.".format(
                name=self.node_name)
                )
        my_zeroconf = Zeroconf()
        my_zeroconf.register_service(info)
        self.lock.acquire(blocking=True, timeout=-1)
        my_zeroconf.unregister_service(info)
        my_zeroconf.close()
        self.lock.release()

#------------------------------------------------------------------------------

class Publisher(threading.Thread):
    def __init__(self, node, lock):
        super(Publisher, self).__init__()
        self.node = node
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://*:{port}".format(port=self.node.port))
        self.lock = lock
    def run(self):
        self.lock.acquire(blocking=True, timeout=-1)
        self.lock.release()
    def message_shout(self, topic, messagedata):
        self.socket.send_string("%s %s" % (topic, messagedata))

#------------------------------------------------------------------------------

class Subscriber(object):
    def __init__(self, node, lock):
        self.logger = logging.getLogger(node.name)
        super(Subscriber, self).__init__()
        self.connections = {}
        self.node = node
        self.running = lock
        self.context = zmq.Context()
        self.sub = self.context.socket(zmq.SUB)

    def run(self):
        while self.running:
            self.logger.debug(self.sub.recv(zmq.NOBLOCK))
        self.logger.debug("done")
        self.context = None

    def on_connect(self, address, port):
        self.logger.debug("1")
        url = "tcp://" + address + ":" + port
        self.logger.debug(url)
        self.sub.connect(url)
        for topic in self.node.topics:
            self.logger.debug(topic)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)

#------------------------------------------------------------------------------

class Node(object):
    '''
    Handles connecting, disconnecting,
    and message passing for a node in a mesh network
    '''
    def __init__(self, node_name, port, log_level, topics):
        self.name = node_name
        self.port = port
        self.topics = topics # topics to subscribe to
        self.log_level = log_level
        self.__initialise_logger__()
        '''
        zmq
        '''
        self.publisher = None
        self.subscriber = None
        self.publisher_lock = threading.Lock()
        self.subscriber_run = False
        '''
        zeroconf
        '''
        self.network_browser = None
        self.browser_lock = threading.Lock()
        self.network_advertiser = None
        self.advertiser_lock = threading.Lock()
    def __initialise_logger__(self):
        """Set up logging"""
        self.__logger = logging.getLogger(self.name)
        self.__logger.setLevel(self.log_level)
        if not self.__logger.handlers:
            f_handler = logging.FileHandler('./{name}.log'.format(name=self.name))
            f_handler.setLevel(self.log_level)
            f_formatter = CustomJsonFormatter('(timestamp) (level) (name) (message)')
            f_handler.setFormatter(f_formatter)
            self.__logger.addHandler(f_handler)

    def on_connect(self, name, address, port):
        '''
        Triggered when zeroconf finds a new node,
        initiates a zmq connection betwwen this node and
        the newly-discovered node
        '''
        self.subscriber.on_connect(address, port)

    def on_disconnect(self, name):
        '''
        Triggered when memory_db completes a delete,
        signifying a zmq connection betwwen this node and
        the removed node should be terminated
        '''
        self.__logger.debug("entered on_disconnect %s", name)

    def mesh_discovery_start(self):
        ''' search for mesh nodes '''
        self.subscriber_run = True
        self.start_subscriber()
        self.browser_lock.acquire(blocking=True, timeout=-1)
        self.start_browser()
        self.advertiser_lock.acquire(blocking=True, timeout=-1)
        self.start_advertiser()
        self.publisher_lock.acquire(blocking=True, timeout=-1)
        self.start_publisher()
    def mesh_discovery_end(self):
        ''' stop searching for mesh nodes '''
        self.advertiser_lock.release()
        self.subscriber_run = False
        self.browser_lock.release()
        self.publisher_lock.release()

    def start_browser(self):
        if self.network_browser is None:
            self.network_browser = NetworkBrowser(self.name, self, self.browser_lock)
        self.network_browser.start()
    def start_advertiser(self):
        if self.network_advertiser is None:
            self.network_advertiser = NetworkAdvertiser(self.name, self.port, self.advertiser_lock)
        self.network_advertiser.start()
    def start_publisher(self):
        if self.publisher is None:
            self.publisher = Publisher(self, self.publisher_lock)
        self.publisher.start()
    def start_subscriber(self):
        if self.subscriber is None:
            self.subscriber = Subscriber(self, self.subscriber_run)

    def message_shout(self, topic, messagedata):
        ''' Broadcast a message to all peers '''
        self.publisher.message_shout(topic, messagedata)
    def message_whisper(self):
        ''' Send a message to a single peer '''
        raise NotImplementedError()
    def message_recieve(self):
        ''' ? Need to look at zmq docs! '''
        raise NotImplementedError()

#------------------------------------------------------------------------------

def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', "-n", action='store', dest='name', help='Name of node')
    parser.add_argument('--port', "-p", action='store', dest='port', type=int, help='Port the node will publish on')
    parser.add_argument('--log-level', "-l", action='store', dest='log_level', help='logging level ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")', default='WARNING')
    parser.add_argument('--topics', "-t", nargs='+', action='store', dest='topics', help='topics to subscribe to (name will be subscribed to automatically)')
    
    a = parser.parse_args(args)
    a.topics.append(a.name)
    
    mesh_node = Node(a.name, a.port, a.log_level, a.topics)
    mesh_node.mesh_discovery_start()
    try:
        while True:
            sleep(5)
            mesh_node.message_shout("World", "Hello, World!")
    except KeyboardInterrupt:
        pass
    mesh_node.mesh_discovery_end()

#------------------------------------------------------------------------------

if __name__ == "__main__":
    main(sys.argv[1:])
