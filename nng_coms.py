import pynng
import json
import threading
import time


class NNGComs:
    def __init__(self, identity, rep_addr=None, pub_addr=None, bus_addr=None):
        self.identity = identity

        # REQ/REP
        self.rep_addr = rep_addr
        self.rep_thread = None

        # PUB/SUB
        self.pub_addr = pub_addr
        self.pub_socket = None

        # BUS
        self.bus_addr = bus_addr
        self.bus_socket = None
        self.bus_thread = None

        self.running = False

    # ============ REQ/REP ==============

    def start_rep_server(self, handler):
        def serve():
            with pynng.Rep0(listen=self.rep_addr) as rep:
                print(f"[{self.identity}] REPLY socket bound to {self.rep_addr}")
                while self.running:
                    try:
                        msg = rep.recv().decode()
                        print(f"[{self.identity}] Received REQ: {msg}")
                        response = handler(json.loads(msg))
                        rep.send(json.dumps(response).encode())
                    except Exception as e:
                        print(f"[{self.identity}] REP error: {e}")
        self.running = True
        self.rep_thread = threading.Thread(target=serve, daemon=True)
        self.rep_thread.start()

    def request(self, addr, message: dict, timeout=2000):
        with pynng.Req0(dial=addr, recv_timeout=timeout) as req:
            req.send(json.dumps(message).encode())
            print(f"[{self.identity}] Sent REQ to {addr}: {message}")
            response = req.recv()
            return json.loads(response)

    # ============ PUB/SUB ==============

    def start_pub(self):
        self.pub_socket = pynng.Pub0(listen=self.pub_addr)
        print(f"[{self.identity}] PUB socket bound to {self.pub_addr}")

    def publish(self, topic: str, payload: dict):
        if self.pub_socket is None:
            raise RuntimeError("Publisher not started.")
        msg = json.dumps(payload).encode()
        self.pub_socket.send(topic.encode() + b' ' + msg)
        print(f"[{self.identity}] Published on {topic}: {payload}")

    def subscribe(self, addr: str, topic_filter: str, callback):
        def listen():
            with pynng.Sub0(dial=addr) as sub:
                sub.subscribe(topic_filter.encode())
                print(f"[{self.identity}] SUB socket connected to {addr}, filtering '{topic_filter}'")
                while self.running:
                    try:
                        msg = sub.recv()
                        topic, data = msg.split(b' ', 1)
                        callback(topic.decode(), json.loads(data.decode()))
                    except Exception as e:
                        print(f"[{self.identity}] SUB error: {e}")
        t = threading.Thread(target=listen, daemon=True)
        t.start()

    # ============ BUS ==================

    def start_bus(self):
        self.bus_socket = pynng.Bus0(listen=self.bus_addr)
        print(f"[{self.identity}] BUS bound to {self.bus_addr}")
        time.sleep(0.1)  # Allow time for peers to connect

    def connect_bus_peer(self, addr: str):
        self.bus_socket.dial(addr)
        print(f"[{self.identity}] BUS connected to peer {addr}")
        time.sleep(0.1)

    def broadcast(self, message: dict):
        if self.bus_socket is None:
            raise RuntimeError("BUS not started.")
        self.bus_socket.send(json.dumps(message).encode())
        print(f"[{self.identity}] Broadcasted: {message}")

    def listen_bus(self, callback):
        def listen():
            while self.running:
                try:
                    msg = self.bus_socket.recv()
                    callback(json.loads(msg.decode()))
                except Exception as e:
                    print(f"[{self.identity}] BUS receive error: {e}")
        self.bus_thread = threading.Thread(target=listen, daemon=True)
        self.bus_thread.start()

    def stop(self):
        self.running = False
        print(f"[{self.identity}] Communication stopped.")

