from nng_coms import NNGComs
import time

coms = NNGComs("nodeB", bus_addr="tcp://127.0.0.1:5004")
coms.start_bus()
coms.connect_bus_peer("tcp://127.0.0.1:5003")
coms.listen_bus(lambda msg: print(f"[nodeB] Got broadcast: {msg}"))

# Subscribe to telemetry
coms.subscribe("tcp://127.0.0.1:5002", "telemetry", lambda topic, msg: print(f"[nodeB] Got {topic}: {msg}"))

time.sleep(1)
reply = coms.request("tcp://127.0.0.1:5001", {"cmd": "status"})
print(f"[nodeB] REQ/REP got reply: {reply}")

while True:
    time.sleep(1)

