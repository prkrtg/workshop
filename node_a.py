from nng_coms import NNGComs
import time

coms = NNGComs("nodeA", rep_addr="tcp://127.0.0.1:5001", pub_addr="tcp://127.0.0.1:5002", bus_addr="tcp://127.0.0.1:5003")

coms.start_rep_server(lambda msg: {"echo": msg})
coms.start_pub()
coms.start_bus()
coms.listen_bus(lambda msg: print(f"[nodeA] Received on BUS: {msg}"))

time.sleep(1)

while True:
    coms.publish("telemetry.temp", {"value": 42})
    coms.broadcast({"from": "nodeA", "status": "alive"})
    time.sleep(2)

