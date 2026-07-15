import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

NOMINAL_VOLTAGE = {"SUB-A": 34.5, "SUB-B": 13.8}
RATED_CURRENT = 400.0
NOMINAL_FREQ = 60.0

COMPONENTS = [
    {"id": "SUB-A-XFMR-1",  "type": "TRANSFORMER", "substation": "SUB-A", "nominalV": 34.5},
    {"id": "SUB-A-FDR-1",   "type": "FEEDER",      "substation": "SUB-A", "nominalV": 34.5},
    {"id": "SUB-A-BKR-1",   "type": "BREAKER",     "substation": "SUB-A", "nominalV": 34.5},
    {"id": "SUB-A-FDR-2",   "type": "FEEDER",      "substation": "SUB-A", "nominalV": 34.5},
    {"id": "SUB-A-BKR-2",   "type": "BREAKER",     "substation": "SUB-A", "nominalV": 34.5},
    {"id": "SUB-B-XFMR-1",  "type": "TRANSFORMER", "substation": "SUB-B", "nominalV": 13.8},
    {"id": "SUB-B-FDR-1",   "type": "FEEDER",      "substation": "SUB-B", "nominalV": 13.8},
    {"id": "SUB-B-BKR-1",   "type": "BREAKER",     "substation": "SUB-B", "nominalV": 13.8},
    {"id": "SUB-B-FDR-2",   "type": "FEEDER",      "substation": "SUB-B", "nominalV": 13.8},
    {"id": "SUB-B-BKR-2",   "type": "BREAKER",     "substation": "SUB-B", "nominalV": 13.8},
]

FEEDERS = [c for c in COMPONENTS if c["type"] == "FEEDER"]
BREAKERS = [c for c in COMPONENTS if c["type"] == "BREAKER"]
TRANSFORMERS = [c for c in COMPONENTS if c["type"] == "TRANSFORMER"]

breaker_pair = {"SUB-A-FDR-1": "SUB-A-BKR-1", "SUB-A-FDR-2": "SUB-A-BKR-2",
                "SUB-B-FDR-1": "SUB-B-BKR-1", "SUB-B-FDR-2": "SUB-B-BKR-2"}

INITIAL_STATE = {}
for c in COMPONENTS:
    nv = NOMINAL_VOLTAGE[c["substation"]]
    st = {
        "voltage": nv,
        "current": random.uniform(300.0, 450.0),
        "frequency": NOMINAL_FREQ,
    }
    if c["type"] == "BREAKER":
        st["breakerStatus"] = "CLOSED"
    if c["type"] == "TRANSFORMER":
        st["oilTemp"] = random.uniform(55.0, 70.0)
        st["tapPosition"] = 0
    INITIAL_STATE[c["id"]] = st

active_scenario = {}


def wait_for_kafka(bootstrap, retry_secs=2.0):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap],
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=0,
                max_block_ms=3000,
                request_timeout_ms=2000,
                api_version_auto_timeout_ms=3000,
            )
            print(f"[scada-sim] Connected to Kafka at {bootstrap}")
            return producer
        except NoBrokersAvailable:
            print(f"[scada-sim] Kafka not ready, retrying in {retry_secs}s...")
            time.sleep(retry_secs)


def start_scenario(scenario_type, tick):
    if scenario_type == "SAG_THEN_TRIP":
        feeder = random.choice(FEEDERS)
        brkr_id = breaker_pair[feeder["id"]]
        print(f"[scada-sim] scenario started: SAG_THEN_TRIP feeder={feeder['id']} breaker={brkr_id} (tick={tick})")
        return {"type": "SAG_THEN_TRIP", "components": [feeder["id"], brkr_id],
                "nominalV": feeder["nominalV"], "feeder": feeder["id"], "breaker": brkr_id,
                "sag_count": random.randint(4, 6), "sag_emitted": 0,
                "open_count": 10, "open_emitted": 0}

    elif scenario_type == "TRANSFORMER_OVERHEAT":
        xfmr = random.choice(TRANSFORMERS)
        print(f"[scada-sim] scenario started: TRANSFORMER_OVERHEAT transformer={xfmr['id']} (tick={tick})")
        return {"type": "TRANSFORMER_OVERHEAT", "step": 0, "components": [xfmr["id"]], "cooling": False}

    elif scenario_type == "BREAKER_FLAPPING":
        brkr = random.choice(BREAKERS)
        print(f"[scada-sim] scenario started: BREAKER_FLAPPING breaker={brkr['id']} (tick={tick})")
        return {"type": "BREAKER_FLAPPING", "step": 0, "components": [brkr["id"]],
                "toggles": 0, "lastToggle": 0, "hold": random.randint(2, 3),
                "toggleLimit": random.randint(5, 6)}

    elif scenario_type == "OVERCURRENT_BURST":
        feeder = random.choice(FEEDERS)
        print(f"[scada-sim] scenario started: OVERCURRENT_BURST feeder={feeder['id']} (tick={tick})")
        return {"type": "OVERCURRENT_BURST", "components": [feeder["id"]],
                "burst_count": random.randint(6, 8), "emitted": 0}

    return None


def apply_scenario(sc, cid, state, tick):
    if sc["type"] == "SAG_THEN_TRIP":
        if cid == sc["feeder"]:
            sc["sag_emitted"] += 1
            if sc["sag_emitted"] <= sc["sag_count"]:
                state[cid]["voltage"] = sc["nominalV"] * random.uniform(0.80, 0.88)
            else:
                state[cid]["voltage"] = sc["nominalV"]
        elif cid == sc["breaker"] and sc["sag_emitted"] >= sc["sag_count"]:
            if sc["open_emitted"] < sc["open_count"]:
                state[cid]["breakerStatus"] = "OPEN"
                sc["open_emitted"] += 1
            else:
                state[cid]["breakerStatus"] = "CLOSED"
                state[sc["feeder"]]["voltage"] = sc["nominalV"]
                print(f"[scada-sim] scenario ended: SAG_THEN_TRIP (tick={tick})")
                return False
        return True

    elif sc["type"] == "TRANSFORMER_OVERHEAT":
        xfmr_id = sc["components"][0]
        sc["step"] += 1
        if not sc["cooling"]:
            state[xfmr_id]["oilTemp"] = state[xfmr_id].get("oilTemp", 60) + 2.5
            if sc["step"] >= 20:
                sc["cooling"] = True
        else:
            state[xfmr_id]["oilTemp"] = max(55, state[xfmr_id]["oilTemp"] - 2.5)
            if state[xfmr_id]["oilTemp"] <= 65:
                print(f"[scada-sim] scenario ended: TRANSFORMER_OVERHEAT (tick={tick})")
                return False
        return True

    elif sc["type"] == "BREAKER_FLAPPING":
        brkr_id = sc["components"][0]
        sc["step"] += 1
        if sc["step"] - sc["lastToggle"] >= sc["hold"]:
            current = state[brkr_id].get("breakerStatus", "CLOSED")
            state[brkr_id]["breakerStatus"] = "OPEN" if current == "CLOSED" else "CLOSED"
            sc["lastToggle"] = sc["step"]
            sc["hold"] = random.randint(2, 3)
            sc["toggles"] += 1
        if sc["toggles"] >= sc["toggleLimit"] or sc["step"] > 25:
            state[brkr_id]["breakerStatus"] = "CLOSED"
            print(f"[scada-sim] scenario ended: BREAKER_FLAPPING (tick={tick})")
            return False
        return True

    elif sc["type"] == "OVERCURRENT_BURST":
        feeder_id = sc["components"][0]
        sc["emitted"] += 1
        if sc["emitted"] <= sc["burst_count"]:
            state[feeder_id]["current"] = RATED_CURRENT * random.uniform(1.3, 1.6)
        else:
            state[feeder_id]["current"] = random.uniform(300.0, 450.0)
            print(f"[scada-sim] scenario ended: OVERCURRENT_BURST (tick={tick})")
            return False
        return True

    return True


def generate_reading(comp, state, tick):
    cid = comp["id"]
    s = dict(state[cid])
    nv = NOMINAL_VOLTAGE[comp["substation"]]

    if "voltage" not in s or "current" not in s:
        s["voltage"] = nv
        s["current"] = random.uniform(300.0, 450.0)
    if "frequency" not in s:
        s["frequency"] = NOMINAL_FREQ

    # normal random walk
    s["voltage"] += random.uniform(-0.005, 0.005) * nv
    s["voltage"] = max(0.0, s["voltage"])
    s["current"] += random.uniform(-5.0, 5.0)
    s["current"] = max(0.0, s["current"])
    s["frequency"] += random.uniform(-0.05, 0.05)
    s["frequency"] = max(59.0, min(61.0, s["frequency"]))

    if comp["type"] == "TRANSFORMER":
        oil = s.get("oilTemp", 60.0)
        oil += random.uniform(-0.2, 0.2)
        s["oilTemp"] = max(40.0, min(120.0, oil))
        if random.random() < 0.05:
            s["tapPosition"] = s.get("tapPosition", 0) + random.choice([-1, 1])
            s["tapPosition"] = max(-5, min(5, s["tapPosition"]))
    if comp["type"] == "BREAKER":
        s["breakerStatus"] = s.get("breakerStatus", "CLOSED")

    # save back
    state[cid] = s

    reading = {
        "componentId": cid,
        "componentType": comp["type"],
        "substationId": comp["substation"],
        "voltage": round(s["voltage"], 4),
        "current": round(s["current"], 4),
        "frequency": round(s["frequency"], 4),
        "breakerStatus": s.get("breakerStatus"),
        "oilTemp": round(s["oilTemp"], 2) if s.get("oilTemp") is not None else None,
        "tapPosition": s.get("tapPosition"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return reading


def run():
    bootstrap = "kafka:9092"
    producer = wait_for_kafka(bootstrap)

    state = {k: dict(v) for k, v in INITIAL_STATE.items()}
    idx = 0
    # ticks are 0.1s, so 450-900 ticks = 45-90s between scenarios
    next_scenario_tick = random.randint(450, 900)

    try:
        while True:
            comp = COMPONENTS[idx % len(COMPONENTS)]
            cid = comp["id"]

            # check scenario engine
            if not active_scenario and idx >= next_scenario_tick:
                sc_type = random.choice(["SAG_THEN_TRIP", "TRANSFORMER_OVERHEAT", "BREAKER_FLAPPING", "OVERCURRENT_BURST"])
                sc = start_scenario(sc_type, idx)
                if sc:
                    active_scenario.update(sc)
                    active_scenario["_start_tick"] = idx

            if active_scenario and cid in active_scenario.get("components", []):
                keep = apply_scenario(active_scenario, cid, state, idx)
                if not keep:
                    active_scenario.clear()
                    next_scenario_tick = idx + random.randint(450, 900)

            reading = generate_reading(comp, state, idx)
            producer.send("scada.telemetry", key=cid, value=reading)
            idx += 1
            time.sleep(0.1)
    except KeyboardInterrupt:
        producer.close()
        print("[scada-sim] Stopped.")


if __name__ == "__main__":
    run()
