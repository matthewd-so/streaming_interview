from typing import Any, Iterable, Generator

def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    # Keep a map from stationName to a dict {"high": float, "low": float}
    station_state: dict[str, dict[str, float]] = {}
    seen_any_sample = False
    latest_sample_ts: int | None = None

    for msg in events:
        # Every message must have a "type" key; otherwise it’s invalid
        if "type" not in msg:
            raise Exception("Malformed message (missing 'type'). Please verify input.")
        mtype = msg["type"]

        # If this is a temperature sample, update our internal state
        if mtype == "sample":
            if ("stationName" not in msg
                or "timestamp" not in msg
                or "temperature" not in msg):
                raise Exception("Malformed sample message; missing required fields. Please verify input.")

            station: str = msg["stationName"]
            ts = msg["timestamp"]
            temp = msg["temperature"]

            # Keep track of the latest timestamp we've seen
            if latest_sample_ts is None or ts > latest_sample_ts:
                latest_sample_ts = ts

            # If this is the first reading for that station, set both high and low to temp
            if station not in station_state:
                station_state[station] = {"high": temp, "low": temp}
            else:
                # Otherwise, update the high or low if needed
                if temp > station_state[station]["high"]:
                    station_state[station]["high"] = temp
                if temp < station_state[station]["low"]:
                    station_state[station]["low"] = temp

            seen_any_sample = True
            # We do not emit any output for a pure sample message
            continue

        # If this is a control message, check if it’s a snapshot or reset
        elif mtype == "control":
            if "command" not in msg:
                raise Exception("Malformed control message (missing 'command'). Please verify input.")
            cmd = msg["command"]

            if cmd == "snapshot":
                # If we haven’t seen any sample since the last reset, ignore
                if not seen_any_sample:
                    continue

                snapshot_obj: dict[str, Any] = {
                    "type": "snapshot",
                    "asOf": latest_sample_ts,
                    "stations": {}
                }
                for station, hl in station_state.items():
                    snapshot_obj["stations"][station] = {
                        "high": hl["high"],
                        "low": hl["low"]
                    }

                yield snapshot_obj
                continue

            elif cmd == "reset":
                # If no samples have arrived since the last reset, ignore
                if not seen_any_sample:
                    continue

                # Emit a reset object with the last timestamp, then clear everything
                reset_obj: dict[str, Any] = {
                    "type": "reset",
                    "asOf": latest_sample_ts
                }
                station_state.clear()
                seen_any_sample = False
                # Keep latest_sample_ts around so that reset_obj["asOf"] points to the right moment
                yield reset_obj
                continue

            else:
                # Unknown control command (neither snapshot nor reset)
                raise Exception(f"Unknown control command '{cmd}'. Please verify input.")

        else:
            # If message type isn’t "sample" or "control", we can’t handle it
            raise Exception(f"Unknown message type '{mtype}'. Please verify input.")
