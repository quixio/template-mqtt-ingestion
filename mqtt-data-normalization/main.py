import os
from quixstreams import Application
from datetime import datetime

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


def expand_key(row, key, timestamp, headers):
    expanded_key = key.split("/")
    return {
        "machine": expanded_key[-2],
        "sensor": expanded_key[-1],
        "value": float(bytes.decode(row)),
        "timestamp": timestamp,
    }


def window_initializer(row: dict) -> dict:
    return {"machine": row["machine"]}


def window_reducer(agg: dict, row: dict) -> dict:
    agg.setdefault(row["sensor"], []).append(row["value"])
    return agg


def window_finalizer(finalized_window: dict):
    agg_values = finalized_window["value"]
    machine = agg_values.pop("machine")
    return {
        **{k: round(sum(v) / len(v), 2) for k, v in agg_values.items()},
        "timestamp": str(datetime.fromtimestamp(finalized_window["start"] / 1000)),
        "machine": machine
    }


app = Application(
    consumer_group="mqtt_data_normalization",
    auto_offset_reset="earliest")
input_topic = app.topic(os.environ["input"], value_deserializer="bytes", key_deserializer="str")
output_topic = app.topic(os.environ["output"])
sdf = app.dataframe(input_topic)

sdf = sdf.apply(expand_key, metadata=True)
sdf.update(lambda row: print('PRIOR TO TIMESTAMP')).print(metadata=True)
sdf = sdf.set_timestamp(lambda row, *_: row["timestamp"])
sdf = sdf.drop("timestamp")
sdf = sdf.group_by("machine")
sdf.update(lambda row: print('AFTER GROUPBY')).print(metadata=True)
sdf = sdf.hopping_window(1000, 200, 500).reduce(reducer=window_reducer, initializer=window_initializer).final()
sdf = sdf.apply(window_finalizer)
sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()