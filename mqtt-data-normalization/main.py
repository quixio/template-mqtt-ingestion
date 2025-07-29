import os
from quixstreams import Application
from datetime import datetime

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


def expand_key(row, key, timestamp, headers):
    expanded_key = key.split("/")
    result = {
        "machine": expanded_key[-2],
        "sensor": expanded_key[-1],
        "timestamp": timestamp
    }
    result["value"] = bytes.decode(row)
    return result


app = Application(
    consumer_group="mqtt_data_normalization",
    auto_offset_reset="earliest")
input_topic = app.topic(os.environ["input"], value_deserializer="bytes", key_deserializer="str")
output_topic = app.topic(os.environ["output"])
sdf = app.dataframe(input_topic)

sdf = sdf.apply(expand_key, metadata=True)
sdf = sdf.set_timestamp(lambda row, *_: row["timestamp"])
sdf = sdf.drop("timestamp")
sdf = sdf.group_by("machine")
sdf = sdf.apply(
    lambda row: {row["sensor"]: row["value"], "machine": row["machine"]}
)
sdf = sdf.hopping_window(10000, 2000).reduce(lambda window, row: {**window, **row}, lambda row: row).final()
sdf = sdf.apply(lambda row: {
    **row["value"],
    "timestamp": str(datetime.fromtimestamp(row["start"] / 1000))
})
sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()