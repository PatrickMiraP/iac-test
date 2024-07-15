import os
from quixstreams import Application
import json

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="default-group", auto_offset_reset="earliest", use_changelog_topics=False)

input_topic = app.topic(os.getenv("input", "input-topic"))
output_topic = app.topic(os.getenv("output", "output-topic"))

sdf = app.dataframe(input_topic)

# Filter items out without 'my_value' value.
sdf = sdf[sdf["my_value"].notnull()] 

# Calculate hopping window of 'my_value'. 
# 1 second window with 200ms steps.
sdf = sdf.apply(lambda row: row["my_value"]) \
        .hopping_window(1000, 200).mean().final() 

def transform(data, state):
    pass

sdf = sdf.apply(transform, use_state=True)

# Print JSON messages in console.
sdf = sdf.update(lambda row: print(json.dumps(row, indent=4)))

# Send the message to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)