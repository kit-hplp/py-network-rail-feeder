import os
import time
import logging

import stomp
import sqlite3
import pandas as pd

from topicmapping import TopicMapping
from datafeeder import RailDataFeeder

pd.options.display.max_columns = None

# Set up the personal information for the data feeds
USERNAME = 'KElliott@hartreepartners.com'
PASSWORD = 'a122a2dd-f9fa-4b9c-a65f-be4e62578b71'

PASSWORD = 'uv"<X5&U6&;ZmaVm'
# four topics to choose from - 1. MVT 2. PPM 3. VSTP 4. TD
TOPIC = "MVT"

train_rdf = RailDataFeeder(
                    db_name=TopicMapping[TOPIC][2], 
                    channel=TopicMapping[TOPIC][1], 
                    topic=TOPIC,
                    schema=TopicMapping[TOPIC][0],
                    username=USERNAME,
                    password=PASSWORD,
                    drop_if_exists=True,
                    view=False
)

train_rdf.download_feed()

# convert to dataframe
df = train_rdf.to_pandas()


HOSTNAME = "datafeeds.networkrail.co.uk"
conn = stomp.Connection(host_and_ports=[(HOSTNAME, 61618)])
#conn.set_listener('listener', listener(msger, view))
conn.start()
conn.connect(username=USERNAME, passcode=PASSWORD)

conn.subscribe(destination=f"/topic/MVT", id=1, ack='auto')