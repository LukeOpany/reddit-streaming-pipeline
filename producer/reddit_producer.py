import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

sample_messages = [
    {
        "subreddit": "python",
        "text": "The new Python release is really impressive."
    },
    {
        "subreddit": "dataengineering",
        "text": "Kafka makes streaming pipelines much easier to scale."
    },
    {
        "subreddit": "datascience",
        "text": "I am struggling to understand Spark window functions."
    },
    {
        "subreddit": "programming",
        "text": "This library has terrible documentation."
    },
    {
        "subreddit": "python",
        "text": "Has anyone tried this new data processing package?"
    }
]


while True:
    sample = random.choice(sample_messages)

    event = {
        "id": str(uuid4()),
        "subreddit": sample["subreddit"],
        "text": sample["text"],
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    producer.produce(
        "reddit_messages",
        value=json.dumps(event)
    )

    producer.flush()

    print(event)

    time.sleep(1)