import json
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

event = {
    "id": str(uuid4()),
    "subreddit": "python",
    "text": "Has anyone tried the new Python release?",
    "created_at": datetime.now(timezone.utc).isoformat()
}

producer.produce(
    "reddit_messages",
    value=json.dumps(event)
)

producer.flush()

print("Event sent to Kafka:")
print(event)