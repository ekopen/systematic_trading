from kafka import KafkaConsumer

# Replace with your topic name
TOPIC = "price_ticks"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["159.65.41.22:9092"],
    auto_offset_reset="earliest",   # start from beginning if no offset
    enable_auto_commit=True,
    group_id="simple-consumer",     # consumer group name
)

print(f"Connected to Kafka, listening to topic '{TOPIC}'...")
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
