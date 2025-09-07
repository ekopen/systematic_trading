import clickhouse_connect

# replace with the real hostname or IP of the server running ClickHouse in Docker
client = clickhouse_connect.get_client(
    host="159.65.41.22",
    port=8123,   # or 9000 if you prefer the native protocol
    username="default",   # or your custom user
    password="mysecurepassword",
    database="default"
)

# pull the whole table into a pandas DataFrame
df = client.query_df("SELECT AVG(price) FROM ticks_db LIMIT 100")

print(df.values[0][0])
