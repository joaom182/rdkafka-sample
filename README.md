# Rust + Kafka sample application

Steps to run:

1. Run Docker Compose to setup a local Kafka

```
docker-compose up
```

2. Run the cargo package
```
cargo run --bin consumer
```
> The application will listen on the topic "test" and will print out all the messages produced on this topic
>

3. Produce messages to the topic "test"
```
docker-compose exec kafka1 kafka-console-producer.sh --broker-list 0.0.0.0:9092 --topic test
```
> Input values, hit enter, then press CTRL + C to finish the session.
>

# Expected Behavior
You should see the Rust application printing the produced messages in the topic "test".

# Current Behavior
Rust application is not printing out the messages produced and neither is it able to create a topic with the AdminClient, it's timing out.