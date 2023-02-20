use std::time::Duration;

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::{FromClientConfig, RDKafkaLogLevel},
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    types::RDKafkaError,
    ClientConfig, Message,
};
use tokio::stream::StreamExt;

fn get_topic_test() -> NewTopic<'static> {
    return NewTopic::new("test", 1, TopicReplication::Fixed(1));
}

fn get_client_config() -> ClientConfig {
    let client_config = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("session.timeout.ms", "5000")
        .set("message.timeout.ms", "5000")
        .set_log_level(RDKafkaLogLevel::Debug)
        .to_owned();
    return client_config;
}

fn create_admin_client(config: &ClientConfig) -> AdminClient<DefaultClientContext> {
    let admin_client =
        AdminClient::from_config(&config).expect("Error during creating admin client");
    return admin_client;
}

fn create_consumer(group_id: &str, topic: &str, config: &ClientConfig) -> StreamConsumer {
    let consumer: StreamConsumer = config
        .clone()
        .set("group.id", group_id)
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        .set("enable.auto.offset.store", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    consumer
}

async fn create_topics(
    topics: &[NewTopic<'static>],
    admin_client: &AdminClient<DefaultClientContext>,
) -> Result<Vec<Result<String, (String, RDKafkaError)>>, KafkaError> {
    let admin_options = AdminOptions::new()
        .operation_timeout(Some(Duration::from_secs(4)))
        .request_timeout(Some(Duration::from_secs(4)));
    return admin_client.create_topics(topics, &admin_options).await;
}

#[tokio::main]
async fn main() {
    let cfg = get_client_config();
    let admin_client = create_admin_client(&cfg);

    match create_topics(&[get_topic_test()], &admin_client).await {
        Ok(result) => {
            println!("topic created {:?}", result);
        }
        Err(err) => {
            println!("topic not created {}", err);
        }
    }

    println!("Start listenning!");

    let consumer = create_consumer("group_1", get_topic_test().name, &cfg);
    let mut msg_stream = consumer.start();

    while let Some(msg) = msg_stream.next().await {
        match msg {
            Ok(msg) => {
                println!(
                    "Received message: {}",
                    match msg.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(_)) => "<invalid utf-8>",
                    }
                );
                let res = consumer.store_offset(&msg);
                match res {
                    Ok(()) => {}
                    Err(e) => println!("Could not commit message: {} ", e),
                }
            }
            Err(e) => {
                println!("Could not receive and will not process message: {}", e)
            }
        };
    }
}
