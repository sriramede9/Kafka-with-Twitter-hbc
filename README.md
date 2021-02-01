# Kafka-with-Twitter-hbc
Kafka Producer and Consumer concepts with twitter live tweets on the "term" "Kafa", channel it to "twitter" topic and Consume with Kafka consumer groups "twitter_consumer_group"

## Twitter - hbc config

https://github.com/twitter/hbc

## Producer config

```
Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// producer
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
    
    	if (msg != null) {

				log.info(msg);
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter", null, msg);
				producer.send(record, new Callback() {

					public void onCompletion(RecordMetadata metadata, Exception exception) {
						// TODO Auto-generated method stub

						if (exception != null) {
							log.error("some exception with producer ");
							log.error(exception.getLocalizedMessage());
						}
					}
				});
			}
```

## Shutdown Hook

```

Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("stopping application ...");
			log.info("shutting down client from twitter");
			hosebirdClient.stop();
			log.info("closing producer");
			producer.close();
			log.info("done!");

		}));

```