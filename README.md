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

## Importance of Idempotent Producer Problem and Snippet to ignore such problem

```
	props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put("acks", "all");
		props.put("retries", Integer.MAX_VALUE);
```

## Linger ms and batch size to increase throughput

* To compress the message using snappy
* linger.ms will stall the message to reach the given batch size which is 16MB and time or size whichever comes first, the message will be dispatched to kafka broker

```
		// High Throughput Settings

		// 16 bytes
		props.put("batch.size", 16384);
		// 20 seconds
		props.put("linger.ms", 20);
		// snappy compression
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

```

## Elastic Search Set UP using Bonsai [sandbox]

```
create an index with console put request put /twitter
add docs to the index
put /twitter/{documentname}/id
/twitter/tweets/1
```
## Set up Elastic Search config to JAVA

```
<dependency>
    <groupId>org.elasticsearch.client</groupId>
    <artifactId>elasticsearch-rest-high-level-client</artifactId>
    <version>7.10.2</version>
</dependency>
```

### Elastic Search Consumer config

```
	public static RestHighLevelClient createClient() {

//		https://vvwqq42n2r:3wuhgyc6o@kafka-course-3974031019.us-east-1.bonsaisearch.net:443

		String hostname = "kafka-course-3974031019.us-east-1.bonsaisearch.net";
		String username = "vvwqq42n2r";
		String password = "3wuhgyc6o";

		// do if you are not running a local Elastic Search

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;

	}
```

## Response
```
[main] INFO com.sri.kafka.consumer.ElasticSearchConsumer - D8_ZaHcBaCBESApmMUl1

GET /twitter/tweets/D8_ZaHcBaCBESApmMUl1


{
  "_index": "twitter",
  "_type": "tweets",
  "_id": "D8_ZaHcBaCBESApmMUl1",
  "_version": 1,
  "_seq_no": 1,
  "_primary_term": 1,
  "found": true,
  "_source": {
    "foo": "bar"
  }
}
```

## Added Kafka-Consumer config and used Elastic Search to process the consumed records to index [twitter]

```
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {

				IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(record.value(),
						XContentType.JSON);

				IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);

				consumerLogger.info(index.getId());

				try {
					// small delay before we consume next batch

					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}
```

## Added unique Id which is same as tweet id to make the consumer idempotent

```
[main] INFO com.sri.kafka.consumer.ElasticSearchConsumer - idFromRecord 	 -->1357036689613611010

// Now You can find the tweet with the same id on elastic search get /twitter/tweets/1357036689613611010


```
