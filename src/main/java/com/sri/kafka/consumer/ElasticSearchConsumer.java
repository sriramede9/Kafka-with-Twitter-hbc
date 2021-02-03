package com.sri.kafka.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	private static Logger consumerLogger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

	public static RestHighLevelClient createClient() {

////		https://vvwqq42n2r:3wuhgyc6o@kafka-course-3974031019.us-east-1.bonsaisearch.net:443
//
//		String hostname = "kafka-course-3974031019.us-east-1.bonsaisearch.net";
//		String username = "vvwqq42n2r";
//		String password = "3wuhgyc6o";
//
//		// do if you are not running a local Elastic Search
//
//		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
//
//		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
//				.setHttpClientConfigCallback(new HttpClientConfigCallback() {
//
//					@Override
//					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//						// TODO Auto-generated method stub
//						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//					}
//				});
//
//		RestHighLevelClient client = new RestHighLevelClient(builder);
//		return client;
		return null;

	}

	public static RestHighLevelClient trail2() {
		String hostname = "kafka-course-3974031019.us-east-1.bonsaisearch.net";
		String username = "vvwqq42n2r";
		String password = "3wuhgyc6o";

		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;

	}

	public static KafkaConsumer<String, String> CreateConsumer(String topic) {

//		String topic = "twitter";
		String bootstrapserver = "localhost:9092";
		String groupId = "twitter_consumer_group";

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapserver);
		props.put("group.id", groupId);
//		props.put("enable.auto.commit", "true");
//		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));
//		while (true) {
//			ConsumerRecords<String, String> records = consumer.poll(100);
//			for (ConsumerRecord<String, String> record : records) {
//
//				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
////				consumerLogger.info("partition \t" + record.partition());
//			}
//
//		}

		return consumer;
	}

	public static void main(String[] args) throws IOException {

//		RestHighLevelClient client = createClient();

		RestHighLevelClient client = trail2();

		KafkaConsumer<String, String> consumer = CreateConsumer("twitter");

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {

				String idFromRecord = extractIdFrom(record.value());

				consumerLogger.info("idFromRecord \t -->" + idFromRecord);

				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", idFromRecord).source(record.value(),
						XContentType.JSON);
//				IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(record.value(),
//						XContentType.JSON);

				// we need an id to make our consumer idempotent
				// this will solve the problem of duplicate messages when they are received with
				// the same id

				// we have two strategies to achieve a unique id for each message

				// strategy one

				String id = record.topic() + "_" + record.partition() + "_" + record.offset();

				// or use unique id generated by twitter api

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

//		String JsonString = "{\"foo\":\"bar\"}";
//
//

		// close the client

//		client.close();

	}

	private static String extractIdFrom(String value) {

		String id = JsonParser.parseString(value).getAsJsonObject().get("id_str").getAsString();

		return id;

	}

}
