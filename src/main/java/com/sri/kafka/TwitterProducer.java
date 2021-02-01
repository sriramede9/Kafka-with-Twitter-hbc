package com.sri.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	private final Logger log = LoggerFactory.getLogger(TwitterProducer.class);

	public TwitterProducer() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {

		new TwitterProducer().run();
	}

	public void run() {

		log.info("Twitter Setup");

		// create twitter client

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		Client hosebirdClient = this.createTwitterClient(msgQueue);

		hosebirdClient.connect();

		// create a kafka producer

		// configuration

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");

		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// all about acks
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put("acks", "all");
		props.put("retries", Integer.MAX_VALUE);

		// High Throughput Settings

		// 16 bytes
		props.put("batch.size", 16384);
		// 20 seconds
		props.put("linger.ms", 20);
		// snappy compression
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

		// producer
		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		// loop to send tweets to kafka

		// adding shutdown hook

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("stopping application ...");
			log.info("shutting down client from twitter");
			hosebirdClient.stop();
			log.info("closing producer");
			producer.close();
			log.info("done!");

		}));

		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5l, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				hosebirdClient.stop();
			}

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

//			profit();
		}

		log.info("End Of Application");

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("kafka");
		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = new OAuth1("i4Dy8qCECbdRrGzqawBTdPY84",
				"ILb4iXmmS1zrpN61ANTAFa3ypvdTuJS9SRt40SxBUUXRZTBHyf",
				"2204774772-46qSKOv4X1fQpncMBjouF3uDztwHrxtLwv0YJns", "bM6rzlUguUdFBzlT3jWFIH4etOp4csx3ntVp8tdrS1GK3");

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
//				  .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
//		hosebirdClient.connect();

		return hosebirdClient;

	}
}
