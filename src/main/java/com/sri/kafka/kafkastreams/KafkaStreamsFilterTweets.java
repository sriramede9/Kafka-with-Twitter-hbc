package com.sri.kafka.kafkastreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class KafkaStreamsFilterTweets {

	public static void main(String[] args) {
		// create properties

		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// similar to consumer groups
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		// create a topology

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// input topic

		KStream<String, String> stream = streamsBuilder.stream("twitter");

		KStream<String, String> filteredStream = stream.filter((k, jsonTweet) -> {
			// filter for tweets which has a user of over 10000 followers

			return extractIdFrom(jsonTweet);

		});

		filteredStream.to("important_tweets");

		// build the topology

		KafkaStreams kafkaStream = new KafkaStreams(streamsBuilder.build(), properties);

		// start our streams application

		kafkaStream.start();
	}

	private static boolean extractIdFrom(String value) {

		int asInt = JsonParser.parseString(value).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
				.getAsInt();

		if (asInt > 1000) {
			return true;
		}

		return false;

	}
}
