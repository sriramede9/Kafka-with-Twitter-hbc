package com.sri.kafka.consumer;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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

	public static void main(String[] args) throws IOException {

//		RestHighLevelClient client = createClient();

		RestHighLevelClient client = trail2();

		String JsonString = "{\"foo\":\"bar\"}";

		IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(JsonString, XContentType.JSON);

		IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);

		consumerLogger.info(index.getId());

		// close the client

		client.close();

	}

}
