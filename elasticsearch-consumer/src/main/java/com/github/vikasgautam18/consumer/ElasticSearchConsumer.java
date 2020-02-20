package com.github.vikasgautam18.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.ResourceBundle;

import static com.github.vikasgautam18.consumer.Constants.*;

public class ElasticSearchConsumer {

    private static ResourceBundle consumerProps = ResourceBundle.getBundle("consumer");

    public static RestHighLevelClient createClient(){

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(consumerProps.getString(BONSAI_USERNAME),
                        consumerProps.getString(BONSAI_PASSWORD)));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(consumerProps.getString(BONSAI_HOSTNAME), 443, "https"))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerProps.getString(BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerProps.getString(GRP_ID_PREFIX)+"twitter2elk20Feb4");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer(consumerProps.getString(KAFKA_TOPIC));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook called..");
            logger.info("shutting down Kafka Consumer... ");
            consumer.close();
            logger.info("Kafka Consumer closed... ");
            logger.info("closing ELK client... ");
            try {
                client.close();
            } catch (IOException e) {
                logger.error("Error closing ELK client... ", e);
            }
            logger.info("ELK client closed... ");
        }));
        //TODO - fix while
        while (true){
            BulkRequest bulkRequest = new BulkRequest();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            logger.info("found "+ records.count() + " records...");
            for (ConsumerRecord<String, String> record : records) {
                try{
                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(record.value(), XContentType.JSON)
                            .id(getGenericId(record));

                    bulkRequest.add(indexRequest);

                } catch (Exception e){
                    e.printStackTrace();
                }
            }
            if(bulkRequest.numberOfActions() > 0) {
                logger.info("writing " + bulkRequest.numberOfActions() + " records to ElasticSearch... ");
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                for (BulkItemResponse item : bulkResponse.getItems()) {
                    logger.info("Record with Id " + item.getId() + " was added to ElasticSearch !");
                }
                logger.info("Commiting Offsets...");
                consumer.commitSync();
                logger.info("Offsets committed...");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //TODO: fix the client close
        //client.close();
    }

    private static String getGenericId(ConsumerRecord<String, String> record) {
        return record.topic() + "_" + record.partition() + "_" + record.offset();
    }
}
