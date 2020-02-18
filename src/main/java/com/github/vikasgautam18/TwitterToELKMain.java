package com.github.vikasgautam18;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.vikasgautam18.Constants.*;

public class TwitterToELKMain {
    private static ResourceBundle producerProps = ResourceBundle.getBundle("producer");
    private static Logger logger = LoggerFactory.getLogger(TwitterToELKMain.class.getName());
    private final String apiKey = producerProps.getString(API_KEY);
    private final String apiKeySecret = producerProps.getString(API_KEY_SECRET);
    private final String accessToken = producerProps.getString(ACCESS_TOKEN);
    private final String accessTokenSecret = producerProps.getString(ACCESS_TOKEN_SECRET);
    private final ArrayList<String> terms = Lists.newArrayList("SachinTendulkar", "SidNaaz");

    public TwitterToELKMain() {}

    public static void main(String[] args) {
        new TwitterToELKMain().run();
    }

    private void run() {

        //set up twitter client
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);
        Client client = getTwitterClient(queue);

        // Establish a connection
        client.connect();

        // add all necessary kafka properties
        KafkaProducer<String, String> producer = getKafkaProducer();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook called..");
            logger.info("shutting down twitter client...");
            client.stop();
            logger.info("Client is stopped!");
            logger.info("Closing Kafka Producer...");
            producer.close();
            logger.info("Producer is closed!");
        }));

        // read twitter messages and write them to Kafka
        try {
            int msgRead = 0;
            while(!client.isDone()) {
                String msg;
                msg = queue.poll(5, TimeUnit.SECONDS);

                if(msg != null){
                    msgRead++;
                    logger.debug(msg);
                    producer.send(new ProducerRecord<>(producerProps.getString(KAFKA_TOPIC),
                            String.valueOf(msgRead), msg), (metadata, exception) -> {
                        if(exception != null){
                            exception.printStackTrace();
                            producer.close();
                        }
                        else
                            logger.info("Partition - Offset = partition-" + metadata.partition() + "-" + metadata.offset());
                    });
                    producer.flush();
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            client.stop();
            producer.close();
        }
    }

    private KafkaProducer<String, String> getKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProps.getString(BOOTSTRAP_SERVERS));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, producerProps.getString(ACKS));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerProps.getString(COMPRESSION_TYPE));

        // return Producer Instance
        return new KafkaProducer<>(properties);
    }

    private Client getTwitterClient(BlockingQueue<String> queue) {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(terms);

        Authentication auth = new OAuth1(apiKey, apiKeySecret, accessToken, accessTokenSecret);

        // return client
        return new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }
}
