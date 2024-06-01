package demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
   // poll method to get messages from the Kafka broker, and we'll see that the poll method will return data immediately
    // if possible, else will return empty and wait for a timeout until it responds.

   private static final Logger log= LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    //Loggers in Spring Boot offer better control, configurability, performance, and scalability compared to
    // `System.out.println()`. They also facilitate centralized log management for easier monitoring and debugging
    // in production environments

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");//Control: Logging frameworks like SLF4J allow you to control the logging
        // levels (INFO, DEBUG, ERROR) and output destinations (console, file, etc.) dynamically without changing the code.
        //Flexibility: You can easily filter and format log messages, and integrate with monitoring tools.

        String groupId="my-java-application";// by externalizing this at the top, we can quickly change it.
        String topic = "demo_java";


        //create Producer properties
        Properties properties= new Properties();
      //  properties.setProperty("key","value");


        //port pairs which are used for establishing an initial connection to the Kafka cluster.
        //connect to local host
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");


        // connect to conductor playground/ remote server
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"your-username\" password=\"your-password\";");
//        properties.setProperty("sasl.mechanism", "PLAIN");
//

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());// So the producer was serializing,
        // and the consumer deserializes,
        // that means takes bytes and transform them into an actual object. So we need a stringDeserializer.class.getName,
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.reset", "earliest");//Earliest means read from the beginning of my
        // topic.This corresponds to the --from beginning option when we looked at the Kafka CLI, and latest corresponds
        // to, only read the new messages sent from now." So because we wanna read the entire history of our topic,
        // we'll choose earliest.

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();//

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            // if I click on here on exit,it's going to send a shutdown.So thread zero, which is a new thread,not the
            // main thread will say,I detected a shutdown let's exit by calling consumer.wake up.So we were
            // here in the shutdown hook.And then the consumer.wakeup got called.

            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();//We went into poll, which triggered an exception because now we will see the main
                // thread saying,"Consumer is starting to shut down."

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });



        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
            // poll for data
            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }

            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
            //Then we go and close the consumer.
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");//this is called
            // a graceful shutdown of the consumer because now we're revoking the previously assigned partitions.
            // We are resetting generation and so on. We're leaving the group. This is all the things that happen
            // . Then the metrics shut down, the app info bar that's there also shuts down. And finally, when
            // Kafka has done all its thing, we display one final message called, that says, "The consumer is now
            // gracefully shut down."
        }


    }
}