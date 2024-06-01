package demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log= LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

//For us to understand from the producer itself to which partition and offset the message was sent to, we'll use the callback interface.
// "RoundRobinPartitioner". The reason I used  round robin partitioner in CLI is because I want to produce to one
// partition at a time, and change every partition. If you do not use this round robin partitioner,
// There have been so many optimizations built in into Kafka right now that you will keep on producing
// to the same partition up until you send about 16 kilobytes of data, and then you will switch partition this is StickyPartitioner
// Here by default its being followed
    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");


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

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());//strings will be serialized  using
        // the StringSerializer which is a class provided with the Kafka clients.in Kafka when you use a producer,you pass
        // in some information and at first there will be a string and that will be serialized into bytes by this
        // key.serializer and the value.serializer before being sent to Apache Kafka.


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

       // let's produce multiple messages at a time in our topic and see the behavior.
        for (int j=0; j<10; j++){

            for (int i=0; i<30; i++){

                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "hello world " + i);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                // Since there is a delay, the Kafka cluster might have more time to process messages and manage
                // partitions, potentially affecting how messages are distributed across partitions.
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}