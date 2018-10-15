import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by ashok on 3/11/2018.
 */
public class ConsumerDemo {

    public static void main(String[] args){

        //1. Create properties object.
        Properties properties = new Properties();

        //2. Set the properties
        properties.setProperty("bootstrap.servers", "192.168.99.100:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        //set the groupId
        properties.setProperty("group.id", "test");
        //This is used to commit the offset whenever read completes.
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "earliest");

        //3. Create KafkaConsumer object.

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("second_topic"));

        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
                //consumerRecord.key();
                //consumerRecord.value();
                //consumerRecord.offset();
                //consumerRecord.partition();
                //consumerRecord.topic();
                //consumerRecord.timestamp();

                System.out.println("partition : " + consumerRecord.partition() +
                        ", Offset : " + consumerRecord.offset() +
                        ", key : " + consumerRecord.key() +
                        ", value : " + consumerRecord.value() );
            }
        }
    }
}
