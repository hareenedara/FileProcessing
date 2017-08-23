import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by edara on 5/14/17.
 */
public class KafkaExceptionConsumer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group1");
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("exceptions"));
        while(true) {
            ConsumerRecords<String,String> records = consumer.poll(200);

            for(TopicPartition partition: records.partitions()){
                List<ConsumerRecord<String,String>> partitionRecords = records.records(partition);
                for(ConsumerRecord record: partitionRecords) {
                    Thread.sleep(100);
                    long offset = record.offset();
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    if(String.valueOf(record.key()).equals("11")){
                        throw new Exception();
                    }

                    consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(offset+1)));
                }

            }


        }
    }
}
