import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by edara on 5/14/17.
 */
public class KafkaConsumerSample1 {
    public static void main(String[] args) throws Exception {
        int exceptionCounter = 0;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group1");
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic2"));
        while(true) {
            try {
                consume(consumer);
            } catch (KafkaSendException ex) {
                sendException(exceptionCounter, ex.getMsg());
            }
        }

    }

    public static void sendException(int counter,String msg) {
        Producer<String,String> producer = KafkaProducerSample1.getProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>("exceptions", Integer.toString(++counter), msg);
        producer.send(record);

    }

    public static void consume(KafkaConsumer consumer) throws Exception {
        while(true) {
            ConsumerRecords<String,String> records = consumer.poll(200);

            for(TopicPartition partition: records.partitions()){
                List<ConsumerRecord<String,String>> partitionRecords = records.records(partition);
                for(ConsumerRecord record: partitionRecords) {
                    Thread.sleep(100);
                    long offset = record.offset();
                    if(String.valueOf(record.key()).equals("13")){
                        KafkaSendException ex = new KafkaSendException();
                        ex.setMsg(String.valueOf(record.value()));
                        consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(offset+1)));
                        throw ex;
                    }
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(offset+1)));
                }

            }

        }
    }
}
