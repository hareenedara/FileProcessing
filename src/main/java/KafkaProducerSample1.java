import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerSample1 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Producer<String,String> producer = getProducer();
        String filename = "/Users/edara/Downloads/test1.txt";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        long t1 = System.currentTimeMillis();
        sendMessages(reader,producer);
        System.out.println("TimeTaken (sec): "+(System.currentTimeMillis()-t1)/1000);
        System.out.println("Done");
    }

    public static void sendMessages(BufferedReader reader, Producer<String,String> producer) throws IOException, InterruptedException {
        String msg = "";
        int counter =1;
        try {
            while ((msg = reader.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>("topic2", Integer.toString(counter++), msg);
                producer.send(record);
                System.out.println(msg);
                Thread.sleep(500);
                if(counter == 50)
                    return;
            }
        }catch(Exception ex) {
            throw ex;
        }finally{
            reader.close();
            producer.close();
        }

    }

    public static Producer<String,String> getProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
//        for(int i = 0; i < 100; i++)
//            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
//
//        producer.close();
        return producer;

    }
}
