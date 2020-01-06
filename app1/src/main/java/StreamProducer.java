import kafka.utils.Json;
import kafka.utils.json.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Ordre {
    public String societe;
    public String typeOrdre;
    public int bnAction;
    public Collection<Double> prix;

    public Ordre(String societe, String typeOrdre, int nbrAction, Collection<Double> prices) {
        this.societe = societe;
        this.typeOrdre = typeOrdre;
        this.bnAction = nbrAction;
        this.prix = prices;
        this.setPrix();
    }

    public void setPrix() {
        if(bnAction > 0) {
            for (int i = 0; i < bnAction; i++) {
                prix.add(Math.random()*9000);
            }
        }
    }

    @Override
    public String toString() {
        return "Ordre{" +
                "'company': '" + societe + '\'' +
                ", 'typeOrdre': '" + typeOrdre + '\'' +
                ", 'nbrAction': " + bnAction +
                ", 'prices': " + prix +
                '}';
    }
}

public class StreamProducer {
    private int counter;
    private String KAFKA_BROKER_URL = "192.168.43.2:9092";
    private String TOPIC_NAME = "testTopic";
    private String clientID = "client_prod_1";
    public static Map<Integer, String> companies = new HashMap<>();
    public static Map<Integer, String> typeOrdre = new HashMap<>();

    public static void loadCompanies() {
        companies.put(1, "Lesieur");
        companies.put(2, "Inwi");
        companies.put(3, "IAM");
        companies.put(4, "CGI");
        companies.put(5, "Orange");
        companies.put(6, "Brexia");
    }

    public static void loadTypeOrdre() {
        typeOrdre.put(2, "VENTE");
        typeOrdre.put(1, "ACHAT");
    }

    public static void main(String[] args) {
        loadCompanies();
        loadTypeOrdre();
        new StreamProducer();
    }

    public StreamProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER_URL);
        properties.put("client.id", clientID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(properties);
        Random random = new Random();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
            ++counter;
            Ordre ordre = new Ordre(companies.get(random.nextInt(6)+1), typeOrdre.get(random.nextInt(2)+1), random.nextInt(10)+1,new ArrayList<>());
            String msg = ordre.toString();
            kafkaProducer.send(new ProducerRecord<Integer, String>(TOPIC_NAME, ++counter, msg), (metadata, ex) -> {
                System.out.println("Sending Message key: " + counter + ", Value: " + msg);
                System.out.println("Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
            });
        }, 10, 10, TimeUnit.MILLISECONDS);
    }
}
