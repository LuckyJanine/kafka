package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class HydraulicLogProducer {
    public static void main(String[] args) {
        String topic = "hydraulic-logs";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        int[] numbers = {1026, 1789, 1690};
        String[] ids = {"AC123", "SL456", "HT789"};

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        while(true) {
            int currentAircraftIndex = random.nextInt(numbers.length);
            String flightNumber = "Flight-" + numbers[currentAircraftIndex];
            String aircraftId = ids[currentAircraftIndex];

            String message = String.format(
                            "\"aircraft id\": %1$s%n\"" +
                            "-------------------------%n" +
                            "-----------------------------------------------------%n" +
                            "[2025-02-10 09:05:00] INFO HYD SYS 1: %2$s%n" +
                            "[2025-02-10 09:05:00] INFO HYD SYS 2: %2$s%n" +
                            "[2025-02-10 09:05:00] INFO HYD SYS 3: %2$s%n" +
                            "[2025-02-10 09:05:00] INFO HYD SYS 1 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:00] INFO HYD SYS 2 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:00] INFO HYD SYS 3 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:02] INFO HYD SYS 1: %2$s%n" +
                            "[2025-02-10 09:05:02] INFO HYD SYS 2: %2$s%n" +
                            "[2025-02-10 09:05:02] INFO HYD SYS 3: %2$s%n" +
                            "[2025-02-10 09:05:02] INFO HYD SYS 1 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:02] INFO HYD SYS 2 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:02] INFO HYD SYS 3 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:04] INFO HYD SYS 1: %2$s%n" +
                            "[2025-02-10 09:05:04] INFO HYD SYS 2: %2$s%n" +
                            "[2025-02-10 09:05:04] INFO HYD SYS 3: %2$s%n" +
                            "[2025-02-10 09:05:04] INFO HYD SYS 1 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:04] INFO HYD SYS 2 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:04] INFO HYD SYS 3 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:06] INFO HYD SYS 1: %2$s%n" +
                            "[2025-02-10 09:05:06] INFO HYD SYS 2: %2$s%n" +
                            "[2025-02-10 09:05:06] INFO HYD SYS 3: %2$s%n" +
                            "[2025-02-10 09:05:06] INFO HYD SYS 1 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:06] INFO HYD SYS 2 TEMP: %3$s%n" +
                            "[2025-02-10 09:05:06] INFO HYD SYS 3 TEMP: %3$s%n",
                            aircraftId,
                            "Pressure nominal (3000 PSI)",
                            "Normal (30°C)"
            );

            producer.send(new ProducerRecord<>(topic, flightNumber, message));

            // System.out.println("Sent: " + message);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
