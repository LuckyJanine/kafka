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
        props.put("partitioner", "round_robin");

        int[] numbers = {1549, 143, 32, 751, 780, 236};
        String[] ids = {"A320-214/N106US", "B767-233/C-GAUN", "A380-842/VH-OQA",
                        "MD-81/OY-KHO", "A330-342/B-HLL", "A330-243/C-GITS"};

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        try {
            while(!Thread.currentThread().isInterrupted() && AircraftProducer.running) {
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
                                "[2025-02-10 09:05:05] INFO HYD SYS 1: %2$s%n" +
                                "[2025-02-10 09:05:05] INFO HYD SYS 2: %2$s%n" +
                                "[2025-02-10 09:05:05] INFO HYD SYS 3: %2$s%n" +
                                "[2025-02-10 09:05:05] INFO HYD SYS 1 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:05] INFO HYD SYS 2 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:05] INFO HYD SYS 3 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:10] INFO HYD SYS 1: %2$s%n" +
                                "[2025-02-10 09:05:10] INFO HYD SYS 2: %2$s%n" +
                                "[2025-02-10 09:05:10] INFO HYD SYS 3: %2$s%n" +
                                "[2025-02-10 09:05:10] INFO HYD SYS 1 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:10] INFO HYD SYS 2 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:10] INFO HYD SYS 3 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:15] INFO HYD SYS 1: %2$s%n" +
                                "[2025-02-10 09:05:15] INFO HYD SYS 2: %2$s%n" +
                                "[2025-02-10 09:05:15] INFO HYD SYS 3: %2$s%n" +
                                "[2025-02-10 09:05:15] INFO HYD SYS 1 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:15] INFO HYD SYS 2 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:15] INFO HYD SYS 3 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:20] INFO HYD SYS 1: %2$s%n" +
                                "[2025-02-10 09:05:20] INFO HYD SYS 2: %2$s%n" +
                                "[2025-02-10 09:05:20] INFO HYD SYS 3: %2$s%n" +
                                "[2025-02-10 09:05:20] INFO HYD SYS 1 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:20] INFO HYD SYS 2 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:20] INFO HYD SYS 3 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:25] INFO HYD SYS 1: %2$s%n" +
                                "[2025-02-10 09:05:25] INFO HYD SYS 2: %2$s%n" +
                                "[2025-02-10 09:05:25] INFO HYD SYS 3: %2$s%n" +
                                "[2025-02-10 09:05:25] INFO HYD SYS 1 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:25] INFO HYD SYS 2 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:25] INFO HYD SYS 3 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:30] INFO HYD SYS 1: %2$s%n" +
                                "[2025-02-10 09:05:30] INFO HYD SYS 2: %2$s%n" +
                                "[2025-02-10 09:05:30] INFO HYD SYS 3: %2$s%n" +
                                "[2025-02-10 09:05:30] INFO HYD SYS 1 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:30] INFO HYD SYS 2 TEMP: %3$s%n" +
                                "[2025-02-10 09:05:30] INFO HYD SYS 3 TEMP: %3$s%n",
                        aircraftId,
                        "Pressure nominal (3000 PSI)",
                        "Normal (30°C)"
                );

                producer.send(new ProducerRecord<>(topic, message));

                System.out.println("hydraulic log data Sent: " + flightNumber);

                Thread.sleep(30000);
            }
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } finally {
            producer.close();
            System.out.println("hydraulic log producer closed ...");
        }
    }
}
