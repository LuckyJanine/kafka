package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class EngineLogProducer {
    public static void main(String[] args) {
        String topic = "engine-logs";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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
                        "Timestamp: 2024-02-10T09:05:00Z%n" +
                                "----------------------------%n" +
                                "\"flight number\": %s%n" +
                                "\"aircraft id\": %s%n" +
                                "\"engine number\": %d%n" +
                                "----------------------------------%n" +
                                "\"Engine N1 (Fan Speed)\": %.1f%%%n" +
                                "\"Engine N2 (Core Speed)\": %.1f%%%n" +
                                "\"engine thrust\": %d lbs%n" +
                                "\"EGT (Exhaust Gas Temperature)\": %d°C%n" +
                                "\"Fuel Flow\": %d kg/hr%n" +
                                "\"Oil Pressure\": %d psi%n" +
                                "\"Oil Temperature\": %d°C%n" +
                                "\"Vibration Level\": %.1f IPS%n" +
                                "\"Throttle Lever Angle\": %.1f°%n" +
                                "\"EPR (Engine Pressure Ratio)\": %.2f%n" +
                                "------------------------------------------%n" +
                                "Status: NORMAL%n" +
                                "--------------------------------------------%n" +
                                "Warnings: NONE%n" +
                                "-------------------------------------------------------%n" +
                                "Source: EEC (Electronic Engine Controller)",
                        flightNumber,
                        aircraftId,
                        1,
                        89.7,
                        94.6,
                        25000,
                        800,
                        2750,
                        60,
                        93,
                        0.8,
                        82.1,
                        1.68
                );

                producer.send(new ProducerRecord<>(topic, flightNumber, message));

                System.out.println("engine log data Sent: " + flightNumber);

                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } finally {
            producer.close();
            System.out.println("engine log producer closed ...");
        }
    }
}
