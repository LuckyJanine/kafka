package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;


public class AircraftFlightProducer {
    public static void main(String[] args) throws InterruptedException {
        String topic = "flight-atc";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        int[] numbers = {1549, 143, 32, 751, 780, 236};
        String[] ids = {"A320-214/N106US", "B767-233/C-GAUN", "A380-842/VH-OQA",
                        "MD-81/OY-KHO", "A330-342/B-HLL", "A330-243/C-GITS"};

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        while(true) {

            // long createTime = System.currentTimeMillis();

            int currentAircraftIndex = random.nextInt(numbers.length);
            String flightNumber = "Flight-" + numbers[currentAircraftIndex];
            String aircraftId = ids[currentAircraftIndex];
            double latitude = 30 + random.nextDouble() * 40;
            double longitude = -100 + random.nextDouble() * 80;
            int altitude = 30000 + random.nextInt(10000);
            int groundSpeed = 400 + random.nextInt(200);
            int airSpeed = 350 + random.nextInt(200);
            int heading = random.nextInt(360); // from true north
            int remainingFuel = 1500 + random.nextInt(115000); //kg (min value around 2000 liters)
            AircraftConfiguration aircraftConfiguration = AircraftConfiguration.currentConfiguration();

            String message = String.format(
                            "\"flight number\": \"%s\"%n" +
                            "\"aircraft id\": %s%n" +
                            "-------------------------%n" +
                            "------------------------------------------------------------%n" +
                            "\"latitude\": %.2f, \"longitude\": %.2f, \"altitude\": %d%n" +
                            "\"ground speed\": %d knots%n" +
                            "\"air speed\": %d knots%n" +
                            "\"heading\": %d%n" +
                            "\"remaining fuel\": %d%n" +
                            "\"flap\": %d, " +
                            "\"slat\": %d, " +
                            "\"gear\": %d%n" +
                            "---------------------------------------" +
                            "PIREP (Pilot Weather Report):%n" +
                            "\"wind\": %s%n" +
                            "\"wind shear\": %s%n" +
                            "\"TB (turbulence)\": %s%n" +
                            "\"cloud cover\": %s%n" +
                            "\"visibility\": %d+ km%n" +
                            "\"icing\": %s%n" +
                            "\"barometric pressure\": %d hPa%n" +
                            "----------------------------------------" +
                            "AP (Autopilot) status:%n" +
                            "[2025-02-10 09:05:00] INFO AUTOPILOT: %s%n" +
                            "[2025-02-10 09:05:00] INFO AUTOPILOT: Mode - %s%n",
                    flightNumber, aircraftId,
                    latitude, longitude, altitude,
                    groundSpeed, airSpeed, heading, remainingFuel,
                    aircraftConfiguration.flap,
                    aircraftConfiguration.slat,
                    aircraftConfiguration.gear,
                    "180° at 15 knots",
                    "Smooth wind profile",
                    "None",
                    "Sky clear",
                    10,
                    "None",
                    1000,
                    "Engaged",
                    "Altitude Hold"
                    );

            producer.send(new ProducerRecord<>(topic, flightNumber, message));

            System.out.println("atc data Sent for: " + flightNumber);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
                producer.close();
                throw new InterruptedException();
            }
        }
    }

    record AircraftConfiguration (int flap, int slat, int gear) {
        public static AircraftConfiguration currentConfiguration(){
            Random random = new Random();
            int config = random.nextDouble() < 0.95 ? 0 : 1;
            // 1 - deployed; 0 - not
            int flap = config;
            int slat = config;
            int gear = config;
            return new AircraftConfiguration(flap, slat, gear);
        }
    }
}
