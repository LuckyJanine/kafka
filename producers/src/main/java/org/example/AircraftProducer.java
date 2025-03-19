package org.example;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AircraftProducer {
    protected static volatile boolean running = true;
    public static void main(String[] args) {
        System.out.println("Starting up producers ...");

        ExecutorService executorService = Executors.newFixedThreadPool(3); // 3 pool threads

        // use ExecutorService.submit() instead? and future.get() to pause main thread
        executorService.execute(() -> AircraftFlightProducer.main(args));
        executorService.execute(() -> EngineLogProducer.main(args));
        executorService.execute(() -> HydraulicLogProducer.main(args));

        // JVM runs shutdown thread for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try{
                executorService.shutdown(); // executor stops accepting new task
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) { // if all tasks finish within ...
                    // sends interruption signal to all worker threads
                    // but doesn't guarantee termination
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }

            System.out.println("All producers stopped.");
        }));

        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Main thread interrupted.");
        }

        System.out.println("Main thread exiting.");
    }
}
