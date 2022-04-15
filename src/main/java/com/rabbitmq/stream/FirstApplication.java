package com.rabbitmq.stream;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class FirstApplication {

   public static final String STREAM_NAME = "first-application-stream";

   public static class Publish {

      public static void main(String[] args) throws Exception {
         log("Connecting...");
         try (Environment environment = createEnvironment()) {
            log("Connected");

            log("Creating stream...");
            environment.streamCreator()
                  .maxAge(Duration.ofMinutes(90))
                  .maxLengthBytes(ByteCapacity.MB(100))
                  .stream(STREAM_NAME)
                  .create();
            log("Stream created");

            log("Creating producer...");
            Producer producer = environment.producerBuilder()
                  .stream(STREAM_NAME)
                  .build();
            log("Producer created");

            long start = System.currentTimeMillis();
            int messageCount = 10;
            CountDownLatch confirmLatch = new CountDownLatch(messageCount);
            log("Sending %,d messages", messageCount);

            IntStream.range(0, messageCount).forEach(i -> {

               Message message = producer.messageBuilder()
                     .properties()
                     .creationTime(5)
                     .messageId(i)
                     .messageBuilder()
                     .addData((LocalTime.now() + " hello world").getBytes(StandardCharsets.UTF_8))
                     .build();

               producer.send(message, confirmationStatus -> confirmLatch.countDown());
            });
            log("Messages sent, waiting for confirmation...");
            boolean done = confirmLatch.await(1, TimeUnit.MINUTES);
            log(
                  "All messages confirmed? %s (%d ms)",
                  done ? "yes" : "no", (System.currentTimeMillis() - start));
            log("Closing environment...");
         }
         log("Environment closed");
      }
   }

   public static class Consume {

      public static void main(String[] args) {
         log("Connecting...");
         try (Environment environment = createEnvironment()) {
            log("Connected");

            AtomicInteger messageConsumed = new AtomicInteger(0);
            long start = System.currentTimeMillis();

            Instant hourAgo = Instant.now().minus(Duration.ofHours(1));

            log("Start consumer...");
            environment.consumerBuilder()
                  .stream(STREAM_NAME)
                  .offset(OffsetSpecification.timestamp(hourAgo.toEpochMilli()))
                  .messageHandler((context, message) -> {
                     int count = messageConsumed.incrementAndGet();
                     System.out.println(count + " " + new String(message.getBodyAsBinary()));
                  })
                  .build();

//            Utils.waitAtMost(65, () -> messageConsumed.get() >= 10);
            log(
                  "Consumed %,d messages in %s ms",
                  messageConsumed.get(), (System.currentTimeMillis() - start));
            log("Closing environment...");
         }
         log("Environment closed");
      }
   }

   static void log(String format, Object... arguments) {
      System.out.println(String.format(format, arguments));
   }

   private static Environment createEnvironment() {
      return Environment.builder()//.uri("rabbitmq-stream://localhost:5552")
            .host("localhost")
            .virtualHost("my-vhost")
            .port(5552)
            .username("guest")
            .password("guest")
            .build();
   }
}
