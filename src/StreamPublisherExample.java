import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.workflowstreams.internal.StreamPublisher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StreamPublisherExample {

    public static void main(String[] args) {
        // 1. In-memory storage to capture signals for verification
        List<Object> capturedBatches = Collections.synchronizedList(new ArrayList<>());

        // 2. Implement the mock SignalFunction
        // In production, this would signal a WorkflowClient stub.
        StreamPublisher.SignalFunction mockSignalFunction = (batch) -> {
            System.out.printf("[Mock Signal] Delivering batch to workflow: %s%n", batch);
            capturedBatches.add(batch);
        };

        // 3. Configure publisher parameters
        Duration batchInterval = Duration.ofMillis(500); // Max wait time before flushing
        int maxBatchSize = 3;                            // Trigger flush after 3 items
        Duration maxRetryDuration = Duration.ofSeconds(5);

        // 4. Initialize StreamPublisher
        StreamPublisher publisher = new StreamPublisher(
            mockSignalFunction,
            DefaultDataConverter.newDefaultInstance(),
            batchInterval,
            maxBatchSize,
            maxRetryDuration
        );

        try {
            System.out.println("--- Publishing first 2 items (below maxBatchSize) ---");
            publisher.publish("events-topic", "Event-1", false);
            publisher.publish("events-topic", "Event-2", false);

            System.out.println("--- Publishing 3rd item (triggers maxBatchSize flush) ---");
            // maxBatchSize of 3 is reached here, triggering a batch send
            publisher.publish("events-topic", "Event-3", false);

            System.out.println("--- Publishing 4th item with forceFlush=true ---");
            // Immediately flushes regardless of batch count or interval
            publisher.publish("events-topic", "Event-4", true);

            System.out.println("--- Publishing 5th item and calling explicit flush() ---");
            publisher.publish("events-topic", "Event-5", false);
            publisher.flush(); // Blocks until confirmed

        } catch (Exception e) {
            System.err.println("Publish error: " + e.getMessage());
        } finally {
            // 5. Close publisher to stop background loops and drain any remaining items
            publisher.close();
            System.out.println("Publisher closed. Total batches received by mock: " + capturedBatches.size());
        }
    }
}
