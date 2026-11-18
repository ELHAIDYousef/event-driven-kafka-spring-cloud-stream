package ma.enset.springkafka.controller;

import ma.enset.springkafka.event.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author Yousef ELHAID
 */
@RestController
public class PageEventController {

    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;

    public PageEventController(StreamBridge streamBridge, InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name, @RequestParam String topic) {
        PageEvent pageEvent = new PageEvent(
                name,
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                10 + new Random().nextInt(1000));
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> {
                    Map<String, Long> stats = new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore =
                            interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.windowStore());
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);
                    try (KeyValueIterator<Windowed<String>, Long> iterator = windowStore.fetchAll(from, now)) {
                        while (iterator.hasNext()) {
                            KeyValue<Windowed<String>, Long> next = iterator.next();
                            stats.put(next.key.key(), next.value);
                        }
                    }
                    return stats;
                });
    }
}