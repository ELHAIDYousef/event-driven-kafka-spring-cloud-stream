package ma.enset.springkafka.handler;

import ma.enset.springkafka.event.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

/**
 *
 * @author Yousef ELHAID
 */
@Component
public class PageEventHandler {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return input -> {
            System.out.println("************");
            System.out.println(input);
            System.out.println("************");
        };
    }
}