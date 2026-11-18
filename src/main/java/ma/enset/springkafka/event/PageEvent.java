package ma.enset.springkafka.event;

import java.util.Date;

/**
 *
 * @author Yousef ELHAID
 */
public record PageEvent(String name, String user, Date date, long duration) {
}