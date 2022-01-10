
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

public class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Map<String,Object>> {
    public TimestampExtractor() {
        super(Time.seconds(10));
    }

    @Override
    public long extractTimestamp(Map<String,Object> ride) {
        long timestamp = Long.parseLong((String)ride.get("orderid"));
        return timestamp;
    }

}
