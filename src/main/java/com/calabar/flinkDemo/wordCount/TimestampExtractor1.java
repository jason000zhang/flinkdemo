package com.calabar.flinkDemo.wordCount;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

public class TimestampExtractor1 extends BoundedOutOfOrdernessTimestampExtractor<Map<String,Object>> {
    public TimestampExtractor1() {
        super(Time.seconds(10));
    }

    @Override
    public long extractTimestamp(Map<String,Object> ride) {
        long timestamp = (Long)ride.get("orderid");
        return timestamp;
    }

}
