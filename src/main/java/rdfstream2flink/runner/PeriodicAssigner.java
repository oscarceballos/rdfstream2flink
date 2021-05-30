package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.sql.Timestamp;

/**
 *  An assigner with periodic timestamps that produces watermarks by keeping track of the maximum element timestamp
 *  it has seen so far. When asked for a new watermark, the assigner returns a watermark with the maximum timestamp
 *  minus a 1-minute tolerance interval
 */

public class PeriodicAssigner implements AssignerWithPeriodicWatermarks<TripleTS> {

    public static Long bound = Long.valueOf(30 * 1000); //60 * 1000
    public static Long maxTs = 18001000L; //(new Timestamp(System.currentTimeMillis())).getTime(); //Long.MIN_VALUE; // the maximum observed timestamp

    @Override
    public Watermark getCurrentWatermark() {
        // generated watermark with 1 sec tolerance
        return new Watermark(maxTs - bound);
    }

    @Override
    public long extractTimestamp(TripleTS tripleTS, long l) {
        // update maximum timestamp
        maxTs = Long.max(tripleTS.getTimeStamp(), maxTs);
        // return record timestamp
        return tripleTS.getTimeStamp();
    }
}
