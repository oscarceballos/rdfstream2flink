package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.sql.Timestamp;

public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<TripleTS> {
//public class PunctuatedAssigner implements AssignerWithPeriodicWatermarks<TripleTS> {
    public static Long backTimestampEvent = null;
    public static Long backTimestampTrans = null;

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(TripleTS tripleTS, long l) {
        return new Watermark((new Timestamp(System.currentTimeMillis())).getTime());
    }

    @Override
    public long extractTimestamp(TripleTS tripleTS, long l) {
        return getTimestampFromCurrentTimestamp(tripleTS.getTimeStamp());
    }

    private long getTimestampFromCurrentTimestamp(long t) {
        if(backTimestampEvent ==null) backTimestampEvent = t;
        if(backTimestampTrans ==null) backTimestampTrans = (new Timestamp(System.currentTimeMillis())).getTime();

        long diff = backTimestampEvent - t;
        backTimestampTrans += diff;

        backTimestampEvent = t;

        return backTimestampTrans;
    }
}
