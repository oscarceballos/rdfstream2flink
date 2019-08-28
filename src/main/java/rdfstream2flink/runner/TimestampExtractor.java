package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.sql.Timestamp;

public class TimestampExtractor implements AssignerWithPunctuatedWatermarks<TripleTS> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(TripleTS tripleTS, long l) {
        return new Watermark((new Timestamp(System.currentTimeMillis())).getTime());
    }

    @Override
    public long extractTimestamp(TripleTS tripleTS, long l) {
        return tripleTS.getTimeStamp();
    }
}
