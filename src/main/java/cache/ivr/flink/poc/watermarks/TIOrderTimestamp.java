package cache.ivr.flink.poc.watermarks;

import cache.ivr.flink.poc.model.tiorder.TIOrderEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TIOrderTimestamp implements AssignerWithPeriodicWatermarks<TIOrderEvent> {

    private final long maxTimeLag = 2000; // 2 segundos


    @Override
    public long extractTimestamp(TIOrderEvent e, long l) {
        return Long.parseLong(e.metadata.DBCommitTimestamp);
    }


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }

}