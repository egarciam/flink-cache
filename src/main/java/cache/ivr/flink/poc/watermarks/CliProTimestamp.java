package cache.ivr.flink.poc.watermarks;

import cache.ivr.flink.poc.model.CliProd;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CliProTimestamp implements AssignerWithPeriodicWatermarks<CliProd>{

    private final long maxTimeLag = 2000; // 2 segundos

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }

    @Override
    public long extractTimestamp(CliProd cliProd, long l) {
        return cliProd.ts;
    }
}
