package cache.ivr.flink.poc.watermarks;

import cache.ivr.flink.poc.model.Product;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class ProductTimestamp implements AssignerWithPeriodicWatermarks<Product>{

    private final long maxTimeLag = 2000; // 2 segundos

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }

    @Override
    public long extractTimestamp(Product product, long l) {
        return product.ts;
    }
}
