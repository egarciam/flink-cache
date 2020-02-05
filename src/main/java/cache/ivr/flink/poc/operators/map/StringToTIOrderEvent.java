package cache.ivr.flink.poc.operators.map;

import cache.ivr.flink.poc.model.tiorder.TIOrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;

public class StringToTIOrderEvent implements MapFunction<String, TIOrderEvent> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public TIOrderEvent map(String s) throws Exception {

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        TIOrderEvent event = null;
        try {
            event = mapper.readValue(s, TIOrderEvent.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return event;
    }
}
