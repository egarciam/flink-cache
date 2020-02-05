package cache.ivr.flink.poc.operators.map;

import cache.ivr.flink.poc.model.CliProd;
import org.apache.flink.api.common.functions.MapFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class StringToCliProd implements MapFunction<String, CliProd> {
    @Override
    public CliProd map(String s) throws Exception {

        CliProd cp = new CliProd();
        JSONParser parser = new JSONParser();
        JSONObject ob= (JSONObject) parser.parse(s);

        cp.ts= (long) ob.get("timestamp");
        cp.id_product = (Long) ob.get("id_product");
        cp.id_client = (Long) ob.get("id_client");

        return cp;

    }
}
