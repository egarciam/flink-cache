package cache.ivr.flink.poc.operators.map;

import cache.ivr.flink.poc.model.Product;
import org.apache.flink.api.common.functions.MapFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class StringToProduct implements MapFunction<String, Product> {
    @Override
    public Product map(String s) throws Exception {
        Product p = new Product();
        JSONParser parser = new JSONParser();
        JSONObject ob= (JSONObject) parser.parse(s);

        p.description = (String) ob.get("description");
        p.ts= (long) ob.get("timestamp");
        p.name = (String) ob.get("name");
        p.id_product = (Long) ob.get("id_product");

        return p;
    }
}
