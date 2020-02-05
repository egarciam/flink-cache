package cache.ivr.flink.poc.operators.map;

import cache.ivr.flink.poc.model.Client;
import org.apache.flink.api.common.functions.MapFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class StringToClient implements MapFunction<String, Client> {
    @Override
    public Client map(String s) throws Exception {
        Client c = new Client();
        JSONParser parser = new JSONParser();
        JSONObject ob= (JSONObject) parser.parse(s);

        c.ts= (long) ob.get("timestamp");
        c.name = (String) ob.get("name");
        c.last_name = (String) ob.get("lastName");
        c.id_client = (Long) ob.get("id_client");

        return c;

    }
}
