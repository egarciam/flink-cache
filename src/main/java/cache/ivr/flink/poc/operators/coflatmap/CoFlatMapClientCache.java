package cache.ivr.flink.poc.operators.coflatmap;

import cache.ivr.flink.poc.model.ClientExtended;
import cache.ivr.flink.poc.model.ClientCache;
import cache.ivr.flink.poc.model.Product;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class CoFlatMapClientCache extends RichCoFlatMapFunction<ClientExtended, Product, ClientCache> {


    private ValueState<ClientCache> lastVal;

    @Override
    public void open(Configuration conf) {
        ValueStateDescriptor<ClientCache> valueDesc =
                new ValueStateDescriptor<ClientCache>("table", ClientCache.class);
        lastVal = getRuntimeContext().getState(valueDesc);
    }

    @Override
    public void flatMap1(ClientExtended ce, Collector<ClientCache> collector) throws Exception {

        ClientCache lookup = lastVal.value();
        //check if the client already exist in the internal state.
        if (lookup == null) {
            ClientCache aux = new ClientCache(ce);
            //create current client (only "Client", without Products)
            lastVal.update(aux);
            collector.collect(aux);
        }
        else{
            //update existing ClientExtended with the updated info from Client
            lookup.updateClientExtendedInfo(ce);
            lastVal.update(lookup);
            collector.collect(lookup);
        }

    }

    @Override
    public void flatMap2(Product product, Collector<ClientCache> collector) throws Exception {

        ClientCache lookup = lastVal.value();
        //check if the client already exist in the internal state. If there is no client, no updated about products
        // are required
        if (lookup == null) { return;}

        else{
            //check if the client has this Product in their profile and update the product Info
            Product p = lookup.getProductInfo(product);
            if (p!= null) {
                lookup.updateProductInfo(p);
                lastVal.update(lookup);
                collector.collect(lookup);
            }
            else { return;} // the client does not have that product
  }

    }
}
