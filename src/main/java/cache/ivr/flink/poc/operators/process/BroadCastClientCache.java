package cache.ivr.flink.poc.operators.process;

import cache.ivr.flink.poc.model.ClientExtended;
import cache.ivr.flink.poc.model.ClientCache;
import cache.ivr.flink.poc.model.Product;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;


public class BroadCastClientCache extends KeyedBroadcastProcessFunction<Long, ClientExtended, Product, ClientCache> {

    private final ValueStateDescriptor<ClientCache> valueDesc = new ValueStateDescriptor<ClientCache>("table", ClientCache.class);

    private final MapStateDescriptor<Long, Product> broadcasStateDescriptor = new MapStateDescriptor<Long, Product>(
            "ProductsBroadcastState",
            Long.class,
            Product.class
    );

    @Override
    public void processElement(ClientExtended ce, ReadOnlyContext ctx, Collector<ClientCache> out) throws Exception {

        //check if the client already exist in the internal state.
        ValueState<ClientCache> lastVal = getRuntimeContext().getState(valueDesc);
        ClientCache lookup = lastVal.value();

        if (lookup == null)
            lookup = new ClientCache(ce);
        else
            lookup.updateClientExtendedInfo(ce);

        //products_id of the client
        Iterator<Product> itr= lookup.products.iterator();
        itr = new ArrayList<Product>(lookup.products).iterator();
        Product client_product =null;
        Product catalog_product = null;

        // obtain extended and updated info from the products catalog
        while (itr.hasNext()){
            client_product = itr.next();
            catalog_product= ctx.getBroadcastState(broadcasStateDescriptor).get(client_product.id_product);
            if (catalog_product!= null)
                lookup.updateProductInfo(catalog_product);

        }

        //update new version of Client Cache in internal State
        lastVal.update(lookup);
        out.collect(lookup);

    }

    @Override
    public void processBroadcastElement(Product p, Context ctx, Collector<ClientCache> out) throws Exception {

        ctx.getBroadcastState(broadcasStateDescriptor).remove(p.id_product);
        ctx.getBroadcastState(broadcasStateDescriptor).put(p.id_product, p);

        // TO DO
        // update existing users in the case of an updated information of product their have

    }
}
