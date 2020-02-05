package cache.ivr.flink.poc.operators.coflatmap;

import cache.ivr.flink.poc.model.CliProd;
import cache.ivr.flink.poc.model.ClientExtended;
import cache.ivr.flink.poc.model.Client;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class CoFlatMapClientExtended extends RichCoFlatMapFunction<Client, CliProd, ClientExtended> {


    private ValueState<ClientExtended> lastVal;

    @Override
    public void open(Configuration conf) {
        ValueStateDescriptor<ClientExtended> valueDesc =
                new ValueStateDescriptor<ClientExtended>("table", ClientExtended.class);
        lastVal = getRuntimeContext().getState(valueDesc);
    }

    @Override
    public void flatMap1(Client client, Collector<ClientExtended> collector) throws Exception {

        ClientExtended lookup = lastVal.value();
        //check if the client already exist in the internal state.
        if (lookup == null) {
            ClientExtended aux = new ClientExtended(client);
            //create current client (only "Client", without Products)
            lastVal.update(aux);
            collector.collect(aux);
        }
        else{
            //update existing ClientExtended with the updated info from Client
            lookup.updateClientInfo(client);
            lastVal.update(lookup);
            collector.collect(lookup);
        }

    }

    @Override
    public void flatMap2(CliProd cliProd, Collector<ClientExtended> collector) throws Exception {
        ClientExtended lookup = lastVal.value();
        //check if the client already exist in the internal state.
        if (lookup == null) {
            ClientExtended aux = new ClientExtended(cliProd);
            //create current client (only "CliProd", without Client info yet)
            lastVal.update(aux);
            collector.collect(aux);
        }
        else{
            //update existing ClientExtended with the updated info from CliProd
            lookup.updateCliProdInfo(cliProd);
            lastVal.update(lookup);
            collector.collect(lookup);
        }

    }
}
