package cache.ivr.flink.poc.model;

public class CliProd {

    public long ts;
    public Long id_client;
    public Long id_product;

    @Override
    public String toString() {
        return this.ts + " "+ this.id_client + " " + this.id_product;
    }
}


