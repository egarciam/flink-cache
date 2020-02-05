package cache.ivr.flink.poc.model;

public class Client {

    public Long id_client;
    public String name;
    public String last_name;
    public long ts;


    @Override
    public String toString() {
        return "timestamp: " + this.ts + ", "
                + "id_client: "+ this.id_client + ","
                + "name: " + this.name + ","
                + "last_name: " + this.last_name;
    }
}
