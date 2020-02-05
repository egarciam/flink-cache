package cache.ivr.flink.poc.model;

public class Product {

    public Long id_product;
    public long ts;
    public String name;
    public String description;

    public Product () {

    }

    @Override
    public String toString() {
        return "{ "+
                "timestamp: " + this.ts + ", " +
                "id_product: "+ this.id_product + ", " +
                "name: " + this.name + ", " +
                "description: " + this.description +
                "}";
    }

    public Product (Long i) {
        this.id_product=i;
    }

    public boolean equals(Long obj) {
        return this.id_product==obj;
    }

    public boolean equals(Product obj) {
        return this.id_product==obj.id_product;
    }
}
