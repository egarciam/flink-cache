package cache.ivr.flink.poc.model;

import java.util.ArrayList;
import java.util.Iterator;

public class ClientCache extends Client {

    public ArrayList<Product> products = new ArrayList<Product> ();

    public ClientCache (ClientExtended ce) {
        this.name= ce.name;
        this.last_name = ce.last_name;
        this.ts = ce.ts;
        this.id_client = ce.id_client;

        Iterator<Long> itr = ce.products.iterator();
        Long aux= null;
        while (itr.hasNext()) {
            aux= itr.next();
            this.products.add(new Product (aux));
        }
    }

    public void updateClientExtendedInfo(ClientExtended ce){

        //client management
        this.name= ce.name;
        this.last_name = ce.last_name;
        this.ts = ce.ts;
        this.id_client = ce.id_client;

        //products management
        Iterator<Product> itr1 = this.products.iterator();
        itr1 = new ArrayList<Product>(this.products).iterator();


        Iterator<Long> itr2= ce.products.iterator();

        // there is no change in the products
        if (this.products.size()== ce.products.size()) { return;}

        //some product has been deleted
        if (this.products.size() > ce.products.size()) {
            Product aux= null;
            while (itr1.hasNext()) {
                aux= itr1.next();
                if (!ce.products.contains(aux))
                    this.products.remove(aux);
            }
            return;

        }

        //some product has been added
        if (this.products.size() < ce.products.size()) {
            Long aux= null;
            while (itr2.hasNext()) {
                aux= itr2.next();
                if (!this.products.contains(aux))
                    this.products.add(new Product (aux));
            }
            return;

        }

    }

    public Product getProductInfo (Product p){
        Product pro = null;

        Iterator<Product> itr = this.products.iterator();
        while (itr.hasNext()){
            pro = itr.next();
            if (pro.equals( p))
                return pro;
        }
        return null;
    }

    public Product getProductInfo (Long id_product){
        Product pro = null;

        Iterator<Product> itr = this.products.iterator();
        while (itr.hasNext()){
            pro = itr.next();
            if (pro.equals(id_product))
                return pro;
        }
        return null;
    }

    public void updateProductInfo (Product p) {
        this.products.remove(p); //remove old version of the product (equals() is based only the id_product
        this.products.add (p); // add the new version of the product
    }

    @Override
    public String toString() {

        String s= "{" + super.toString();

        s+=", products: [";
        Iterator<Product> itr = this.products.iterator();
        if (itr!=null && itr.hasNext()) {
            Product aux= null;
            aux= itr.next();
            if (aux!=null)
                s+=aux.toString();
            while (itr.hasNext()) {
                aux= itr.next();
                s+=", "+ aux.toString();
            }
            s+="]";
        }
        s+="}";
        return s;
    }
}
