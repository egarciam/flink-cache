package cache.ivr.flink.poc.model;

import java.util.ArrayList;
import java.util.Iterator;


/* Class to represent the inner join between CLIENT and CLIPROD Streams.
 *  The object includes the info from Client plus a list with products IDs.
 */
public class ClientExtended extends Client {

    public ArrayList<Long> products = new ArrayList<Long> ();


    public ClientExtended() {

    }
    public ClientExtended(Client c){
        this.name = c.name;
        this.last_name = c.last_name;
        this.id_client = c.id_client;
        this.ts = c.ts;
    }

    public ClientExtended(CliProd cp){
        this.id_client = cp.id_client;

        this.name = null;
        this.last_name = null;
        this.ts = -1;
    }


    public void updateClientInfo (Client c){
        this.name = c.name;
        this.last_name = c.last_name;
        this.id_client = c.id_client;
        this.ts = c.ts;
    }

    public void updateCliProdInfo (CliProd cp){
        this.products.add(cp.id_product);
        this.ts=cp.ts;
    }

    @Override
    public String toString(){
        String s= super.toString();
        Iterator<Long> itr = this.products.iterator();

        // add the Products ids to the String in json-based format
        Long aux= null;
        int cont=0;

        while (itr.hasNext()) {
            aux = itr.next();
            s+= ", " + "prod"+cont +": " + aux.toString();
            cont++;
        }

        return s;
    }
}
