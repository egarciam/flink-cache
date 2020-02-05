
package cache.ivr.flink.poc.model.tiorder;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class Before {

    public String order_type;
    public String status;
    public Object d_rfb_date;
    public Object technology_origin;
    public String technology_access;

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("order_type", order_type).append("status", status).append("d_rfb_date", d_rfb_date).append("technology_origin", technology_origin).append("technology_access", technology_access).toString();
    }

}
