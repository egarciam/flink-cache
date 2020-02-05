
package cache.ivr.flink.poc.model.tiorder;


import org.apache.commons.lang3.builder.ToStringBuilder;

public class Striimmetadata {

    public Object position;

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("position", position).toString();
    }

}
