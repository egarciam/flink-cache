
package cache.ivr.flink.poc.model.tiorder;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class TIOrderEvent {

    public Metadata metadata;
    public Data data;
    public Before before;
    public Object userdata;
    public Striimmetadata striimmetadata;

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("metadata", metadata).append("data", data).append("before", before).append("userdata", userdata).append("striimmetadata", striimmetadata).toString();
    }

}
