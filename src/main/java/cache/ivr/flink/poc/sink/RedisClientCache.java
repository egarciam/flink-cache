package cache.ivr.flink.poc.sink;

import cache.ivr.flink.poc.model.ClientCache;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisClientCache implements RedisMapper<ClientCache> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET, "HASH_NAME");
    }

    @Override
    public String getKeyFromData(ClientCache data) {
        return data.id_client.toString();
    }

    @Override
    public String getValueFromData(ClientCache data) {
        return data.toString();
    }

}
