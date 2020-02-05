/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache.ivr.flink.poc;

import cache.ivr.flink.poc.model.CliProd;
import cache.ivr.flink.poc.model.Client;
import cache.ivr.flink.poc.model.Product;
import cache.ivr.flink.poc.watermarks.ClientTimestamp;
import orange.ivr.flink.poc.model.*;
import cache.ivr.flink.poc.operators.map.StringToCliProd;
import cache.ivr.flink.poc.operators.map.StringToClient;
import cache.ivr.flink.poc.operators.map.StringToProduct;
import cache.ivr.flink.poc.watermarks.CliProTimestamp;
import cache.ivr.flink.poc.watermarks.ProductTimestamp;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * PoC to evaluate real-time generation of Clients 360º cache into Redis joining client-producs events from Kafka
 */

public class SQLJob {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		env.setParallelism(4);

		// kafka configuration
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");


		// DataStream from client topics
		DataStream<Client> clients = env
				.addSource(new FlinkKafkaConsumer<>("clientes", new SimpleStringSchema(), properties))
				.map(new StringToClient())
				.assignTimestampsAndWatermarks(new ClientTimestamp());

		tableEnv.registerDataStream("Clientes", clients, "id_client, name, last_name, ts, UserActionTime.rowtime");


		// DataStream from Cli-Prod topics
		DataStream<CliProd> cli_prods = env
				.addSource(new FlinkKafkaConsumer<>("cli_prod", new SimpleStringSchema(), properties))
				.map(new StringToCliProd())
				.assignTimestampsAndWatermarks(new CliProTimestamp());
				//.keyBy(e -> e.id_client);

		tableEnv.registerDataStream("CliProd", cli_prods, "id_client, id_product, ts, UserActionTime.rowtime");


		// DataStream from Products topics
		DataStream<Product> products = env
				.addSource(new FlinkKafkaConsumer<>("productos", new SimpleStringSchema(), properties))
				.map(new StringToProduct())
				.assignTimestampsAndWatermarks(new ProductTimestamp());
				//.keyBy(e -> e.id_product);

		tableEnv.registerDataStream("Productos", products, "id_product, name, description, ts, UserActionTime.rowtime");


		String sql= "SELECT c.id_client, c.name, cp.id_product, p.name, p.description, c.ts, cp.ts, p.ts " +
					"FROM Clientes c JOIN CliProd cp ON c.id_client = cp.id_client " +
									"JOIN  Productos p ON cp.id_product = p.id_product";


		Table result = tableEnv.sqlQuery(sql);


		// convert the Table into a retract DataStream of Row.
		//   A retract stream of type X is a DataStream<Tuple2<Boolean, X>>.
		//   The boolean field indicates the type of the change.
		//   True is INSERT, false is DELETE.

		DataStream<Tuple2<Boolean, Row>> sql_result = tableEnv.toRetractStream(result, Row.class);

		//sql_result
		//		.filter(t -> t.f0==true)
		//		.map (t -> t.f1)
		//		.print();
		sql_result.print();


		/*
		// DataStream from client products. Broadcas since no many changes are expected.
		MapStateDescriptor<Long, Product> rulesStateDescriptor = new MapStateDescriptor<Long, Product>(
				"ProductsBroadcastState",
				Long.class,
				Product.class
		);
		BroadcastStream<Product> products = env
				.addSource(new FlinkKafkaConsumer<>("productos", new SimpleStringSchema(), properties))
				.map(new StringToProduct())
				.assignTimestampsAndWatermarks(new ProductTimestamp())
				.broadcast(rulesStateDescriptor);


		// Improvement 1: serialize from kafka to DataModel in the FlinkKafkaConsumer


		// Connect Client and Cli-Prod streams to create ClientExtended view
		DataStream<ClientExtended> clientsExtended= clients.connect(cli_prods)
				.keyBy("id_client", "id_client")
				.flatMap(new CoFlatMapClientExtended());

		// Connect ClienteExtended with Product streams to create final ClientCache view
		DataStream<ClientCache> clientcache= clientsExtended
				.keyBy("id_client")
				.connect(products)
				.process(new BroadCastClientCache());



		// Sink to Redis
		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
		clientcache.addSink(new RedisSink<ClientCache>(conf, new RedisClientCache()));

		clientcache.print();

*/


		// execute program
		env.execute("Flink IVR Cache");
	}
}
