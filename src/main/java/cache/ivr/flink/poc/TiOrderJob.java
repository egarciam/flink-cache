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

import cache.ivr.flink.poc.model.tiorder.TIOrderEvent;
import cache.ivr.flink.poc.watermarks.TIOrderTimestamp;
import orange.ivr.flink.poc.model.*;
import cache.ivr.flink.poc.operators.map.StringToTIOrderEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class TiOrderJob {

	public static void main(String[] args) throws Exception {

		//AÑADIR EL Nº DE TELEFONO COMO ID DEL REGISTRO Y ALGO DE INFO DEL CLIENTE (generador y pojo)

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


		// kafka configuration
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");


		// DataStream from client topics
		DataStream<TIOrderEvent> tiorder = env
				.addSource(new FlinkKafkaConsumer<>("tiorder", new SimpleStringSchema(), properties))
				.map(new StringToTIOrderEvent())
				.assignTimestampsAndWatermarks(new TIOrderTimestamp());
				//.keyBy(e -> e.data.id_phone);


		tableEnv.registerDataStream("TIOrders", tiorder, "metadata, data, before, striimmetadata, UserActionTime.rowtime");

		String sql= "SELECT tio.data.order_type, tio.data.status, tio.data.technology_origin, tio.data.technology_access, tio.data.d_rfb_date FROM TIOrders tio " +
				"WHERE (tio.data.order_type in ('ALTA') " +
		  	" AND (tio.data.status IN ('PENDIENTE CANCELACION','INVALIDA','REGISTRADA') OR (tio.data.status = 'EN EJECUCION' AND tio.data.d_rfb_date is null)) " +
				"AND tio.data.technology_origin is null " +
				"AND tio.data.technology_access in ('FTTH','FTTH_NEBA','FTTH_XPJ','FTTH_MM','HFC_ONO','FTTH_VOD','FTTH_VULA')) "+
				"OR (tio.data.order_type in ('CAMBIO DE TECNOLOGIA') " +
				"AND (tio.data.status IN ('PENDIENTE CANCELACION','INVALIDA','REGISTRADA') OR (tio.data.status = 'EN EJECUCION' AND tio.data.d_rfb_date is null)) "+
				"AND tio.data.technology_origin in ('FTTH','FTTH_NEBA','FTTH_XPJ','FTTH_MM','HFC_ONO','FTTH_VOD','FTTH_VULA') "+
				"AND tio.data.technology_access in ('FTTH','FTTH_NEBA','FTTH_XPJ','FTTH_MM','HFC_ONO','FTTH_VOD','FTTH_VULA'))";

		System.out.println(sql);
		Table result = tableEnv.sqlQuery(sql);
		DataStream<Row> sql_result = tableEnv.toAppendStream(result, Row.class);

		//FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

		//sql_result.keyBy("phone_id")
		//		.map (new UpdateState())
		//		.addSink new RedisSink<Row>(conf, new RedisClientCache()))

		sql_result.print();



		// execute program
		env.execute("Flink IVR Cache");
	}
}
