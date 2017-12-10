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

package com.inatel.demos;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class ChangesGear {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env
        .addSource(new FlinkKafkaConsumer09<>("flink-demo", new JSONDeserializationSchema(), properties));

    stream.flatMap(new TelemetryJsonParser()).keyBy(0).timeWindow(Time.seconds(3)).reduce(new ChangesGearReducer())
        .flatMap(new ChangesGearMapper()).map(new ChangesGearPrinter()).print();

    env.execute();
  }

  static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, Integer, Integer>> {
    @Override
    public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
      String carNumber = "car" + jsonTelemetry.get("Car").asText();
      Integer gear = jsonTelemetry.get("telemetry").get("Gear").intValue();
      out.collect(new Tuple3<>(carNumber, gear, 0));
    }
  }

  static class ChangesGearReducer implements ReduceFunction<Tuple3<String, Integer, Integer>> {
    @Override
    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1,
        Tuple3<String, Integer, Integer> value2) {
      if (value1.f1 != value2.f1) {
        return new Tuple3<>(value1.f0, value2.f1, value1.f2 + 1);
      } else {
        return new Tuple3<>(value1.f0, value1.f1, value1.f2);
      }
    }
  }

  static class ChangesGearMapper implements FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> {
    @Override
    public void flatMap(Tuple3<String, Integer, Integer> carInfo, Collector<Tuple2<String, Integer>> out)
        throws Exception {
      out.collect(new Tuple2<>(carInfo.f0, carInfo.f2));
    }
  }

  static class ChangesGearPrinter implements MapFunction<Tuple2<String, Integer>, String> {
    @Override
    public String map(Tuple2<String, Integer> changesEntry) throws Exception {
      return String.format("Changes of gear for the %s: %d", changesEntry.f0, changesEntry.f1);
    }
  }

}
