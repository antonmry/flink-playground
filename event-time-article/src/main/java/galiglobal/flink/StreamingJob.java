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

package galiglobal.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private SourceFunction<Long> source;
    private SinkFunction<Long> sink;

    public StreamingJob(SourceFunction<Long> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {
        Properties props = new Properties();
        props.put("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory");
        Configuration conf = ConfigurationUtils.createConfiguration(props);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> longStream =
            env.addSource(source)
                .returns(TypeInformation.of(Long.class));
        longStream
            .map(new IncrementMapFunction())
            .addSink(sink);

        LOG.debug("Start Flink example job");

//        DataStreamSink<Long> logTestStream = env.fromElements(0L, 1L, 2L)
//            .map(new IncrementMapFunction())
//            .addSink(sink);

        LOG.debug("Stop Flink example job");


        env.execute();
    }

    public static void main(String[] args) throws Exception {
        StreamingJob job = new StreamingJob(new RandomLongSource(), new PrintSinkFunction<>());
        job.execute();
    }

}