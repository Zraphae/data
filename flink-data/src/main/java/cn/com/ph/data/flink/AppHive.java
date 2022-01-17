package cn.com.ph.data.flink;

import cn.com.ph.data.flink.common.model.OGGMessage;
import cn.com.ph.data.flink.schemas.OGGMessageSchema;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AppHive {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String appName = params.get("app.name", "test");
        String checkPointPath = params.get("check.point.path", "hdfs:///tmp/");
        long flinkCheckPointInterval = params.getLong("flink.checkpoint.interval", 5 * 1000L);
        long flinkWindowInterval = params.getLong("flink.window.interval", 20);
        String primaryKeyName = params.get("primary.key.name", "id");

        String readTopic = params.get("read.topic");
        String readBootstrapServers = params.get("read.bootstrap.servers");
        String readGroupName = params.get("read.group.id", "synch_group");
        int kafkaPartitionNum = params.getInt("kafka.partition.num",2);
        long startupOffsetsTimestamp = params.getLong("startup.offsets.timestamp", 0L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(flinkCheckPointInterval);

        env.setStateBackend(new RocksDBStateBackend(checkPointPath, true));
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000L);

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getConfig()
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10 * 1000L));

//        Properties kafkaPro = new Properties();
//        kafkaPro.setProperty("bootstrap.servers", readBootstrapServers);
//        kafkaPro.setProperty("group.id", readGroupName);
//        FlinkKafkaConsumer<OGGMessage> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
//                readTopic, new OGGMessageSchema(), kafkaPro
//        );
//
//        flinkKafkaConsumer.setStartFromTimestamp(startupOffsetsTimestamp);

//        DataStreamSource<OGGMessage> stream = env.addSource(flinkKafkaConsumer).setParallelism(kafkaPartitionNum);
//        SingleOutputStreamOperator<List<OGGMessage>> apply = stream
//                .keyBy((KeySelector<OGGMessage, String>) oggMessage -> (String.valueOf(oggMessage.getPartition())))
//                .window(TumblingEventTimeWindows.of(Time.seconds(flinkWindowInterval)))
//                .apply(new WindowFunction<OGGMessage, List<OGGMessage>, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow window, Iterable<OGGMessage> input, Collector<List<OGGMessage>> out) {
//                        ArrayList<OGGMessage> oggMessages = Lists.newArrayList();
//                        input.forEach(oggMessage -> oggMessages.add(oggMessage));
//                        out.collect(oggMessages);
//                    }
//                }).setParallelism(kafkaPartitionNum);
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/zhaopeng/Soft/hive/conf"; // hive配置文件地址
        String version = "3.1.1";

        Catalog catalog = new HiveCatalog(name,defaultDatabase, hiveConfDir, version);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        TableEnvironment tEnv = TableEnvironment.create(bsSettings);

        tEnv.registerCatalog("myhive", catalog);
        tEnv.useCatalog("myhive");

        Table table = tEnv.sqlQuery("select * from test");
        table.printSchema();

        env.execute();
    }
}
