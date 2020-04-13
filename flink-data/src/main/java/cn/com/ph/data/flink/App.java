package cn.com.ph.data.flink;

import cn.com.ph.data.flink.common.model.OGGMessage;
import cn.com.ph.data.flink.common.utils.ElasticSearchSinkUtil;
import cn.com.ph.data.flink.es.ESSinkFunction;
import cn.com.ph.data.flink.es.RetryRejectedExecutionFailureHandler;
import cn.com.ph.data.flink.hbase.HBaseWriter4J;
import cn.com.ph.data.flink.schemas.OGGMessageSchema;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@Slf4j
public class App {

    private static final String FLINK_CHECKPOINT_PATH = "hdfs://pengzhaos-MacBook-Pro.local:9000/checkpoints-data/";

    public static void main(String[] args) throws Exception {

        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        int paramCount = 13;
        boolean writeEs = params.getBoolean("write.es");
        if(writeEs){
            paramCount = 20;
        }

        if (params.getNumberOfParameters() < paramCount) {
            System.out.println("\nUsage: FlinkReadKafka " +
                    "--app.name <appName> " +
                    "--flink.checkpoint.interval <flinkCheckPointInterval> " +
                    "--flink.window.interval <flinkWindowInterval> " +
                    "--primary.key.name <primaryKeyName> " +
                    "--read.topic <readTopic> " +
                    "--read.bootstrap.servers <readBootstrapServers> " +
                    "--read.group.id <readGroupName> " +
                    "--kafka.partition.num <kafkaPartitionNum> " +
                    "--startup.offsets.timestamp <startupOffsetsTimestamp> " +
                    "--hbase.zookeeper.quorum <hbaseZkQuorum> " +
                    "--hbase.zookeeper.quorum.port <hbaseZkQuorumPort> " +
                    "--hbase.table.name <hbaseTableName> " +
                    "--hbase.family.name <hbaseFamilyName> " +
                    "--hbase.user.name <hbaseUserName> " +
                    "--es.hosts <esHosts> " +
                    "--es.index.name <esIndexName> " +
                    "--es.index.fields <esIndexFields> " +
                    "--es.user.name <esUserName> " +
                    "--es.password <esPassword> " +
                    "--es.bulk.flush.max.actions <esBulkFlushMaxActions> " +
                    "--es.bulk.flush.interval <esBulkFlushInterval>");
            return;
        }
        String appName = params.get("app.name", "test");
        String checkPointPath = params.get("check.point.path");
        long flinkCheckPointInterval = params.getLong("flink.checkpoint.interval", 5 * 1000L);
        long flinkWindowInterval = params.getLong("flink.window.interval", 20);
        String primaryKeyName = params.get("primary.key.name", "id");

        String readTopic = params.get("read.topic");
        String readBootstrapServers = params.get("read.bootstrap.servers");
        String readGroupName = params.get("read.group.id", "synch_group");
        int kafkaPartitionNum = params.getInt("kafka.partition.num");
        long startupOffsetsTimestamp = params.getLong("startup.offsets.timestamp", 0L);

        String hbaseZkQuorum = params.get("hbase.zookeeper.quorum");
        String hbaseZkQuorumPort = params.get("hbase.zookeeper.quorum.port", "2181");
        String hbaseTableName = params.get("hbase.table.name");
        String hbaseFamilyName = params.get("hbase.family.name", "info");
        String hbaseUserName = params.get("hbase.user.name", "synch_user");

        String esHosts = params.get("es.hosts");
        String esIndexName = params.get("es.index.name");
        String esIndexFields = params.get("es.index.fields");
        String esUserName = params.get("es.user.name", "elastic");
        String esPassword = params.get("es.password", "elasticph");

        int esBulkFlushMaxActions = params.getInt("es.bulk.flush.max.actions", 1000);
        long esBulkFlushInterval = params.getLong("es.bulk.flush.interval", 10 * 1000L);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(flinkCheckPointInterval);

        env.setStateBackend((StateBackend) new RocksDBStateBackend(checkPointPath, true));
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


        Properties kafkaPro = new Properties();
        kafkaPro.setProperty("bootstrap.servers", readBootstrapServers);
        kafkaPro.setProperty("group.id", readGroupName);
        FlinkKafkaConsumer<OGGMessage> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                readTopic, new OGGMessageSchema(), kafkaPro
        );

        flinkKafkaConsumer.setStartFromTimestamp(startupOffsetsTimestamp);

        DataStreamSource<OGGMessage> stream = env.addSource(flinkKafkaConsumer).setParallelism(kafkaPartitionNum);
        SingleOutputStreamOperator<List<OGGMessage>> apply = stream
                .keyBy((KeySelector<OGGMessage, String>) oggMessage -> (String.valueOf(oggMessage.getPartition())))
                .window(TumblingEventTimeWindows.of(Time.seconds(flinkWindowInterval)))
                .apply(new WindowFunction<OGGMessage, List<OGGMessage>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<OGGMessage> input, Collector<List<OGGMessage>> out) {
                        ArrayList<OGGMessage> oggMessages = Lists.newArrayList();
                        input.forEach(oggMessage -> oggMessages.add(oggMessage));
                        out.collect(oggMessages);
                    }
                }).setParallelism(kafkaPartitionNum);


        HBaseWriter4J hBaseWriter4J = HBaseWriter4J.builder()
                .hbaseZookeeperQuorum(hbaseZkQuorum)
                .hbaseZookeeperClientPort(hbaseZkQuorumPort)
                .tableNameStr(hbaseTableName)
                .family(hbaseFamilyName)
                .primaryKeyName(primaryKeyName)
                .hbaseUserName(hbaseUserName)
                .build();

        apply.addSink(hBaseWriter4J).setParallelism(kafkaPartitionNum);

        if (writeEs) {
            List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(esHosts);

            ESSinkFunction esSinkFunction = ESSinkFunction.builder()
                    .indexName(esIndexName)
                    .esIndexFields(esIndexFields)
                    .primaryKeys(primaryKeyName)
                    .build();

            ElasticsearchSink.Builder<List<OGGMessage>> esSinkBuilder =
                    new ElasticsearchSink.Builder<>(esAddresses, esSinkFunction);
            esSinkBuilder.setBulkFlushMaxActions(esBulkFlushMaxActions);
            esSinkBuilder.setBulkFlushInterval(esBulkFlushInterval);
            esSinkBuilder.setRestClientFactory((RestClientFactory) restClientBuilder -> {
                final CredentialsProvider credentialsProvider =
                        new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(esUserName, esPassword));
                restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                   httpClientBuilder.disableAuthCaching();
                   return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                });
            });

            esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
            ElasticsearchSink<List<OGGMessage>> elasticsearchSink = esSinkBuilder.build();
            apply.addSink(elasticsearchSink).setParallelism(kafkaPartitionNum);
        }

        env.execute(appName);
    }
}
