package cn.com.ph.data.flink.hbase;

import cn.com.ph.data.flink.common.model.OGGMessage;
import cn.com.ph.data.flink.common.model.OGGOpType;
import cn.com.ph.data.flink.common.utils.HBaseUtil;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Builder
public class HBaseWriter4J extends RichSinkFunction<List<OGGMessage>> implements SinkFunction<List<OGGMessage>> {

    private String tableNameStr;
    private String hbaseZookeeperQuorum;
    private String hbaseZookeeperClientPort;
    private Connection conn;
    private BufferedMutator bufferedMutator;

    private String family;
    private String primaryKeyName;
    private String hbaseUserName;

    private byte[] familyNameBytes;
    private byte[] delFlagBytes;
    private byte[] delFlagTrueValueBytes;
    private byte[] delFlagFalseValueBytes;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.familyNameBytes = Bytes.toBytes(this.family);
        this.delFlagBytes = Bytes.toBytes(HBaseUtil.DELETE_FLAG);
        this.delFlagTrueValueBytes = Bytes.toBytes(HBaseUtil.DELETE_FLAG_VALUE_TRUE);
        this.delFlagFalseValueBytes = Bytes.toBytes(HBaseUtil.DELETE_FLAG_VALUE_FALSE);

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, this.hbaseZookeeperQuorum);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, this.hbaseZookeeperClientPort);

        User user = User.createUserForTesting(configuration, this.hbaseUserName,
                (String[]) Collections.singletonList("supergroup").toArray());
        this.conn = ConnectionFactory.createConnection(configuration, user);
        TableName tableName = TableName.valueOf(this.tableNameStr);
        BufferedMutatorParams bufferedMutatorParams = new BufferedMutatorParams(tableName);
        bufferedMutatorParams.writeBufferSize(1024L * 1024L);
        this.bufferedMutator = this.conn.getBufferedMutator(bufferedMutatorParams);

    }

    @Override
    public void invoke(List<OGGMessage> oggMessages, Context context) throws Exception {

        for(OGGMessage oggMessage : oggMessages){
            String hbaseRowKey = HBaseUtil.getHBaseRowKey(oggMessage, this.primaryKeyName);
            Put put = new Put(Bytes.toBytes(hbaseRowKey));
            if(StringUtils.equals(OGGOpType.D.getValue(), oggMessage.getOpType())){
                put.addColumn(this.familyNameBytes, this.delFlagBytes, this.delFlagTrueValueBytes);
            }else {
                if(StringUtils.equals(OGGOpType.I.getValue(), oggMessage.getOpType())){
                    put.addColumn(this.familyNameBytes, this.delFlagBytes, this.delFlagFalseValueBytes);
                }
                Map<String, String> keyValues = oggMessage.getKeyValues();
                keyValues.forEach((key, value) -> {
                    if(StringUtils.isBlank(value)){
                        value = HBaseUtil.NULL_STRING;
                    }
                    put.addColumn(this.familyNameBytes, Bytes.toBytes(key), Bytes.toBytes(value));
                });
            }

            this.bufferedMutator.mutate(put);
        }
        this.bufferedMutator.flush();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (!Objects.isNull(this.bufferedMutator)) {
            this.bufferedMutator.flush();
            this.bufferedMutator.close();
        }
        if (!Objects.isNull(this.conn)) this.conn.close();
    }
}
