package cn.com.ph.data.flink.schemas;

import cn.com.ph.data.flink.common.model.OGGMessage;
import cn.com.ph.data.flink.common.utils.GsonUtil;
import cn.com.ph.data.flink.common.utils.HBaseUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class OGGMessageSchema implements KafkaDeserializationSchema<OGGMessage>, KafkaSerializationSchema<String> {

    private String topicName;
    private String primaryKey;

    public TypeInformation<OGGMessage> getProducedType() {
        return TypeExtractor.getForClass(OGGMessage.class);
    }


    public boolean isEndOfStream(OGGMessage nextElement) {
        return false;
    }

    public OGGMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        byte[] value = record.value();
        byte[] key = record.key();
        OGGMessage oggMessage = new OGGMessage();
        if(!Objects.isNull(value)){
            String jsonStr = new String(value, StandardCharsets.UTF_8);
            oggMessage = GsonUtil.fromJson(jsonStr, OGGMessage.class);
            oggMessage.setOffset(record.offset());
            oggMessage.setTopicName(record.topic());
            oggMessage.setPartition(record.partition());
        }
        if(!Objects.isNull(key)){
            String keyStr = new String(key, StandardCharsets.UTF_8);
            oggMessage.setKey(keyStr);
        }
        return oggMessage;
    }



    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        byte[] keyBytes = null;
        if(!StringUtils.isBlank(this.primaryKey)){
            JsonObject jsonObject = GsonUtil.fromJson(element, JsonObject.class);
            String primaryValues = HBaseUtil.getPrimaryValues(this.primaryKey, jsonObject);
            keyBytes = primaryValues.getBytes(StandardCharsets.UTF_8);
        }
        byte[] valuesBytes = element.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(this.topicName, keyBytes, valuesBytes);
    }
}
