package cn.com.ph.data.flink.es;

import cn.com.ph.data.flink.common.model.OGGMessage;
import cn.com.ph.data.flink.common.model.OGGOpType;
import cn.com.ph.data.flink.common.utils.GsonUtil;
import cn.com.ph.data.flink.common.utils.HBaseUtil;
import com.google.gson.JsonObject;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Map;


@Builder
public class ESSinkFunction implements ElasticsearchSinkFunction<List<OGGMessage>> {

    private String indexName;
    private String esIndexFields;
    private String primaryKeys;


    @Override
    public void process(List<OGGMessage> element, RuntimeContext ctx, RequestIndexer indexer) {
        element.forEach(oggMessage -> {
            OGGOpType oggOpType = OGGOpType.valueOf(oggMessage.getOpType());
            switch (oggOpType){
                case D:
                case U:
                    indexer.add(this.getEsUpdateIndex(oggMessage));
                    break;
                case I:
                    indexer.add(this.getEsInsertIndex(oggMessage));
            }
        });
    }

    private IndexRequest getEsInsertIndex(OGGMessage oggMessage) {
        IndexRequest indexRequest = Requests.indexRequest()
                .index(this.indexName)
                .id(HBaseUtil.getHBaseRowKey(oggMessage, this.primaryKeys))
                .source(this.toJsonBytes(oggMessage, this.esIndexFields), XContentType.JSON);
        return indexRequest;
    }


    private UpdateRequest getEsUpdateIndex(OGGMessage oggMessage) {

        UpdateRequest updateRequest = new UpdateRequest()
                .index(this.indexName)
                .id(HBaseUtil.getHBaseRowKey(oggMessage, this.primaryKeys))
                .doc(this.toJsonBytes(oggMessage, this.esIndexFields), XContentType.JSON);
        return updateRequest;
    }

    private byte[] toJsonBytes(OGGMessage oggMessage, String esIndexFields) {
        if(StringUtils.isBlank(esIndexFields)){
            return "".getBytes(GsonUtil.DEFAULT_CHARSET);
        }
        JsonObject result = new JsonObject();
        if(StringUtils.equals(oggMessage.getOpType(), OGGOpType.D.getValue())){
            result.addProperty(HBaseUtil.DELETE_FLAG, HBaseUtil.DELETE_FLAG_VALUE_TRUE);
            return result.toString().getBytes(GsonUtil.DEFAULT_CHARSET);
        }
        boolean insertOp = false;
        if(StringUtils.equals(oggMessage.getOpType(), OGGOpType.I.getValue())){
            result.addProperty(HBaseUtil.DELETE_FLAG, HBaseUtil.DELETE_FLAG_VALUE_FALSE);
            insertOp = true;
        }
        String[] indexFields = esIndexFields.split(",");
        Map<String, String> keyValues = oggMessage.getKeyValues();
        for(int index=0; index<indexFields.length; index++){
            String key = indexFields[0];
            String value = keyValues.get(key);
            if(insertOp){
                if(StringUtils.isBlank(value)){
                    value = HBaseUtil.NULL_STRING;
                }
                result.addProperty(key, value);
            }else {
                if(keyValues.containsKey(key)){
                    if(StringUtils.isBlank(value)){
                        value = HBaseUtil.NULL_STRING;
                    }
                    result.addProperty(key, value);
                }
            }
        }

        return result.toString().getBytes(GsonUtil.DEFAULT_CHARSET);
    }
}
