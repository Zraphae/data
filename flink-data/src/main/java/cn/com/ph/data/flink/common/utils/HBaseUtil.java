package cn.com.ph.data.flink.common.utils;

import cn.com.ph.data.flink.common.model.OGGMessage;
import com.google.common.base.Joiner;
import com.google.gson.JsonObject;

import java.util.Map;


public class HBaseUtil {

    public static final String DELETE_FLAG = "EXTEND_DEL_FLAG";
    public static final String DELETE_FLAG_VALUE_FALSE = "0";
    public static final String DELETE_FLAG_VALUE_TRUE = "1";
    public static final String NULL_STRING = "\\N";

    public static String getHBaseRowKey(OGGMessage oggMessage, String primaryKeyName) {
        Map<String, String> keyValues = oggMessage.getKeyValues();
        String primaryValues = getPrimaryValues(primaryKeyName, keyValues);
        return RandomUtil.makeRowKey(primaryValues);
    }

    public static String getPrimaryValues(String primaryKeyName, Map<String, String> objKeyValues) {
        String[] primaryKeys = primaryKeyName.split(",");
        String[] keyValues = new String[primaryKeys.length];
        for(int index=0; index<primaryKeys.length; index++){
            String keyValue = objKeyValues.get(primaryKeys[index]);
            keyValues[index] = keyValue;
        }
        return Joiner.on("_").join(keyValues);
    }


    public static String getPrimaryValues(String primaryKeyName, JsonObject jsonObject) {
        String[] primaryKeys = primaryKeyName.split(",");
        String[] keyValues = new String[primaryKeys.length];
        for(int index=0; index<primaryKeys.length; index++){
            String keyValue = jsonObject.get(primaryKeys[index]).getAsString();
            keyValues[index] = keyValue;
        }
        return Joiner.on("_").join(keyValues);
    }
}
