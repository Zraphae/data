package cn.com.ph.data.flink.common.model;

import lombok.val;

public enum OGGOpType {
    I("I"), U("U"), D("D");

    private String value;

    OGGOpType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}
