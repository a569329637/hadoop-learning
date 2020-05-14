package com.gsq.hadoop.hbase.domain;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class HBaseModel {
    private String id;
    private String family;
    private String qualifier;
    private String value;
}
