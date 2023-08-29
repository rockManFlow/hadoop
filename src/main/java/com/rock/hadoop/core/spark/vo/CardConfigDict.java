package com.rock.hadoop.core.spark.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CardConfigDict {

    /**
     * 主键Id
     */
    private Long id;

    /**
     * 类型
     */
    private String type;

    /**
     * 编码
     */
    private String code;

    /**
     * 名称
     */
    private String name;

    /**
     * 父类编码
     */
    private String parentCode;

    /**
     * 排序字段
     */
    private Long sort;

    /**
     * 状态：NORMAL-启用，DISABLE-禁用
     */
    private String status;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;

}
