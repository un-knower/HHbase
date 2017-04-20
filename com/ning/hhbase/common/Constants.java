/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 上午11:06:24
 * @version V1.0
 */
package com.ning.hhbase.common;


/**
 * @ClassName: Constants
 * @Description: 常量
 * @author huangjinyan
 * @date 2016年8月15日 下午2:29:52
 **/
public class Constants {

    public static final String HBASE_COLUMNFAMILY = "d";
    public static final String HBASE_COLUMN = "line";

   
    public final static String ES_PREFIX = "elasticsearch_";
   
    public static final String SEPARATOR = "" + new Character((char) 127);

    public static final String KAFKA_CONSUMER_GROUP_ID_PREFIX = "flow_";
    
    /**
     * @Fields BATCHTASK_BATCHDATA_MAX_NUM : 批处理任务每次最大读取数
     **/
    public static final int BATCHTASK_BATCHDATA_MAX_NUM = 20000;
}
