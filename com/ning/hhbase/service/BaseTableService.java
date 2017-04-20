/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:02
 * @version V1.0
 */
package com.ning.hhbase.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.ning.hhbase.bean.ESTable;
import com.ning.hhbase.bean.HbaseTable;
import com.ning.hhbase.bean.Index;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.tools.ESHbaseMetaDataUtils;
import com.ning.hhbase.tools.HiveUtil;


/**
 * @ClassName: BaseTableService
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月16日 上午9:35:01
 *
 **/
public class BaseTableService {

    /**
     * @Fields LOG : 日志
     **/
    private static Logger LOG = Logger.getLogger(BaseTableService.class);
    
    private HiveUtil hiveUtil = null;

    private ESHbaseMetaDataUtils metaUtils = null;
    
    public BaseTableService(NPBaseConfiguration config) {
        hiveUtil = new HiveUtil(config);
        metaUtils = ESHbaseMetaDataUtils.getInstance(config);
    }


    /**
     * @Title: ddlTableWithSql
     * @Description: TODO
     * @param sql
     * @return
     * @throws Exception
     **/
    public String ddlTableWithSql(String sql) throws Exception {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter ddlTableWithSql() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("sql=");
            buffer.append(sql);
            LOG.debug(buffer.toString());
        }
        String msg = hiveUtil.ddlTableWithSql(sql);
        if ("success".equals(msg)) {
            metaUtils.init();
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit createTable() method");
        }
        return msg;
    }

    /**
     * @Title: supportedQueryTypes
     * @Description: TODO
     * @param table
     * @return
     **/
    public List<String> supportedQueryTypes(Table table) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter supportedQueryTypes() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }

        List<String> returnList = new ArrayList<String>();
        if(table instanceof ESTable){
            returnList.add("es");
        }else if(table instanceof HbaseTable){
            returnList.add("rowkey");
            returnList.add("range");
            HbaseTable hbaseTable = (HbaseTable)table;
            List<Index> globalIndexes = hbaseTable.getGlobalIndexes();
            if(globalIndexes != null && globalIndexes.size()>0){
                returnList.add("global");
            }
            Index esIndex = hbaseTable.getEsIndex();
            if(esIndex != null && esIndex.getIndexName() != null){
                returnList.add("es");
            }
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit supportedQueryTypes() method");
        }
        return returnList;
    }

    /**
     * @Title: supportedQueryTypes
     * @Description: TODO
     * @param tableName
     * @return
     **/
    public List<String> supportedQueryTypes(String tableName) {
        Table table = metaUtils.getTable(tableName);
        return supportedQueryTypes(table);
    }

}
