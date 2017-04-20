/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年9月23日 下午3:02:32
 * @version V1.0
 */
package com.ning.hhbase.engine;

import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.service.BaseTableService;
import com.ning.hhbase.tools.ESHbaseMetaDataUtils;

/**
 * @ClassName: NPBaseMetaData
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年9月23日 下午3:02:32
 *
 **/
public final class NPBaseMetaData {

    private BaseTableService baseTableService = null;

    private ESHbaseMetaDataUtils metaUtils = null;

    public NPBaseMetaData(NPBaseConfiguration config) {
        baseTableService = new BaseTableService(config);
        metaUtils = ESHbaseMetaDataUtils.getInstance(config);

    }

    public static enum Type {
        ES, HBASE, ORC, TXT
    }

    /**
     * @Title: supportedQueryTypes
     * @Description: TODO
     * @param tableName
     * @return
     **/
    public List<String> supportedQueryTypes(String tableName) {
        return baseTableService.supportedQueryTypes(tableName);
    }

    public void refreshTables() throws BigDataWareHouseException {
        metaUtils.init();
    }

    public List<Table> getTables() {
        return metaUtils.getTables();
    }

    public List<Table> getTables(Type type) {
        if (type == Type.ES) {
            return metaUtils.getEsTables();
        } else if (type == Type.HBASE) {
            return metaUtils.getHbaseTables();
        } else if (type == Type.ORC) {
            return metaUtils.getOrcTables();
        } else if (type == Type.TXT) {
            return metaUtils.getTxtTables();
        }
        return null;
    }

    public Table getTableByName(String table) {
        return metaUtils.getTable(table);
    }

    public JSONArray getTableNames() {
        return metaUtils.getAllTablesName();
    }

    public JSONArray getTableNames(Type type) {
        if (type == Type.ES) {
            return metaUtils.getEsTablesName();
        } else if (type == Type.HBASE) {
            return metaUtils.getHbaseTablesName();
        } else if (type == Type.ORC) {
            return metaUtils.getORCTablesName();
        } else if (type == Type.TXT) {
            return metaUtils.getTxtTablesName();
        }
        return null;
    }
}
