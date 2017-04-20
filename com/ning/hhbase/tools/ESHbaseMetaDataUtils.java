/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午3:27:43
 * @version V1.0
 */
package com.ning.hhbase.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSONArray;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * @ClassName: ESHbaseMetaDateUtils
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午3:27:43
 *
 **/
public final class ESHbaseMetaDataUtils {

    /**
     * @Fields log : 日志
     **/
    private static Logger LOG = Logger.getLogger(ESHbaseMetaDataUtils.class);

    /**
     * @Fields INSTANCE : 单例实例
     **/
    private static ESHbaseMetaDataUtils INSTANCE = null;

    private static HiveUtil hiveUtil = null;
    
    private static ZookeeperUtils zookeeperUtils = null;
    
    private static boolean orcSwitch = true;
    
    private List<Table> esTables = null;
    private List<Table> hbaseTables = null;
    private List<Table> orcTables = null;
    private List<Table> txtTables = null;
    private Map<String, Table> tableMap = new HashMap<String, Table>();

    private ESHbaseMetaDataUtils(NPBaseConfiguration config) {
        hiveUtil = new HiveUtil(config);
        try {
            zookeeperUtils = new ZookeeperUtils(config);
        } catch (Exception e) {
           e.printStackTrace();
        }
        
        if(!"true".equals(config.getValue(NPBaseConfiguration.HIVE_ORC_SWITCH))){
            orcSwitch = false;
        }
            
        
    }


    public static ESHbaseMetaDataUtils getInstance(NPBaseConfiguration config) {
        if(INSTANCE == null){
            INSTANCE = new ESHbaseMetaDataUtils(config);
        }
        return INSTANCE;
    }
    
//    /**
//     * @Title: getInstance
//     * @Description: 获取实例
//     * @return 实例
//     **/
//    public static ESHbaseMetaDataUtils getInstance() {
//        return INSTANCE;
//    }

    /**
     * @throws BigDataWareHouseException 
     * @Title: init
     * @Description: 初始化
     * @throws Exception
     **/
    public void init() throws BigDataWareHouseException {
        try {
//            ZookeeperUtils zookeeperUtils = new ZookeeperUtils();
            zookeeperUtils.getAllTableDefine();
            
           
           
            tableMap = new HashMap<String, Table>();

            if (null != esTables) {
                for (Table table : esTables) {
                    tableMap.put(table.getName(), table);
                }
            }

            if (null != hbaseTables) {
                for (Table table : hbaseTables) {
                    tableMap.put(table.getName(), table);
                }
            }

            if(orcSwitch){
                // 查询所有orc表
                List<Table> tables = hiveUtil.getOrctables(hiveUtil.getConnection());
                INSTANCE.setOrcTables(tables);
                if (null != tables) {
                    for (Table table : tables) {
                        tableMap.put(table.getName(), table);
                    }
                }
            }
            
            // 查询所有txt表
            List<Table> tables = hiveUtil.getTxtTables(hiveUtil.getConnection());
            INSTANCE.setTxtTables(tables);
            if (null != tables) {
                for (Table table : tables) {
                    tableMap.put(table.getName(), table);
                }
            }
            

        } catch (Exception e) {
            LOG.error("====从zookeeper上获取元数据信息失败");
            throw new BigDataWareHouseException(e.getMessage(),e);
        }
    }

    public List<Table> getTables() {
        List<Table> tables = new ArrayList<Table>();
        tables.addAll(INSTANCE.getEsTables());
        tables.addAll(INSTANCE.getHbaseTables());
        return tables;
    }

    /**
     * @Title: getTable
     * @Description: 根据表名获取表配置
     * @param tableName
     * @return
     **/
    public Table getTable(String tableName) {
        if (null != tableMap && tableMap.containsKey(tableName)) {
            return tableMap.get(tableName);
        }
        return null;
    }

    public JSONArray getEsTablesName() {
        JSONArray tableNames = new JSONArray();
        if (null != esTables) {
            for (Table table : esTables) {
                tableNames.add(table.getName());
            }
        }
        return tableNames;
    }

    public JSONArray getHbaseTablesName() {
        JSONArray tableNames = new JSONArray();
        if (null != hbaseTables) {
            for (Table table : hbaseTables) {
                tableNames.add(table.getName());
            }
        }
        return tableNames;
    }

    public JSONArray getORCTablesName() {
        JSONArray tableNames = new JSONArray();
        if (null != orcTables) {
            for (Table table : orcTables) {
                tableNames.add(table.getName());
            }
        }
        return tableNames;
    }

    public JSONArray getAllTablesName() {
        JSONArray tableNames = new JSONArray();

        if (null != esTables) {
            for (Table table : esTables) {
                tableNames.add(table.getName());
            }
        }

        if (null != hbaseTables) {
            for (Table table : hbaseTables) {
                tableNames.add(table.getName());
            }
        }

        return tableNames;
    }

    public List<Table> getEsTables() {
        return esTables;
    }

    public void setEsTables(List<Table> esTables) {
        this.esTables = esTables;
    }

    public List<Table> getHbaseTables() {
        return hbaseTables;
    }

    public void setHbaseTables(List<Table> hbaseTables) {
        this.hbaseTables = hbaseTables;
    }

    public List<Table> getOrcTables() {
        return orcTables;
    }

    public void setOrcTables(List<Table> orcTables) {
        this.orcTables = orcTables;
    }

    public JSONArray getTxtTablesName() {
        JSONArray tableNames = new JSONArray();
        if (null != txtTables) {
            for (Table table : txtTables) {
                tableNames.add(table.getName());
            }
        }
        return tableNames;
    }

    public List<Table> getTxtTables() {
        return txtTables;
    }

    public void setTxtTables(List<Table> txtTables) {
        this.txtTables = txtTables;
    }
    
}
