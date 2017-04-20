/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午5:20:25
 * @version V1.0
 */
package com.ning.hhbase.tools;

import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ning.hhbase.bean.Column;
import com.ning.hhbase.bean.Condition;
import com.ning.hhbase.bean.Conditions;
import com.ning.hhbase.bean.ESTable;
import com.ning.hhbase.bean.HbaseTable;
import com.ning.hhbase.bean.Index;
import com.ning.hhbase.bean.Limit;
import com.ning.hhbase.bean.ResultCode;
import com.ning.hhbase.bean.Sort;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.bean.TableJoin;
import com.ning.hhbase.bean.TableJoinStruct;
import com.ning.hhbase.common.Constants;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.connection.ConnectionPoolManager;
import com.ning.hhbase.connection.ESHbaseConnection;
import com.ning.hhbase.engine.NPBaseEngine;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.filter.BaseFilter;
import com.ning.hhbase.filter.FilterElement;

import solutions.siren.join.action.coordinate.CoordinateSearchRequestBuilder;

/**
 * @ClassName: EsUtils
 * @Description: es工具类，移植原EsAction 和 EsClient
 * @author huangjinyan
 * @date 2016年8月15日 下午5:20:25
 *
 **/
public final class EsUtils {

    /**
     * @Fields LOG : 日志
     **/
    public static Logger LOG = Logger.getLogger(EsUtils.class);

    /**
     * @Fields df : 日期
     **/
    public static DateFormat df = new SimpleDateFormat("yyyyMMdd HHmmss");

    /**
     * @Fields df : 日期
     **/
    public static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * @Fields df : 日期
     **/
    public static DateFormat sdfSource = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

    /**
     * @Fields cpm : 连接池
     **/
    private static ConnectionPoolManager cpm = ConnectionPoolManager.getInstance();

    /**
     * @Fields client : TODO（用一句话描述这个变量表示什么）
     **/
    private TransportClient client;

    private NPBaseConfiguration config = null;

    public EsUtils(NPBaseConfiguration config) {
        this.config = config;
    }

    public void setConfig(NPBaseConfiguration config) {
        this.config = config;
    }

    public TransportClient getClient() throws NumberFormatException, UnknownHostException {

        if (client == null) {
            // 2.3.1版本
            Settings settings = Settings.settingsBuilder().put("cluster.name", config.getValue(NPBaseConfiguration.ES_CLUSTER))
                    .put("client.transport.sniff", true).build();
            // 1.7.1版本
            // Settings settings = ImmutableSettings.settingsBuilder()
            //
            // // client.transport.sniff=true
            //
            // // 客户端嗅探整个集群的状态，把集群中其它机器的ip地址自动添加到客户端中，并且自动发现新加入集群的机器
            // .put("client.transport.sniff", true).put("client", true)//
            // 仅作为客户端连接
            // .put("data",
            // false).put("cluster.name",config.getValue(NPBaseConfiguration.ES_CLUSTER))//
            // 集群名称
            // .build();

            // 2.3.1版本
            client = TransportClient.builder().settings(settings).build();

            if (config.getValue(NPBaseConfiguration.ES_ADDRESS) != null && config.getValue(NPBaseConfiguration.ES_ADDRESS).contains(",")) {
                String[] address = config.getValue(NPBaseConfiguration.ES_ADDRESS).split(",");
                for (int i = 0; i < address.length; i++) {
                    client.addTransportAddress(
                            new InetSocketTransportAddress(InetAddress.getByName((address[i])), Integer.valueOf(config.getValue(NPBaseConfiguration.ES_PORT))));
                }
            } else {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(config.getValue(NPBaseConfiguration.ES_ADDRESS)),
                        Integer.valueOf(config.getValue(NPBaseConfiguration.ES_PORT))));
            }

            // 1.7.1版本
            // TCP 连接地址
            LOG.debug("es的ip===" + config.getValue(NPBaseConfiguration.ES_ADDRESS) + ", es的port===" + config.getValue(NPBaseConfiguration.ES_PORT));

            // client = new TransportClient(settings);
            // if(config.getValue(NPBaseConfiguration.ES_ADDRESS) != null &&
            // config.getValue(NPBaseConfiguration.ES_ADDRESS).contains(",")){
            // String[] address =
            // config.getValue(NPBaseConfiguration.ES_ADDRESS).split(",");
            // for (int i = 0; i < address.length; i++) {
            // client.addTransportAddress(new
            // InetSocketTransportAddress((address[i]),
            // Integer.valueOf(config.getValue(NPBaseConfiguration.ES_PORT))));
            // }
            // }else{
            // client.addTransportAddress(new
            // InetSocketTransportAddress(config.getValue(NPBaseConfiguration.ES_ADDRESS),
            // Integer.valueOf(config.getValue(NPBaseConfiguration.ES_PORT))));
            // }

        }

        return client;
    }

    /**
     * @Title: createIndex
     * @Description: 创建索引
     * @param index
     *            索引名
     * @param indexColumnList
     *            不分词列
     * @param filterColumnList
     *            分词列
     * @param shards
     * @param replicas
     * @throws BigDataWareHouseException
     */
    public String createTable(String index, List<Column> filterColumnList, int shards, int replicas) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        String message = "success";

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(index)// type名称
                    .startObject("_source").field("enabled", false).endObject().startObject("properties"); // 下面是设置文档列属性。

            int filterColumnSize = filterColumnList.size();

            Column column = null;

            for (int j = 0; j < filterColumnSize; j++) {
                column = filterColumnList.get(j);
                builder.startObject(column.getName()).field("type", column.getType()).field("store", "false").field("analyzer", "ik").endObject();
            }

            builder.endObject().endObject().endObject();

            // 创建索引
            CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(index)
                    .setSettings(Settings.settingsBuilder().put("number_of_shards", shards).put("number_of_replicas", replicas));// index名称
            CreateIndexResponse response = cirb.execute().actionGet();

            if (!response.isAcknowledged()) {
                LogUtils.errorMsg(BigDataWareHouseException.CREATE_INDEX_FAIL, index);
            }

            // 更新mapping
            PutMappingRequest mapping = Requests.putMappingRequest(index).type(index).source(builder);
            PutMappingResponse response1 = client.admin().indices().putMapping(mapping).actionGet();

            if (!response1.isAcknowledged()) {
                DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
                if (deleteIndexResponse.isAcknowledged()) {
                    LogUtils.errorMsg(BigDataWareHouseException.CREATE_INDEX_COLS_FAIL, index);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            message = e.getMessage();
        }
        cpm.close(ec);
        return message;
    }

    public String accessInsert(ESTable table, String[] columns, String rowKey, List<Object[]> dataList, ESHbaseConnection ec) throws BigDataWareHouseException {
        TransportClient client = ec.getEsClient();
        String message = insertCommon(table, columns, rowKey, dataList, client);
        return message;
    }

    /**
     * @Title: insert
     * @Description: 插入索引
     * @param table
     *            索引名
     * @param columns
     *            索引列
     * @param rowKey
     * @param dataList
     *            date
     * @return
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public String insert(ESTable table, String[] columns, String rowKey, List<Object[]> dataList) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        String message = insertCommon(table, columns, rowKey, dataList, client);
        cpm.close(ec);
        return message;
    }

    /**
     * @Title: delete
     * @Description: TODO
     * @param table
     * @param rowKey
     * @return
     * @throws ClassNotFoundException
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     **/
    public String delete(Table table, String rowKey) throws BigDataWareHouseException {
        String res = "success";
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        client.delete(new DeleteRequest(table.getName(), table.getName(), rowKey));
        cpm.close(ec);
        return res;
    }

    public String batchDeleteES(String indexName, String column, List<Object[]> dataList, int index) throws BigDataWareHouseException {
        String res = "success";
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        // BulkRequestBuilder bulkRequest = client.prepareBulk();
        // DeleteByQueryRequestBuilder builder =
        // client.prepareDeleteByQuery(indexName);
        SearchRequestBuilder builder = client.prepareSearch(indexName.toLowerCase());
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        for (int i = 0; i < dataList.size(); i++) {
            qb.should(QueryBuilders.termQuery(column, (String) dataList.get(i)[index]));
        }

        SearchResponse bulkResponse = builder.setQuery(qb).execute().actionGet();
        SearchHit[] searchHists = bulkResponse.getHits().getHits();
        for (SearchHit hit : searchHists) {
            client.prepareDelete(indexName.toLowerCase(), indexName.toLowerCase(), hit.getId()).execute().actionGet();
        }
        cpm.close(ec);
        return res;
    }

    /**
     * @Title: deleteES
     * @Description: TODO
     * @param table
     * @param rowKey
     * @return
     * @throws ClassNotFoundException
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     **/
    public String deleteES(ESTable table, String rowKey) throws BigDataWareHouseException {
        String res = "success";
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        client.delete(new DeleteRequest(table.getIndexName().toLowerCase(), table.getTypeName().toLowerCase(), rowKey));
        cpm.close(ec);
        return res;
    }

    /**
     * @Title: queryCommon
     * @Description: TODO
     * @param table
     * @param condition
     * @param sortList
     * @param limit
     * @param total
     * @param client
     * @param indexName
     * @return
     * @throws BigDataWareHouseException
     **/
    private SearchHit[] queryCommon(Table table, Condition condition, List<Sort> sortList, Limit limit, AtomicLong total, TransportClient client,
            String indexName) throws BigDataWareHouseException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        QueryBuilder qb = parseCons(null, new ArrayList<String>(), condition, queryBuilder, null, table.getColumnList().get(1).getFamilyName());

        SearchRequestBuilder builder = client.prepareSearch(indexName).setTypes(indexName);

        if (limit == null) {
            // limit = new Limit(0,10);
            builder.setScroll("10m").setSize((int) Integer.MAX_VALUE);
            // try {
            // client.admin().indices().updateSettings(Requests.updateSettingsRequest(indexName).settings(org.elasticsearch.common.settings.Settings.settingsBuilder()
            // .put("max_result_window", "100000000"))).get();
            // } catch (Exception e) {
            // throw new
            // BigDataWareHouseException(BigDataWareHouseException.UPDATE_SETTING_ERROR,e);
            // }
        } else {
            builder.setSearchType(SearchType.QUERY_THEN_FETCH).setFrom((int) limit.getIndex()).setSize((int) limit.getCount());
            // if(limit.getCount()>10000){
            // try {
            // client.admin().indices().updateSettings(Requests.updateSettingsRequest(indexName).settings(org.elasticsearch.common.settings.Settings.settingsBuilder()
            // .put("max_result_window", "100000000"))).get();
            // } catch (Exception e) {
            // throw new
            // BigDataWareHouseException(BigDataWareHouseException.UPDATE_SETTING_ERROR,e);
            // }
            // }
        }

        int sortSize = sortList.size();
        SortOrder order = null;
        String column = "";
        boolean desc = false;
        for (int i = 0; i < sortSize; i++) {
            column = table.getColumnList().get(1).getFamilyName() + "_" + sortList.get(i).getColumn();
            desc = sortList.get(i).isDesc();

            if (desc) {
                order = SortOrder.DESC;
            } else {
                order = SortOrder.ASC;
            }

            builder.addSort(column, order);
        }

        builder.setQuery(qb);

        SearchHits hits = builder.execute().actionGet().getHits();
        total.set(hits.getTotalHits());
        SearchHit[] searchHists = hits.getHits();
        return searchHists;
    }

    /**
     * @Title: query
     * @Description: 查询操作
     * @param index
     *            索引
     * @param columns
     *            要返回的列
     * @param queryBuilder
     *            查询条件
     * @param sortList
     *            排序
     * @param limit
     *            分页
     * @return
     * @throws InterruptedException
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     */
    public List<String> queryTable(HbaseTable table, String[] columns, Condition condition, List<Sort> sortList, Limit limit, AtomicLong total)
            throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        Index index = table.getEsIndex();

        String indexName = index.getIndexName();
        SearchHit[] searchHists = queryCommon(table, condition, sortList, limit, total, client, indexName);

        List<String> list = new ArrayList<String>();
        for (SearchHit hit : searchHists) {
            list.add(hit.getId());
        }

        cpm.close(ec);
        return list;
    }

    /**
     * @Title: queryESTable
     * @Description: TODO
     * @param table
     * @param columns
     * @param condition
     * @param sortList
     * @param limit
     * @param total
     * @param filterName
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     **/
    public List<Object[]> queryESTable(ESTable table, String[] columns, Condition condition, List<Sort> sortList, Limit limit, AtomicLong total,
            String filterName, ResultCode rc) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        String indexType = table.getIndexType();
        BaseFilter bf = null;
        if (null != filterName) {
            bf = getFilterInstance(filterName, rc).getFilterInstance();
        }
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        List<String> listConColNames = new ArrayList<String>();
        QueryBuilder qb = parseCons(indexType, listConColNames, condition, queryBuilder, null, null);

        SearchRequestBuilder builder = client.prepareSearch(table.getIndexName().toLowerCase()).setTypes(table.getTypeName().toLowerCase());
        builder.setSearchType(SearchType.QUERY_THEN_FETCH);
        if (limit == null) {
            // builder.setScroll(scroll)
            builder.setScroll("10m").setSize((int) Integer.MAX_VALUE);
            // builder.setFrom((int) 0).setSize((int) Integer.MAX_VALUE);

            // try {
            // client.admin().indices().updateSettings(Requests.updateSettingsRequest(table.getIndexName()).settings(org.elasticsearch.common.settings.Settings.settingsBuilder()
            // .put("max_result_window", "100000000"))).get();
            // } catch (Exception e) {
            // throw new
            // BigDataWareHouseException(BigDataWareHouseException.UPDATE_SETTING_ERROR,e);
            // }

        } else {
            builder.setFrom((int) limit.getIndex()).setSize((int) limit.getCount());
            // if(limit.getCount()>10000){
            // try {
            // client.admin().indices().updateSettings(Requests.updateSettingsRequest(table.getIndexName()).settings(org.elasticsearch.common.settings.Settings.settingsBuilder()
            // .put("max_result_window", "100000000"))).get();
            // } catch (Exception e) {
            // throw new
            // BigDataWareHouseException(BigDataWareHouseException.UPDATE_SETTING_ERROR,e);
            // }
            // }
        }

        int sortSize = sortList.size();
        SortOrder order = null;
        String column = "";
        boolean desc = false;
        for (int i = 0; i < sortSize; i++) {
            column = sortList.get(i).getColumn();
            desc = sortList.get(i).isDesc();

            if (desc) {
                order = SortOrder.DESC;
            } else {
                order = SortOrder.ASC;
            }

            builder.addSort(column, order);
        }

        builder.setQuery(qb);

        if ("ik".equals(indexType)) {
            for (int i = 0; i < listConColNames.size(); i++) {
                if (listConColNames.get(i) != null) {
                    builder.addHighlightedField(listConColNames.get(i));
                }
            }
            builder.setHighlighterPreTags("<span style=\"color:red\">");
            builder.setHighlighterPostTags("</span>");
        }

        Map<String, Column> colMap = table.getHiveColumnMap();

        SearchHits hits = builder.execute().actionGet().getHits();
        total.set(hits.getTotalHits());
        SearchHit[] searchHists = hits.getHits();
        List<Object[]> list = new ArrayList<Object[]>();
        Object[] returnLine = null;
        Object colVal = null;
        for (SearchHit hit : searchHists) {

            Map<String, Object> map = hit.getSource();
            returnLine = new Object[columns.length];
            for (int i = 0; i < columns.length; i++) {
                if (null != hit.getId()) {
                    // if ("key".equals(columns[i]) && table instanceof
                    // HbaseTable) {
                    // colVal = hit.getId();
                    // } else

                    Column colInfo = colMap.get(columns[i]);
                    if ("_id".equals(columns[i]) && table instanceof ESTable) {
                        colVal = hit.getId();
                    } else {
                        if ("date".equals(colInfo.getType()) || "timestamp".equals(colInfo.getType())) {
                            if (map.get(columns[i]) instanceof java.util.ArrayList) {
                                continue;
                            }
                            String dateVal = (String) map.get(columns[i]);
                            if (dateVal != null && dateVal.length() > 20) {
                                try {
                                    colVal = dateVal.substring(0, 19).replace("T", " ");
                                } catch (Exception e) {
                                    throw new BigDataWareHouseException(BigDataWareHouseException.DATA_FORMAT_ERROR, e);
                                }

                            } else {
                                colVal = map.get(columns[i]);
                            }

                        } else {
                            colVal = map.get(columns[i]);
                        }
                    }
                    if (null != colVal && !"".equals(colVal)) {
                        returnLine[i] = colVal;
                    }
                }

            }
            if (null != hit.getId()) {
                filterd(bf, list, returnLine, rc);
            }

        }

        cpm.close(ec);
        return list;
    }

    public List<Map<String, Object>> queryESTableResultKV(ESTable table, String[] columns, Condition condition, List<Sort> sortList, Limit limit,
            AtomicLong total, String filterName, ResultCode rc) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        String indexType = table.getIndexType();
        BaseFilter bf = null;
        if (null != filterName) {
            bf = getFilterInstance(filterName, rc).getFilterInstance();
        }
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        List<String> listConColNames = new ArrayList<String>();
        QueryBuilder qb = parseCons(indexType, listConColNames, condition, queryBuilder, null, null);

        SearchRequestBuilder builder = client.prepareSearch(table.getIndexName().toLowerCase()).setTypes(table.getTypeName().toLowerCase());
        builder.setSearchType(SearchType.QUERY_THEN_FETCH);
        if (limit == null) {
            builder.setScroll("10m").setSize((int) Integer.MAX_VALUE);
            // builder.setFrom((int) 0).setSize((int) Integer.MAX_VALUE);
            // try {
            // client.admin().indices().updateSettings(Requests.updateSettingsRequest(table.getIndexName()).settings(org.elasticsearch.common.settings.Settings.settingsBuilder()
            // .put("max_result_window", "100000000"))).get();
            // } catch (Exception e) {
            // throw new
            // BigDataWareHouseException(BigDataWareHouseException.UPDATE_SETTING_ERROR,e);
            // }
        } else {
            builder.setFrom((int) limit.getIndex()).setSize((int) limit.getCount());
            // if(limit.getCount()>10000){
            // try {
            // client.admin().indices().updateSettings(Requests.updateSettingsRequest(table.getIndexName()).settings(org.elasticsearch.common.settings.Settings.settingsBuilder()
            // .put("max_result_window", "100000000"))).get();
            // } catch (Exception e) {
            // throw new
            // BigDataWareHouseException(BigDataWareHouseException.UPDATE_SETTING_ERROR,e);
            // }
            // }
        }

        int sortSize = sortList.size();
        SortOrder order = null;
        String column = "";
        boolean desc = false;
        for (int i = 0; i < sortSize; i++) {
            column = sortList.get(i).getColumn();
            desc = sortList.get(i).isDesc();

            if (desc) {
                order = SortOrder.DESC;
            } else {
                order = SortOrder.ASC;
            }

            builder.addSort(column, order);
        }

        builder.setQuery(qb);

        if ("ik".equals(indexType)) {
            for (int i = 0; i < listConColNames.size(); i++) {
                if (listConColNames.get(i) != null) {
                    builder.addHighlightedField(listConColNames.get(i));
                }
            }
            builder.setHighlighterPreTags("<span style=\"color:red\">");
            builder.setHighlighterPostTags("</span>");
        }

        Map<String, Column> colMap = table.getHiveColumnMap();

        SearchHits hits = builder.execute().actionGet().getHits();
        total.set(hits.getTotalHits());
        SearchHit[] searchHists = hits.getHits();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        Map<String, Object> returnLine = null;
        Object colVal = null;
        for (SearchHit hit : searchHists) {

            Map<String, Object> map = hit.getSource();
            returnLine = new HashMap<String, Object>(columns.length);
            for (int i = 0; i < columns.length; i++) {
                if (null != hit.getId()) {
                    // if ("key".equals(columns[i]) && table instanceof
                    // HbaseTable) {
                    // colVal = hit.getId();
                    // } else
                    Column colInfo = colMap.get(columns[i]);

                    if ("_id".equals(columns[i]) && table instanceof ESTable) {
                        colVal = hit.getId();
                    } else {
                        if ("date".equals(colInfo.getType()) || "timestamp".equals(colInfo.getType())) {
                            if (map.get(columns[i]) instanceof java.util.ArrayList) {
                                continue;
                            }
                            String dateVal = (String) map.get(columns[i]);
                            if (dateVal != null && dateVal.length() > 20) {
                                try {
                                    colVal = dateVal.substring(0, 19).replace("T", " ");
                                } catch (Exception e) {
                                    throw new BigDataWareHouseException(BigDataWareHouseException.DATA_FORMAT_ERROR, e);
                                }

                            } else {
                                colVal = map.get(columns[i]);
                            }
                        } else {
                            colVal = map.get(columns[i]);
                        }

                    }
                    if (null != colVal && !"".equals(colVal)) {
                        // returnLine[i] = colVal;
                        returnLine.put(columns[i], colVal);
                    }
                }

            }
            if (null != hit.getId()) {
                filterdResultKV(bf, list, returnLine, rc);
            }

        }

        cpm.close(ec);
        return list;
    }

    // /**
    // *
    // * @param listConColNames
    // * @Title: parseConsNames
    // * @Description: TODO
    // * @param condition
    // * @return
    // */
    // private List<String> parseConsNames(Condition con, List<String>
    // listConColNames) {
    // if ((con instanceof Conditions) && ((Conditions) con).getCdList().size()
    // > 0) {
    // Conditions cons = (Conditions) con;
    // List<Condition> conList = cons.getCdList();
    // for (Condition condition : conList) {
    // parseConsNames(condition,listConColNames);
    // }
    // } else {
    // listConColNames.add(con.getColumn());
    // }
    // return listConColNames;
    // }

    /**
     * @Title: getFilterInstance
     * @Description: TODO
     * @param filterName
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     **/
    public FilterElement getFilterInstance(String filterName, ResultCode rc) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        FilterElement fe = null;
        fe = FiltersUtils.getInstance().getFilters().getFilterElements().get(filterName);

        if (fe == null) {
            rc.setResultCode(ResultCode.FILTER_DONT_EXSIST);
            LogUtils.errorMsg(BigDataWareHouseException.FILTER_DONT_EXSIST, filterName);
        }

        return fe;
    }

    /**
     * @Title: filterd
     * @Description: TODO
     * @param bf
     * @param list
     * @param returnLine
     * @param rc
     * @throws BigDataWareHouseException
     **/
    public void filterd(BaseFilter bf, List<Object[]> list, Object[] returnLine, ResultCode rc) throws BigDataWareHouseException {
        if (bf != null) {
            if (bf.isRecordQualify(returnLine)) {
                list.add(returnLine);
            }
        } else {
            list.add(returnLine);
        }
    }

    /**
     * @Title: filterdResultKV
     * @Description: TODO
     * @param bf
     * @param list
     * @param returnLine
     * @param rc
     * @throws BigDataWareHouseException
     **/
    public void filterdResultKV(BaseFilter bf, List<Map<String, Object>> list, Map<String, Object> returnLine, ResultCode rc) throws BigDataWareHouseException {
        if (bf != null) {
            if (bf.isRecordQualifyResultKV(returnLine)) {
                list.add(returnLine);
            }
        } else {
            list.add(returnLine);
        }
    }

    /**
     * 
     * @Title: countESTable
     * @Description: TODO
     * @param esTable
     * @param condition
     * @return
     * @throws BigDataWareHouseException
     */
    public long countESTable(ESTable esTable, Condition condition) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();

        String indexName = esTable.getIndexName();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        QueryBuilder qb = parseCons(esTable.getIndexType(), new ArrayList<String>(), condition, queryBuilder, null, null);

        SearchRequestBuilder builder = client.prepareSearch(indexName.toLowerCase()).setTypes(esTable.getTypeName().toLowerCase());
        builder.setSearchType(SearchType.DEFAULT);

        builder.setQuery(qb);

        SearchHits hits = builder.execute().actionGet().getHits();
        long returnSize = hits.getTotalHits();
        cpm.close(ec);
        return returnSize;
    }

    /**
     * @Title: countTable
     * @Description: count条数
     * @param table
     * @param condition
     * @param total
     * @return
     * @throws BigDataWareHouseException
     */
    public long countTable(HbaseTable table, Condition condition) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        Index index = table.getEsIndex();

        String indexName = index.getIndexName();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        QueryBuilder qb = parseCons(null, new ArrayList<String>(), condition, queryBuilder, null, table.getColumnList().get(1).getFamilyName());

        SearchRequestBuilder builder = client.prepareSearch(indexName).setTypes(indexName);
        builder.setSearchType(SearchType.DEFAULT);

        builder.setQuery(qb);

        SearchHits hits = builder.execute().actionGet().getHits();
        long returnSize = hits.getTotalHits();
        cpm.close(ec);
        return returnSize;
    }

    /**
     * @param indexType
     * @param listConColNames
     * @Title: parseCons
     * @Description: 递归生成QueryBuilder
     * @param con
     * @param queryBuilder
     * @return
     */
    private QueryBuilder parseCons(String indexType, List<String> listConColNames, Condition con, BoolQueryBuilder queryBuilder, Conditions outerCons,
            String fName) {
        if (con == null) {
            return queryBuilder;
        }

        if ((con instanceof Conditions) && ((Conditions) con).getCdList().size() > 0) {
            Conditions cons = (Conditions) con;
            BoolQueryBuilder innerQueryBuilder = QueryBuilders.boolQuery();

            if (outerCons != null) {
                if (outerCons.getIsOr() == 0) {
                    queryBuilder.should(innerQueryBuilder);
                } else if (outerCons.getIsOr() == 1) {
                    queryBuilder.must(innerQueryBuilder);
                } else {
                    queryBuilder.mustNot(innerQueryBuilder);
                }
            } else {
                if (cons.getIsOr() == 0) {
                    queryBuilder.should(innerQueryBuilder);
                } else if (cons.getIsOr() == 1) {
                    queryBuilder.must(innerQueryBuilder);
                } else {
                    queryBuilder.mustNot(innerQueryBuilder);
                }
            }

            List<Condition> conList = cons.getCdList();
            for (Condition condition : conList) {
                parseCons(indexType, listConColNames, condition, innerQueryBuilder, cons, fName);
            }
        } else {
            String column = null;
            if (fName == null) {
                column = con.getColumn();
            } else {
                column = fName + "_" + con.getColumn();
            }
            String value = con.getValue();
            int compareType = con.getCompareType();
            if ("ik".equals(indexType) && compareType == 6) {
                compareType = 5;
            } else if ("keyword".equals(indexType) && compareType == 5) {
                compareType = 6;
            }
            if (outerCons != null) {
                if (outerCons.getIsOr() == 0) {
                    queryBuilder.should(createBuilder(column, value, compareType));
                } else if (outerCons.getIsOr() == 1) {
                    queryBuilder.must(createBuilder(column, value, compareType));
                } else {
                    queryBuilder.mustNot(createBuilder(column, value, compareType));
                }
            } else {
                if (column != null && value != null) {
                    queryBuilder.must(createBuilder(column, value, compareType));
                }

            }
            listConColNames.add(con.getColumn());
        }

        return queryBuilder;
    }

    /**
     * @Title: createBuilder
     * @Description: 根据不同的类型生成不同的QueryBuilder
     * @param column
     * @param value
     * @param compareType
     * @return
     */
    public QueryBuilder createBuilder(String column, String value, int compareType) {
        QueryBuilder qb = null;

        // 等于
        if (compareType == 0) {
            qb = QueryBuilders.termQuery(column, value);// .operator(Operator.AND);
        }

        // 大于
        if (compareType == 1) {
            qb = QueryBuilders.rangeQuery(column).gt(value);
        }

        // 小于
        if (compareType == 2) {
            qb = QueryBuilders.rangeQuery(column).lt(value);
        }

        // 大于等于
        if (compareType == 3) {
            qb = QueryBuilders.rangeQuery(column).gte(value);
        }

        // 小于等于
        if (compareType == 4) {
            qb = QueryBuilders.rangeQuery(column).lte(value);
        }

        // 分词关键字检索
        if (compareType == 5) {
            qb = QueryBuilders.fuzzyQuery(column, value);
        }

        // 全模糊查询
        if (compareType == 6) {
            qb = QueryBuilders.wildcardQuery(column, value);
        }

        return qb;
    }

    /**
     * 
     * @param rowKey
     * @Title: parseCons
     * @Description: TODO
     * @param con
     * @param queryBuilder
     * @param outerCons
     * @param fName
     * @return
     */
    private String insertCommon(ESTable table, String[] columns, String rowKey, List<Object[]> dataList, TransportClient client)
            throws BigDataWareHouseException {
        String message = "success";
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        try {
            List<Integer[]> structIdxs = new ArrayList<Integer[]>();
            List<Column> resStructCols = table.getStructCols();

            for (Column col : resStructCols) {
                List<Column> structCols = col.getStructCols();
                Integer[] idxs = new Integer[structCols.size()];
                boolean flag = false;
                for (int k = 0; k < structCols.size(); k++) {
                    Column structCol = structCols.get(k);
                    for (int i = 0; i < columns.length; i++) {
                        if (structCol.getName().equals(columns[i])) {
                            idxs[k] = i;
                            flag = true;
                        }
                    }
                }
                if (flag) {
                    structIdxs.add(idxs);
                }

            }

            int rowKeyIdx = -1;
            for (int i = 0; i < columns.length; i++) {
                if (rowKey != null && rowKey.equals(columns[i])) {
                    rowKeyIdx = i;
                }
            }

            int dataSize = dataList.size();
            int columnSize = columns.length;

            // 批量入es个数
            int batch = Integer.parseInt(config.getValue(NPBaseConfiguration.ES_BATCH_SIZE));
            XContentBuilder builder = null;
            Object[] valObj = null;

            // 默认 columns[0] 和 Object[0] 为 rowkey字段和值
            for (int i = 0; i < dataSize; i++) {
                builder = XContentFactory.jsonBuilder().startObject();
                valObj = dataList.get(i);
                if (structIdxs.size() > 0) {
                    Integer[] idxs = null;
                    for (int j = 0; j < resStructCols.size(); j++) {
                        idxs = structIdxs.get(j);
                        String value = "";
                        for (int k = 0; k < idxs.length; k++) {
                            if (idxs[k] != null) {
                                value += valObj[idxs[k]];
                            }
                            if (j != idxs.length - 1) {
                                value += "" + new Character((char) 2);
                            }
                        }
                        builder.field(resStructCols.get(j).getName(), value);
                    }
                }
                // 解决导致字段数量不平等问题，取最小字段数量
                int columnSizeTmp = 0;
                if (columnSize > valObj.length) {
                    columnSizeTmp = valObj.length;
                } else {
                    columnSizeTmp = columnSize;
                }
                for (int j = 0; j < columnSizeTmp; j++) {
                    builder.field(columns[j], valObj[j]);
                }

                builder.endObject();
                if (rowKeyIdx == -1) {
                    bulkRequest.add(client.prepareIndex(table.getIndexName().toLowerCase(), table.getTypeName().toLowerCase()).setSource(builder));
                } else {
                    bulkRequest.add(client.prepareIndex(table.getIndexName().toLowerCase(), table.getTypeName().toLowerCase(), (String) valObj[rowKeyIdx])
                            .setSource(builder));
                }

                if ((i + 1) % batch == 0) {
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    if (bulkResponse.hasFailures()) {
                        LogUtils.errorMsg(BigDataWareHouseException.INDEX_INSERT_FAIL, table.getName());
                    }
                    bulkRequest.request().requests().clear();
                }
            }

            if (dataSize % batch != 0) {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    System.out.println(bulkResponse.buildFailureMessage());
                    LogUtils.errorMsg(BigDataWareHouseException.INDEX_INSERT_FAIL, table.getName());
                }
                bulkRequest.request().requests().clear();
            }

        } catch (Exception e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.INSERT_ERROR, table.getName());
            message = e.getMessage();
        }
        return message;
    }

    /**
     * 
     * @Title: queryMutiESTableWithAPI
     * @Description: TODO
     * @param struct
     * @param columns
     * @param sortList
     * @param limit
     * @param total
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     */
    public List<Map<String, Object>> queryMutiESTableWithAPI(TableJoinStruct struct, String[] columns, List<Sort> sortList, Limit limit, AtomicLong total,
            ResultCode rc) throws BigDataWareHouseException {
        if (cpm == null) {
            cpm = ConnectionPoolManager.getInstance();
        }
        ESHbaseConnection ec = cpm.getConnection();
        TransportClient client = ec.getEsClient();
        List<String> listConColNames = new ArrayList<String>();
        String indexType = "keyword";

        CoordinateSearchRequestBuilder requestBuilder = new CoordinateSearchRequestBuilder(client);
        requestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);
        if (limit == null) {
            requestBuilder.setScroll("10m").setFrom((int) 0).setSize((int) Integer.MAX_VALUE);
            // requestBuilder.setFrom((int) 0).setSize((int) Integer.MAX_VALUE);
        } else {
            requestBuilder.setFrom((int) limit.getIndex()).setSize((int) limit.getCount());
        }

        TableJoin outerTable = struct.getOuterTable();
        TableJoin innerTable = struct.getInnerTable();
        // updateSetting(outerTable.getTableName(),client);

        if (innerTable instanceof TableJoinStruct) {
            TableJoinStruct innerStruct = (TableJoinStruct) innerTable;
            requestBuilder.setIndices(outerTable.getTableName().toLowerCase()).setQuery(boolQuery()
                    .filter(solutions.siren.join.index.query.QueryBuilders.filterJoin(struct.getJoinCol())
                            .indices(innerStruct.getOuterTable().getTableName().toLowerCase()).types(innerStruct.getOuterTable().getTableName().toLowerCase())
                            .path(struct.getJoinCol()).query(buildFilteredQuery(innerStruct, requestBuilder, indexType, listConColNames, client)))
                    .filter(parseCons(indexType, listConColNames, outerTable.getCondition(), QueryBuilders.boolQuery(), null, null))
            // filteredQuery(parseCons(indexType, listConColNames,
            // outerTable.getCondition(), QueryBuilders.boolQuery(), null,
            // null),
            // QueryBuilders.filterJoin(struct.getJoinCol()).indices(innerStruct.getOuterTable().getTableName().toLowerCase())
            // .path(struct.getJoinCol()).query(buildFilteredQuery(innerStruct,
            // requestBuilder, indexType, listConColNames)
            // ))
            );
        } else {

            // updateSetting(innerTable.getTableName(),client);
            requestBuilder
                    .setIndices(
                            outerTable.getTableName().toLowerCase())
                    .setQuery(boolQuery()
                            .filter(solutions.siren.join.index.query.QueryBuilders.filterJoin(struct.getJoinCol())
                                    .indices(innerTable.getTableName().toLowerCase()).types(innerTable.getTableName().toLowerCase()).path(struct.getJoinCol())
                                    .query(boolQuery()
                                            .filter(parseCons(indexType, listConColNames, innerTable.getCondition(), QueryBuilders.boolQuery(), null, null))))
                            .filter(parseCons(indexType, listConColNames, outerTable.getCondition(), QueryBuilders.boolQuery(), null, null))
            // filteredQuery(parseCons(indexType,listConColNames,outerTable.getCondition(),
            // QueryBuilders.boolQuery(), null, null),
            // FilterBuilders.filterJoin(struct.getJoinCol())
            // .indices(innerTable.getTableName().toLowerCase()).path(struct.getJoinCol()).query(parseCons(indexType,listConColNames,innerTable.getCondition(),
            // QueryBuilders.boolQuery(), null, null)))
            );
        }

        int sortSize = sortList.size();
        SortOrder order = null;
        String column = "";
        boolean desc = false;
        for (int i = 0; i < sortSize; i++) {
            column = sortList.get(i).getColumn();
            desc = sortList.get(i).isDesc();

            if (desc) {
                order = SortOrder.DESC;
            } else {
                order = SortOrder.ASC;
            }

            requestBuilder.addSort(column, order);
        }

        Map<String, Column> colMap = ESHbaseMetaDataUtils.getInstance(config).getTable(outerTable.getTableName()).getHiveColumnMap();

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            HttpURLConnection conn = null;
            BufferedReader br = null;
            StringBuilder resultBuilder = new StringBuilder();
            String line = null;
            // 1. Prepare url
            String url01 = "http://" + config.getValue(NPBaseConfiguration.ES_ADDRESS).split(",")[0] + ":9200/" + outerTable.getTableName().toLowerCase()
                    + "/_coordinate_search?pretty";
            URL restServiceURL;

            restServiceURL = new URL(url01);

            HttpURLConnection httpConnection = (HttpURLConnection) restServiceURL.openConnection();
            httpConnection.setRequestMethod("POST");
            httpConnection.setRequestProperty("Accept", "application/json");

            // 2. Prepare query param
            // String queryParamJson = buildQueryParamByStr();
            String queryParamJson = requestBuilder.toString();

            // 3. Inject url
            URL url = new URL(url01);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Content-Length", Integer.toString(queryParamJson.getBytes().length));
            conn.setRequestProperty("Content-Language", "en-US");
            conn.setUseCaches(false);
            conn.setDoOutput(true);

            // 4. Inject query param
            // 获取URLConnection对象对应的输出流
            OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
            // 发送请求参数
            out.write(queryParamJson);
            // flush输出流的缓冲
            out.flush();

            // Connection failure handling
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }

            // 5. Get Response
            br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            while ((line = br.readLine()) != null) {
                resultBuilder.append(line);
                resultBuilder.append('\r');
            }
            JSONObject obj = JSON.parseObject(resultBuilder.toString());
            Map hits = (Map) obj.get("hits");
            JSONArray hitsS = (JSONArray) hits.get("hits");

            total.set((int) hits.get("total"));

            Map<String, Object> returnLine = null;
            Object colVal = null;
            for (int i = 0; i < hitsS.size(); i++) {
                Map hit = (Map) hitsS.get(i);
                Map<String, Object> hi = (Map<String, Object>) hit.get("_source");
                returnLine = new HashMap<String, Object>(columns.length);
                for (int j = 0; j < columns.length; j++) {
                    if (null != hit.get("_id")) {
                        // if ("key".equals(columns[i]) && table instanceof
                        // HbaseTable) {
                        // colVal = hit.getId();
                        // } else
                        Column colInfo = colMap.get(columns[j]);
                        if ("_id".equals(columns[j])) {
                            colVal = hit.get("_id");
                        } else {
                            if ("date".equals(colInfo.getType()) || "timestamp".equals(colInfo.getType())) {
                                if (hi.get(columns[i]) instanceof java.util.ArrayList) {
                                    continue;
                                }
                                String dateVal = (String) hi.get(columns[j]);
                                if (dateVal != null && dateVal.length() > 20) {
                                    try {
                                        colVal = dateVal.substring(0, 19).replace("T", " ");
                                    } catch (Exception e) {
                                        throw new BigDataWareHouseException(BigDataWareHouseException.DATA_FORMAT_ERROR, e);
                                    }

                                } else {
                                    colVal = hi.get(columns[j]);
                                }
                            } else {
                                colVal = hi.get(columns[j]);
                            }
                        }
                        if (null != colVal && !"".equals(colVal)) {
                            // returnLine[i] = colVal;
                            returnLine.put(columns[j], colVal);
                        }
                    }

                }
                if (null != hit.get("_id")) {
                    filterdResultKV(null, list, returnLine, rc);
                }

            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
            LogUtils.errorMsg(e, "query muti table error!");
        }

        // SearchHits hits = requestBuilder.execute().actionGet().getHits();
        // total.set(hits.getTotalHits());
        // SearchHit[] searchHists = hits.getHits();
        // List<Map<String, Object>> list = new ArrayList<Map<String,
        // Object>>();
        // Map<String, Object> returnLine = null;
        // String colVal = null;
        // for (SearchHit hit : searchHists) {
        //
        // Map<String, Object> map = hit.getSource();
        // returnLine = new HashMap<String, Object>(columns.length);
        // for (int i = 0; i < columns.length; i++) {
        // if (null != hit.getId()) {
        // // if ("key".equals(columns[i]) && table instanceof
        // // HbaseTable) {
        // // colVal = hit.getId();
        // // } else
        // if ("_id".equals(columns[i])) {
        // colVal = hit.getId();
        // } else {
        // colVal = (String) map.get(columns[i]);
        // }
        // if (null != colVal && !"".equals(colVal)) {
        // // returnLine[i] = colVal;
        // returnLine.put(columns[i], colVal);
        // }
        // }
        //
        // }
        // if (null != hit.getId()) {
        // filterdResultKV(null, list, returnLine, rc);
        // }
        //
        // }

        cpm.close(ec);
        return list;
    }

    // private void updateSetting(String tableName, TransportClient client2)
    // throws BigDataWareHouseException {
    // try {
    // client2.admin().indices().updateSettings(Requests.updateSettingsRequest(tableName).settings(org.elasticsearch.common.settings.Settings.settingsBuilder()
    // .put("max_result_window", "100000000"))).get();
    // } catch (Exception e) {
    // throw new
    // BigDataWareHouseException(BigDataWareHouseException.UPDATE_SETTING_ERROR,e);
    // }
    // }

    /**
     * 内部递归条件
     * 
     * @Title: buildFilteredQuery
     * @Description: TODO
     * @param struct
     * @param requestBuilder
     * @param indexType
     * @param listConColNames
     * @param client
     * @return
     * @throws BigDataWareHouseException
     */
    private BoolQueryBuilder buildFilteredQuery(TableJoinStruct struct, CoordinateSearchRequestBuilder requestBuilder, String indexType,
            List<String> listConColNames, TransportClient client) throws BigDataWareHouseException {
        BoolQueryBuilder returnBuilder = null;
        TableJoin outerTable = struct.getOuterTable();
        TableJoin innerTable = struct.getInnerTable();
        // updateSetting(outerTable.getTableName(),client);

        if (innerTable instanceof TableJoinStruct) {
            TableJoinStruct innerStruct = (TableJoinStruct) innerTable;
            // requestBuilder.setIndices(outerTable.getTableName());

            returnBuilder = boolQuery()
                    .filter(solutions.siren.join.index.query.QueryBuilders.filterJoin(struct.getJoinCol())
                            .indices(innerStruct.getOuterTable().getTableName().toLowerCase()).types(innerStruct.getOuterTable().getTableName().toLowerCase())
                            .path(struct.getJoinCol())
                            .query(boolQuery().filter(buildFilteredQuery(innerStruct, requestBuilder, indexType, listConColNames, client))))
                    .filter(parseCons(indexType, listConColNames, outerTable.getCondition(), QueryBuilders.boolQuery(), null, null));

            // filterJoin(parseCons(indexType, listConColNames,
            // outerTable.getCondition(), QueryBuilders.boolQuery(), null,
            // null),
            // FilterBuilders.filterJoin(struct.getJoinCol()).indices(innerTable.getTableName().toLowerCase()).path(struct.getJoinCol())
            // .query(buildFilteredQuery(innerStruct, requestBuilder, indexType,
            // listConColNames)
            // ));
        } else {
            // requestBuilder.setIndices(outerTable.getTableName());
            // updateSetting(innerTable.getTableName(),client);
            returnBuilder = boolQuery()
                    .filter(solutions.siren.join.index.query.QueryBuilders.filterJoin(struct.getJoinCol()).indices(innerTable.getTableName().toLowerCase())
                            .types(innerTable.getTableName().toLowerCase()).path(struct.getJoinCol())
                            .query(boolQuery().filter(parseCons(indexType, listConColNames, innerTable.getCondition(), QueryBuilders.boolQuery(), null, null))))
                    .filter(parseCons(indexType, listConColNames, outerTable.getCondition(), QueryBuilders.boolQuery(), null, null));

            // filterJoin(parseCons(indexType, listConColNames,
            // outerTable.getCondition(), QueryBuilders.boolQuery(), null,
            // null),
            // FilterBuilders.filterJoin(struct.getJoinCol()).indices(innerTable.getTableName().toLowerCase()).path(struct.getJoinCol())
            // .query(parseCons(indexType, listConColNames,
            // innerTable.getCondition(), QueryBuilders.boolQuery(), null,
            // null)));
        }
        return returnBuilder;

    }

}
