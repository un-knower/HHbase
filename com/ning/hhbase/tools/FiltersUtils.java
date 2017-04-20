/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午3:37:19
 * @version V1.0
 */
package com.ning.hhbase.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.filter.BaseFilter;
import com.ning.hhbase.filter.FilterElement;
import com.ning.hhbase.filter.FilterElements;


/**
 * @ClassName: FiltersUtils
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午3:37:19
 *
 **/
public final class FiltersUtils {

    /**
     * @Fields log : 日志
     **/
    private static Logger LOG = Logger.getLogger(FiltersUtils.class);

    /**
     * @Fields XMLFILE_PATH : xml路径
     **/
    private static String XMLFILE_PATH = "config/filters.xml";

    /**
     * @Fields instance : 单例
     **/
    private static FiltersUtils INSTANCE = new FiltersUtils();

    /**
     * @Fields filters : 过滤器
     **/
    private FilterElements filters = new FilterElements();

    /**
     * 创建一个新的实例 LoadFilters.
     * <p>
     * Title:
     * </p>
     * <p>
     * Description:
     * </p>
     **/
    private FiltersUtils() {
    }

    /**
     * @Title: getInstance
     * @Description: 获取实例
     * @return INSTANCE
     **/
    public static FiltersUtils getInstance() {
        return INSTANCE;
    }

    public void loadConfiguration() throws BigDataWareHouseException {
        try {

            // 加载配置文件
            Document document = loadXml();

            // 获取配置文件根节点
            Element rootElement = document.getRootElement();

            // 获取配置文件中filters节点；获取根节点时就是获取filters节点，无需再次获取filters
//            Element filtersElement = rootElement.element("filters");

            if (null != rootElement) {
                // 加载sys配置文件信息
                loadfilters(rootElement);
            }

        } catch (DocumentException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            throw BigDataWareHouseException.throwException(BigDataWareHouseException.CFG_INIT_FAIL);
        }
    }

    /**
     * @Title: loadFilter
     * @Description: TODO
     * @param filterElement
     * @return
     * @throws BigDataWareHouseException
     **/
    private FilterElement loadFilter(Element filterElement) throws BigDataWareHouseException {
        FilterElement filter = new FilterElement();

        for (Iterator it = filterElement.elementIterator(); it.hasNext();) {
            Element element = (Element) it.next();
            if ("name".equals(element.getName())) {
                filter.setFilterName(element.getText());
            } else if ("class".equals(element.getName())) {
                filter.setFilterClass(element.getText());

                try {
                    Class<BaseFilter> bf = (Class<BaseFilter>) Class.forName(filter.getFilterClass());
                    BaseFilter userFilter = bf.newInstance();
                    filter.setFilterInstance(userFilter);
                } catch (Exception e) {
                    throw BigDataWareHouseException.throwException(BigDataWareHouseException.FILTER_CLASS_DONT_EXSIST, filter.getFilterClass());
                }
            }
        }
        return filter;
    }

    /**
     * @Title: loadfilters
     * @Description: 加载结果过滤器
     * @param filtersElement
     * @throws BigDataWareHouseException
     */
    private void loadfilters(Element filtersElement) throws BigDataWareHouseException {

        // 遍历filters下的所有filter信息
        for (Iterator it = filtersElement.elementIterator(); it.hasNext();) {
            Element filterElement = (Element) it.next();
            FilterElement filter = loadFilter(filterElement);
            filters.add(filter);
        }
    }

    /**
     * 加载配置文件
     * 
     * @return Document
     * @throws DocumentException
     */
    private Document loadXml() throws DocumentException {
        SAXReader reader = new SAXReader();
        Document document = reader.read(getIS());
        return document;
    }

    /**
     * @Title: getIS
     * @Description: 获取xml流对象
     * @return
     **/
    private InputStream getIS() {
        InputStream in = null;
        try {
            in = new FileInputStream(XMLFILE_PATH);
        } catch (IOException e) {
            try {
                in = new FileInputStream(FiltersUtils.class.getClassLoader().getResource("/").getPath() + XMLFILE_PATH);
            } catch (Exception ex) {
                LOG.error("加载xml路径错误", e);
            }
        }
        return in;
    }

    public FilterElements getFilters() {
        return filters;
    }
    
    public static void main(String[] args) throws BigDataWareHouseException
    {
        FiltersUtils.INSTANCE.loadConfiguration();
    }
}
