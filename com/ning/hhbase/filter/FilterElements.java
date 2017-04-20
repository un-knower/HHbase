/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午3:37:19
 * @version V1.0
 */
package com.ning.hhbase.filter;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: FilterElements
 * @Description: TODO
 * @author ningyexin
 * @date 2016年8月15日 下午3:39:55
 *
 **/
public class FilterElements {
    
    private Map<String,FilterElement> filterElements = new HashMap<String,FilterElement>();

    public void add(FilterElement filter) {
        filterElements.put(filter.getFilterName(), filter);
    }

    public Map<String,FilterElement> getFilterElements() {
        return filterElements;
    }

    public void setFilterElements(Map<String,FilterElement> filterElements) {
        this.filterElements = filterElements;
    }

   

}
