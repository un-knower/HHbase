/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午3:37:19
 * @version V1.0
 */
package com.ning.hhbase.filter;

/**
 * @ClassName: FilterElement
 * @Description: TODO
 * @author ningyexin
 * @date 2016年8月15日 下午3:39:55
 *
 **/
public class FilterElement {

    private String filterName;
    
    private String filterClass;

    private BaseFilter filterInstance;
    
    public String getFilterName() {
        return filterName;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }

    public String getFilterClass() {
        return filterClass;
    }

    public void setFilterClass(String filterClass) {
        this.filterClass = filterClass;
    }

    public BaseFilter getFilterInstance() {
        return filterInstance;
    }

    public void setFilterInstance(BaseFilter filterInstance) {
        this.filterInstance = filterInstance;
    }
    
}
