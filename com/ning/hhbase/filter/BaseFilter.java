/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午3:37:19
 * @version V1.0
 */
package com.ning.hhbase.filter;

import java.util.Map;

/**
 * @ClassName: BaseFilter
 * @Description: TODO
 * @author ningyexin
 * @date 2016年8月15日 下午3:39:55
 *
 **/
public interface BaseFilter {
    
    public boolean isRecordQualify(Object[] record);

    public boolean isRecordQualifyResultKV(Map<String, Object> record);

}
