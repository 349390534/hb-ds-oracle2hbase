/**
 * 
 */
package com.howbuy.common;

import java.util.HashMap;
import java.util.Map;

/**
 * 单例数据对象
 * 
 * @author qiankun.li
 * 
 */
public class AppMapDataSingle {

	private static AppMapDataSingle dataMap = new AppMapDataSingle();
	private AppMapDataSingle() {
	}

	public static AppMapDataSingle getDataSource() {
		return dataMap;
	}

	/**
	 *表对应别名 
	 */
	private Map<String,String> tabColMap=new HashMap<String, String>();
	/**
	 * 列对应别名
	 */
	private Map<String,String> colAsMap=new HashMap<String, String>();


	public Map<String, String> getTabColMap() {
		return tabColMap;
	}

	public void setTabColMap(Map<String, String> tabColMap) {
		this.tabColMap = tabColMap;
	}

	public Map<String, String> getColAsMap() {
		return colAsMap;
	}

	public void setColAsMap(Map<String, String> colAsMap) {
		this.colAsMap = colAsMap;
	}
}
