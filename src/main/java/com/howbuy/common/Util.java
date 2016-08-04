/**
 * 
 */
package com.howbuy.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author qiankun.li
 * 
 */
public abstract class Util {
	
	public static boolean isEmpty(String val) {

		if (StringUtils.isEmpty(val) || "null".equals(val))
			return true;
		return false;
	}
	
	/**返回一个config对象
	 * @return 
	 */
	public static Configuration getZkConfig() {
		Configuration config = HBaseConfiguration.create();
		config.set(HbConstants.ZK, SysconfProperty.getSysProperty()
				.getProperties().getProperty(HbConstants.ZK));
		config.set(HbConstants.ZK_PORT, SysconfProperty.getSysProperty()
				.getProperties().getProperty(HbConstants.ZK_PORT));
		return config;

	}
}
