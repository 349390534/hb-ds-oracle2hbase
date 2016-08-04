/**
 * 
 */
package com.howbuy.common;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

/**
 * @author qiankun.li
 * 
 */
public class PoolingDataSource {

	private static PoolingDataSource dataSource = new PoolingDataSource();

	private PoolingDataSource() {
	}

	public static PoolingDataSource getDataSource() {
		return dataSource;
	}

	public DataSource getPoolDataSource(Properties properties) throws Exception{
		DataSource basicDataSource  = BasicDataSourceFactory.createDataSource(properties);
		return basicDataSource;
	}
	
	 
}
