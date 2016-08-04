/**
 * 
 */
package com.howbuy.common;

/**
 * @author qiankun.li
 * 
 */
public interface HbConstants {
	static final int PERPAGE = 1000;
	static final int SLEEPTIME = 2000;
	static final int HBASE_PAGE = 1000;
	static final String CONSCUSTNO = "CONSCUSTNO";
	static final int PERSEND = 1000;
	
	static final String ZK = "hbase.zookeeper.quorum";
	
	static final String ZK_PORT = "hbase.zookeeper.property.clientPort";
	
	static final String REPLICATION = "hbase.replication";
	
	final String TABLE = "DIS_CUST_LABLE_INFO";
	
	final String TABLE_UPDATE = "TEMP_CONSCUSTNO_UPDATE";

	final String FAMILY_NAME_STATIC = "CUST_LAB_CF";
	final String FAMILY_NAME_ASSET = "CUST_LAB_CF";
	final String FAMILY_NAME_LABLE = "CUST_LAB_CF";
	
	final String FAMILY_NAME_UPDATE = "CUST_CF_UPDATE";

	final String CODE = "CODE";
	final String CODE_OPTION = "CODE_OPTION";
	final String FIELS_GROUP = "FIELDS_GROUP";
	final String WEIGHT = "WEIGHT";

	final String DIM_CUST_STATIC_INFO = "DIM_CUST_STATIC_INFO";
	final String DIM_CUST_ASSET_INFO = "DIM_CUST_ASSET_INFO";
	final String MID_CUST_MIXED_LABEL = "MID_CUST_MIXED_LABEL";
}
