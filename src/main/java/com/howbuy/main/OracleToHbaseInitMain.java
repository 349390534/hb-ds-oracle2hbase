/**
 * 
 */
package com.howbuy.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.oracle2hbase.CustLableDataToHbase;

/**
 * @author qiankun.li
 * oracle2hbase全量同步，并推送至kafka
 *
 */
public class OracleToHbaseInitMain {
	
	private static Logger logger = LoggerFactory.getLogger(OracleToHbaseInitMain.class);

	private CustLableDataToHbase toHbase ;
	
	public OracleToHbaseInitMain() {
		toHbase = new CustLableDataToHbase();
	}
	/**
	 * 初始化方法
	 * @throws IOException 
	 */
	public void initOracleToHbaseData() throws IOException{
		//初始化列字典数据
		toHbase.initColData();
		toHbase.initHbaseData();
		
		
	}
	
	public static void main(String[] args) {
		OracleToHbaseInitMain hbaseInitMain = new OracleToHbaseInitMain();
		try {
			hbaseInitMain.initOracleToHbaseData();
		} catch (IOException e) {
			logger.error("",e);
			System.exit(1);
		}
	}
}
