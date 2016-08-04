/**
 * 
 */
package com.howbuy.main;

import java.io.IOException;

import com.howbuy.oracle2hbase.CustLableDataToHbase;

/**
 * @author qiankun.li
 *
 */
public class OracleToHbaseMain {

	private CustLableDataToHbase toHbase ;
	
	public OracleToHbaseMain() {
		toHbase = new CustLableDataToHbase();
	}
	
	/**增量更新 
	 * @param beginDate
	 * @param endDate
	 * @throws IOException 
	 */
	public void oracleToHbase(String beginDate,String endDate) throws IOException{
		//每次初始化列字典数据
		toHbase.initColData();
		toHbase.oracleToHbase(beginDate, endDate);
	}
	
	public static void main(String[] args) throws IOException {


		new OracleToHbaseMain().oracleToHbase("20160316", "20160317");
	}
}
