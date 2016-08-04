package com.howbuy.oracle2hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.howbuy.common.ConnectionDB;

public class DbTest {

	static void testDb(){

		ConnectionDB connectionDB= new ConnectionDB();
		String sql = "SELECT * FROM DIM_CUST_ASSET_INFO WHERE CONSCUSTNO = '1000441751'";
		List<Map<String, Object>> list = connectionDB.excuteQuery(sql, null);
		
		for(Map<String,Object> map : list){
			Set<Entry<String, Object>> sn =  map.entrySet();
			for(Entry<String, Object> en:sn){
				String c = en.getKey();
				Object v = en.getValue();
				if(v instanceof Clob){
					try {
						Clob cl =(Clob)v;
						String cs = clobToString(cl);
						System.out.println(cs);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

	
	}
	
	static void testDt(){
		Date d = new Date();
		System.out.println(d.toString());
	}
	public static void main(String[] args) {
		testDt();
	}
	
	private static String clobToString(Clob clob){
		long start = System.currentTimeMillis();
        String reString = "";
        Reader is = null;
        try {
            is = clob.getCharacterStream();
        } catch (SQLException e) {
        	e.printStackTrace();
        	return reString;
        }
        // 得到流
        BufferedReader br = new BufferedReader(is);
        String s = null;
        try {
            s = br.readLine();
        } catch (IOException e) {
        	e.printStackTrace();
        	return reString;
        }
        StringBuffer sb = new StringBuffer();
        while (s != null) {
            //执行循环将字符串全部取出付值给StringBuffer由StringBuffer转成STRING
            sb.append(s);
            try {
                s = br.readLine();
            } catch (IOException e) {
            	e.printStackTrace();
            }
        }
        reString = sb.toString();
        long end = System.currentTimeMillis();
        System.out.println("clobToString cast time is "+(end-start));
        return reString;

	}

}
