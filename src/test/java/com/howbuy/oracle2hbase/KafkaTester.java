package com.howbuy.oracle2hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.howbuy.hbasetokafka.Callback;
import com.howbuy.hbasetokafka.HbaseToKafka;

public class KafkaTester {
	
	CustLableDataToHbase toHbase;
	
	@Before
	public void init() throws IOException{
		
		toHbase = new CustLableDataToHbase();
		toHbase.initColData();
	}
	
	
	@Test
	public void testToHbase() throws IOException{
		
		toHbase.initHbaseData();
	}

	@Test
	public void testKafka() throws IOException{
		
		HbaseToKafka hbaseToKafka = new HbaseToKafka();
		
		hbaseToKafka.hbaseToKafka(false);
	}
	
	@Test
	public void testrorate() throws IOException{
		
		HbaseToKafka hbaseToKafka = new HbaseToKafka();
		hbaseToKafka.rotateTable("DIS_CUST_LABLE_INFO", "CUST_LAB_CF", 200, new Callback() {
			
			@Override
			public void process(List<Map<String, String>> param) {
				
				
				System.out.println(param);
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	@Test
	public void test1(){
		
		System.out.println("asdfsfd");
	}
	
}
