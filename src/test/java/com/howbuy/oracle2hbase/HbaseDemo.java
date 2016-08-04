package com.howbuy.oracle2hbase;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.howbuy.hbase.BaseHbase;
import com.sun.imageio.plugins.png.RowFilter;

public class HbaseDemo {

	private Configuration conf = null;
	
	@Before
	public void init(){
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.70.70.25:2181,10.70.70.27:2181,10.70.70.28:2181,10.70.70.23:2181,10.70.70.24:2181");
	}
	
	@Test
	public void testList(){
		List<String> a = new ArrayList<String>(500);
		a.add("1");
		System.out.println(a.size());
		for(String s:a){
			System.out.println("-"+s+"-");
			System.out.println("-");
		}
	}
	
	
	@Test
	public void testDrop() throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable("account");
		admin.deleteTable("account");
		admin.close();
	}
	
	@Test
	public void testPut() throws Exception{
		String prefix = "rk";
		
		HTable table = new HTable(conf, "test_tab");
		for(int i = 1; i < 500; i++){
			
			String rk = prefix + StringUtils.leftPad(i + "", 5, '0');
			
			Put put = new Put(Bytes.toBytes(rk));
			
			put.add(Bytes.toBytes("test"), Bytes.toBytes("name"), Bytes.toBytes("liuyan" + i));
			
			table.put(put);
			
//			if(i % 1000 == 0)
//				table.flushCommits();
		}
		table.close();
	}
	
	@Test
	public void testGet() throws Exception{
		//HTablePool pool = new HTablePool(conf, 10);
		//HTable table = (HTable) pool.getTable("user");
		HTable table = new HTable(conf, "account");
		Get get = new Get(Bytes.toBytes("rk00000099"));
		//get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
//		get.setMaxVersions(5);
		
		Result result = table.get(get);
//		result.getValue(family, qualifier);
		for(KeyValue kv : result.list()){
			String family = new String(kv.getFamily());
			System.out.println(family);
			String qualifier = new String(kv.getQualifier());
			System.out.println(qualifier);
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}
	
	@Test
	public void testScan() throws Exception{
		
		long start = System.currentTimeMillis();
		
		HTablePool pool = new HTablePool(conf, 10);
		HTableInterface table = pool.getTable("DIS_CUST_LABLE_INFO");
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("CUST_LAB_CF"));

		FilterList filterList = new FilterList();
//		filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"),  
//                Bytes.toBytes("name"),
//                CompareOp.EQUAL,Bytes.toBytes("liuyan111")
//                ));
		scan.setFilter(filterList);
		
//		RowFilter filter = new rowfilt
		
		scan.setBatch(500);
//		scan.set
		
		ResultScanner scanner = table.getScanner(scan);
		for(Result r : scanner){
			/**
			for(KeyValue kv : r.list()){
				String family = new String(kv.getFamily());
				System.out.println(family);
				String qualifier = new String(kv.getQualifier());
				System.out.println(qualifier);
				System.out.println(new String(kv.getValue()));
			}
			*/
//			System.out.println(new String(r.getRow()));
			byte[] value = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name2"));
			System.out.println(new String(value));
		}
		pool.close();
		
		System.out.println(System.currentTimeMillis()-start);
	}
	
	
	@Test
	public void testDel() throws Exception{
		HTable table = new HTable(conf, "user");
		Delete del = new Delete(Bytes.toBytes("rk0001"));
		del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));
		table.delete(del);
		table.close();
	}
	
	
	
	@Test
	public void createTable() throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor td = new HTableDescriptor("account");
		HColumnDescriptor cd = new HColumnDescriptor("info");
		cd.setMaxVersions(10);
		td.addFamily(cd);
		admin.createTable(td);
		admin.close();
	}
	@Test
	public void tesget2(){
		BaseHbase b = new BaseHbase();
		
//		b.scan("DIS_CUST_LABLE_INFO","CUST_LAB_CF");
		 
		/*System.out.println(NumberUtils.isDigits("75.833433"));
		System.out.println(NumberUtils.isDigits("0.2833433"));*/
		/*System.out.println(new StringBuffer("1000479684").reverse().toString());*/
		String s = new StringBuffer("1003398239").reverse().toString();
		b.testGet(s);
	}
	
	@Test
	public void testRe(){
		StringBuffer sb = new StringBuffer("1004977963");
		System.out.println(sb.reverse());
	}

}
