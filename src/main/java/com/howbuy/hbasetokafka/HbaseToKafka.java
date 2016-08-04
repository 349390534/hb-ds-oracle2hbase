/**
 * 
 */
package com.howbuy.hbasetokafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.common.AppMapDataSingle;
import com.howbuy.common.HbConstants;
import com.howbuy.common.KafkaProducer;
import com.howbuy.common.Util;
import com.howbuy.hbase.BaseHbase;
import com.howbuy.util.JsonParse;

/**
 * @author qiankun.li
 *
 */
public class HbaseToKafka {

	private static final Logger logger = LoggerFactory.getLogger(HbaseToKafka.class);
	
	private KafkaProducer kafkaProducer;

	private BaseHbase baseHbase ;
	
	public HbaseToKafka() {
		
		this.baseHbase= new BaseHbase();
		try {
			
			kafkaProducer=new KafkaProducer();
			
		} catch (IOException e) {
			
			logger.error("init KafkaProducer error",e);
		}
		
		
	}
	/**
	 * 发布数据至kafka
	 * @param isUpdate
	 * @throws IOException 
	 */
	public void hbaseToKafka(boolean isall) throws IOException{
		
		final Map<String, String> fieldCodeMap=AppMapDataSingle.getDataSource().getColAsMap();
		
		if(isall){
			//默认推送增量
	        String tableName = HbConstants.TABLE;  
	        String cfName = HbConstants.FAMILY_NAME_STATIC;
	        rotateTable(tableName,cfName,100,new Callback() {
				
				@Override
				public void process(List<Map<String, String>> param) {
					List<Map<String, String>> newResult= convertMapToAs(param, fieldCodeMap);
					//发送kafka
					pubToKafka(newResult);
				}
			});
	    
		}else{//增量表
			
			final String tableName = HbConstants.TABLE_UPDATE;
			final String cfName = HbConstants.FAMILY_NAME_UPDATE;
		    final String tableNameAll = HbConstants.TABLE;  
			final String cfNameAll = HbConstants.FAMILY_NAME_STATIC;
			final List<String> custnos = new ArrayList<String>(200);
			QualifierFilter qfilter = new QualifierFilter(CompareOp.EQUAL, 
					new BinaryComparator(Bytes.toBytes(HbConstants.CONSCUSTNO)));
			
			rotateTable(tableName,cfName,100,new Callback() {
				
				@Override
				public void process(List<Map<String, String>> param) {
					Map<String, String>  m=null;
					if(CollectionUtils.isNotEmpty(param))
						m = param.get(0);
					if(null!=m){
						custnos.add(m.get(HbConstants.CONSCUSTNO));
					}
					
					if(custnos.size() == 200){
						
						//获取客户数据
						List<Map<String,String>> custList=baseHbase.getByRow(tableNameAll, cfNameAll, custnos);
						List<Map<String, String>> newResult= convertMapToAs(custList, fieldCodeMap);
						pubToKafka(newResult);
						
						custnos.clear();
					}
				}
			},qfilter);
			
			//剩余数据推入kafka
			if(custnos.size() > 0){
				//获取客户数据
				List<Map<String,String>> custList=baseHbase.getByRow(tableNameAll,cfNameAll, custnos);
				List<Map<String, String>> newResult= convertMapToAs(custList, fieldCodeMap);
				pubToKafka(newResult);
			}
			
		}
	}
	
	/*
	 * 遍历hbase表，push kafka
	 */
	public void rotateTable(String tablename,String cfName,long pagesize,Callback callback,Filter... filters) throws IOException{
		
		Configuration config = Util.getZkConfig();
	     FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
	     
	     if(null != filters){
	    	 
	    	 for(int i = 0; i < filters.length; i++){
	    		 
	    		 filterList.addFilter(filters[i]);
	    	 }
	     }
	     
		PageFilter filter = new PageFilter(pagesize);
		
		filterList.addFilter(filterList);
		
		byte[] POSTFIX = new byte[] { 0x00 };
		byte[] lastRow = null;  
        int totalRows = 0;
        HTable table = new HTable(config, tablename);
		
        
        long start = System.currentTimeMillis();
        
		while (true) {
			
            Scan scan = new Scan();
            scan.setFilter(filter);
            
            if(lastRow != null){
            	
                byte[] startRow = Bytes.add(lastRow,POSTFIX);  
                scan.setStartRow(startRow);  
            }
            
            ResultScanner scanner = table.getScanner(scan); 
            int localRows = 0;  
            Result result;  
            while((result = scanner.next()) != null){
            	List<Map<String,String>> mapList = new ArrayList<Map<String,String>>();
            	Map<byte[], byte[]> fmap = packFamilyMap(result,cfName);
				Map<String, String> rmap = packRowMap(fmap);
				Map<String,String> newm = new HashMap<String, String>();
				
				for(String col : rmap.keySet()){
					
					String value = rmap.get(col);
					if(StringUtils.isNotBlank(col)){
						if(StringUtils.isNotBlank(value))
							newm.put(col, value);
					}
				}
				mapList.add(newm);
				if(null != callback)
					callback.process(mapList);
            	
                totalRows ++; 
                localRows++;
                lastRow = result.getRow();  
            } 
            scanner.close();  
            if(localRows == 0) break;
            logger.info("current size :{}",totalRows);
        }
		table.close();		
		logger.info("get data from hbase total rows:{} in {} milsec", totalRows,System.currentTimeMillis()-start);
		
		
	}
	
	/**
	 * 发送对应的客户数据至kafka
	 * @param custNoList
	 */
	public void hbaseToKafka(List<String> custNoList){
		//处理参数数据，讲其倒转
		List<String> rowKeyList = new ArrayList<String>();
		if(CollectionUtils.isNotEmpty(custNoList)){
			for(String cust:custNoList){
				rowKeyList.add(StringUtils.reverse(cust));
			}
			//获取客户数据
			List<Map<String,String>> custList=baseHbase.getByRow(HbConstants.TABLE_UPDATE, null, rowKeyList);
			Map<String,String> fieldCodeMap=AppMapDataSingle.getDataSource().getColAsMap();
			List<Map<String, String>> newResult= convertMapToAs(custList, fieldCodeMap);
			pubToKafka(newResult);
		}
	}
	
	private void pubToKafka(List<Map<String, String>> newResult){
		for(Map<String, String> en:newResult){
			pubToKafka(en);
		}
	}
	
	private void pubToKafka(Map<String,String> map){
		
		String json = JsonParse.objToJsonStr(map);
		String key = System.currentTimeMillis()+"";
		kafkaProducer.pubMessage(key, json);
		
	}
	
	private List<Map<String, String>> convertMapToAs(List<Map<String, String>> result,Map<String,String> fieldCodeMap){
		List<Map<String, String>> cmapList = new ArrayList<Map<String,String>>(result.size());
		for(Map<String, String> m:result){
			Set<Entry<String, String>> set=m.entrySet();
			Map<String,String> newm = new HashMap<String, String>();
			for(Entry<String, String> en:set){
				String col = en.getKey();
				String value=en.getValue();
				Object colAs = fieldCodeMap.get(col);
				if(null!=colAs){
					if(StringUtils.isNotBlank(value)){
						newm.put(colAs.toString(), value);
					}
				}
			}
			cmapList.add(newm);
		}
		return cmapList;
	}
	
	
	/**
	 * 封装每行数据
	 */
	private static Map<String, String> packRowMap(Map<byte[], byte[]> dataMap) {
		Map<String, String> map = new LinkedHashMap<String, String>();

		for (byte[] key : dataMap.keySet()) {

			byte[] value = dataMap.get(key);

			map.put(toStr(key), toStr(value));

		}
		return map;
	}
	
	/**
	 * 根据列族封装配置的所有字段列族
	 */
	private static Map<byte[], byte[]> packFamilyMap(Result result,String columnFamily) {
		Map<byte[], byte[]> dataMap = null;
		dataMap = new LinkedHashMap<byte[], byte[]>();
		dataMap.putAll(result.getFamilyMap(getBytes(columnFamily)));
		
		return dataMap;
	}
	
	private static String toStr(byte[] bt) {
		return Bytes.toString(bt);
	}
	
	/* 转换byte数组 */
	public static byte[] getBytes(String str) {
		if (str == null)
			str = "";

		return Bytes.toBytes(str);
	}
	
}
