package com.howbuy.util;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.howbuy.common.Util;

public class HbasePage {
	private static Configuration config = null;
	private static HTablePool tp = null;
	static {
		// 加载集群配置
		config = Util.getZkConfig();
		// 创建表池(可伟略提高查询性能，具体说明请百度或官方API)
		tp = new HTablePool(config, 10);
	}

	/*
	 * 获取hbase的表
	 */
	public static HTableInterface getTable(String tableName) {

		if (StringUtils.isEmpty(tableName))
			return null;

		return tp.getTable(getBytes(tableName));
	}

	/* 转换byte数组 */
	public static byte[] getBytes(String str) {
		if (str == null)
			str = "";

		return Bytes.toBytes(str);
	}

	/**
	 * 查询数据
	 * 
	 * @param tableKey
	 *            表标识
	 * @param queryKey
	 *            查询标识
	 * @param startRow
	 *            开始行
	 * @param paramsMap
	 *            参数集合
	 * @return 结果集
	 */
	public static TBData getDataMap(String tableName, String startRow,
			String stopRow, Integer currentPage, Integer pageSize,String columnFamily)
			throws IOException {
		List<Map<String, String>> mapList = null;
		mapList = new LinkedList<Map<String, String>>();

		ResultScanner scanner = null;
		// 为分页创建的封装类对象，下面有给出具体属性
		TBData tbData = null;
		try {
			// 获取最大返回结果数量
			if (pageSize == null || pageSize == 0L)
				pageSize = 100;

			if (currentPage == null || currentPage == 0)
				currentPage = 1;

			// 计算起始页和结束页
			Integer firstPage = (currentPage - 1) * pageSize;

			Integer endPage = firstPage + pageSize;

			// 从表池中取出HBASE表对象
			HTableInterface table = getTable(tableName);
			// 获取筛选对象
			Scan scan = getScan(startRow, stopRow);
			// 给筛选对象放入过滤器(true标识分页,具体方法在下面)
			scan.setFilter(packageFilters(true));
			// 缓存1000条数据
			scan.setCaching(5000);
			scan.setCacheBlocks(false);
			scanner = table.getScanner(scan);
			
			int i = 0;
			List<byte[]> rowList = new LinkedList<byte[]>();
			
			// 遍历扫描器对象， 并将需要查询出来的数据row key取出
			for (Result result : scanner) {
			
				String row = toStr(result.getRow());
				
				if (i >= firstPage && i < endPage) {
					
					rowList.add(getBytes(row));
				}
				i++;
			}

			// 获取取出的row key的GET对象
			List<Get> getList = getList(rowList);
			
			Result[] results = table.get(getList);
			
			// 遍历结果
			for (Result result : results) {
				
				Map<byte[], byte[]> fmap = packFamilyMap(result,columnFamily);
				Map<String, String> rmap = packRowMap(fmap);
				mapList.add(rmap);
			}

			// 封装分页对象
			tbData = new TBData();
			tbData.setCurrentPage(currentPage);
			tbData.setPageSize(pageSize);
			tbData.setTotalCount(i);
			tbData.setTotalPage(getTotalPage(pageSize, i));
			tbData.setResultList(mapList);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeScanner(scanner);
		}

		return tbData;
	}

	private static int getTotalPage(int pageSize, int totalCount) {
		int n = totalCount / pageSize;
		if (totalCount % pageSize == 0) {
			return n;
		} else {
			return ((int) n) + 1;
		}
	}
	
	
	

	// 获取扫描器对象
	private static Scan getScan(String startRow, String stopRow) {
		Scan scan = new Scan();
		if(!StringUtils.isEmpty(startRow) && !StringUtils.isEmpty(stopRow)){
			
			
			scan.setStartRow(getBytes(startRow));
			scan.setStopRow(getBytes(stopRow));
		}

		return scan;
	}

	/**
	 * 封装查询条件
	 */
	private static FilterList packageFilters(boolean isPage) {
		FilterList filterList = null;
		// MUST_PASS_ALL(条件 AND) MUST_PASS_ONE（条件OR）
		filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//		Filter filter1 = null;
//		Filter filter2 = null;
//		filter1 = newFilter(getBytes("family1"), getBytes("column1"),
//				CompareOp.EQUAL, getBytes("condition1"));
//		filter2 = newFilter(getBytes("family2"), getBytes("column1"),
//				CompareOp.LESS, getBytes("condition2"));
//		filterList.addFilter(filter1);
//		filterList.addFilter(filter2);
		if (isPage) {
			filterList.addFilter(new FirstKeyOnlyFilter());
		}
		return filterList;
	}

	/*private static Filter newFilter(byte[] f, byte[] c, CompareOp op, byte[] v) {
		return new SingleColumnValueFilter(f, c, op, v);
	}*/

	private static void closeScanner(ResultScanner scanner) {
		if (scanner != null)
			scanner.close();
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

	/* 根据ROW KEY集合获取GET对象集合 */
	private static List<Get> getList(List<byte[]> rowList) {
		List<Get> list = new LinkedList<Get>();
		for (byte[] row : rowList) {
			
			Get get = new Get(row);
			
			get.addFamily(Bytes.toBytes("CUST_LAB_CF"));
//			get.addColumn(getBytes("info"), getBytes("name"));
//			get.addColumn(getBytes("family1"), getBytes("column2"));
//			get.addColumn(getBytes("family2"), getBytes("column1"));
			list.add(get);
		}
		return list;
	}
	
	
	/**
	 * 分页查询数据
	 * @param tableName
	 * @param currentPage
	 * @param pageSize
	 * @param columnFamily
	 * @return
	 * @throws IOException
	 */
	public TBData getDataMap(String tableName, Integer currentPage, Integer pageSize,String columnFamily) throws IOException{
		
		return getDataMap(tableName,null,null, currentPage, pageSize,columnFamily);
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

	public static void main(String[] args) throws IOException {
		// 拿出row key的起始行和结束行
		// #<0<9<:
//		String startRow = "rk00100";
//		String stopRow = "rk00900";
		
		String startRow = null;
		String stopRow = null;
		int currentPage = 1;
		int pageSize = 1000;
		// 执行hbase查询
		TBData tb = getDataMap("DIS_CUST_LABLE_INFO", startRow, stopRow, currentPage, pageSize,"CUST_LAB_CF");
		
		System.out.println(tb.getResultList().get(2));
		System.out.println("total:" + tb.getTotalCount());
		//int totalsize = tb.getTotalCount();
		
		while((tb.getPageSize() * currentPage) < 2500){
			
			currentPage++;
			
			tb = getDataMap("DIS_CUST_LABLE_INFO", startRow, stopRow, currentPage, pageSize,"CUST_LAB_CF");
			
			System.out.println(tb.getResultList().get(2));
		}
		
		System.out.println("totalpage:" + currentPage);

	}
	
	public static class TBData {
		private Integer currentPage;
		private Integer pageSize;
		private Integer totalCount;
		private Integer totalPage;
		private List<Map<String, String>> resultList;

		public Integer getCurrentPage() {
			return currentPage;
		}

		public void setCurrentPage(Integer currentPage) {
			this.currentPage = currentPage;
		}

		public Integer getPageSize() {
			return pageSize;
		}

		public void setPageSize(Integer pageSize) {
			this.pageSize = pageSize;
		}

		public Integer getTotalCount() {
			return totalCount;
		}

		public void setTotalCount(Integer totalCount) {
			this.totalCount = totalCount;
		}

		public Integer getTotalPage() {
			return totalPage;
		}

		public void setTotalPage(Integer totalPage) {
			this.totalPage = totalPage;
		}

		public List<Map<String, String>> getResultList() {
			return resultList;
		}

		public void setResultList(List<Map<String, String>> resultList) {
			this.resultList = resultList;
		}
	}
}


