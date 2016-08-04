/**
 * 
 */
package com.howbuy.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.common.HbConstants;
import com.howbuy.common.Util;

/**
 * @author qiankun.li
 * 
 */
public class BaseHbase {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(BaseHbase.class);

	static Configuration conf;

	private BlockingQueue<HTable> htables = new LinkedBlockingQueue<HTable>(20);

	private BlockingQueue<HTable> updatehtables = new LinkedBlockingQueue<HTable>(10);

	public void closeHtables() throws IOException {

		for (HTable h : htables) {
			h.close();
		}

		for (HTable h : updatehtables) {
			h.close();
		}
	}

	
	/**
	 * 初始化htable池
	 */
	public void initHtables() {
		conf = Util.getZkConfig();
		conf.set(HbConstants.REPLICATION, "1");

		for (int i = 0; i < 20; i++) {

			try {

				HTable table = new HTable(conf,
						Bytes.toBytes(HbConstants.TABLE));// HTabel负责跟记录相关的操作如增删改查等//
				table.setAutoFlush(false, true);
				table.setWriteBufferSize(10 * 1024 * 1024);
				htables.add(table);

			} catch (IOException e) {
				LOGGER.error("", e);
			}
		}

		for (int i = 0; i < 10; i++) {

			try {

				HTable table = new HTable(conf,
						Bytes.toBytes(HbConstants.TABLE_UPDATE));// HTabel负责跟记录相关的操作如增删改查等//
				table.setAutoFlush(false, true);
				table.setWriteBufferSize(10 * 1024 * 1024);
				updatehtables.add(table);

			} catch (IOException e) {
				LOGGER.error("", e);
			}
		}
	}

	/**
	 * 更新增量表
	 * 
	 * @param rowKeys
	 * @param tableUpdate
	 * @param familyNameUpdate
	 * @param columnMapUpdate
	 * @param valueMapUpdate
	 * @param fromTable
	 * @throws InterruptedException
	 */
	public void addupdate(String[] rowKeys, String tableUpdate,
			String familyNameUpdate, Map<String, String[]> columnMapUpdate,
			Map<String, Object[]> valueMapUpdate, String fromTable)
			throws InterruptedException {

		HTable htable = updatehtables.take();

		if (htable != null) {
			doadd(rowKeys, tableUpdate, familyNameUpdate, columnMapUpdate,
					valueMapUpdate, fromTable, htable, 2);
		}

	}

	public void add(String[] rowKeys, String tableName, String familyName,
			Map<String, String[]> columnMap, Map<String, Object[]> valueMap,
			String fromTable) throws InterruptedException {

		HTable htable = htables.take();

		if (htable != null) {
			doadd(rowKeys, tableName, familyName, columnMap, valueMap,
					fromTable, htable, 1);
		}

	}

	public void doadd(String[] rowKeys, String tableName, String familyName,
			Map<String, String[]> columnMap, Map<String, Object[]> valueMap,
			String fromTable, HTable htable, int type){

		try {

			HColumnDescriptor[] columnFamilies = htable.getTableDescriptor()
					.getColumnFamilies(); // 获取所有的列族
			List<Put> puts = new ArrayList<Put>();
			for (String rowKey : rowKeys) {

				StringBuffer sb = new StringBuffer(rowKey);
				// 设置rowkey倒序存入,增加散列
				String rk = sb.reverse().toString();
				Put put = new Put(Bytes.toBytes(rk));
				put.setWriteToWAL(false);
				for (int i = 0; i < columnFamilies.length; i++) {
					String familyNameVar = columnFamilies[i].getNameAsString(); // 获取列族名
					if (familyNameVar.equals(familyName)) { // 对应列族put数据
						String[] columns = columnMap.get(rowKey);
						Object[] values = valueMap.get(rowKey);
						int cLen = columns.length;
						for (int j = 0; j < cLen; j++) {
							Object v = values[j];
							if (v == null) {
								put.add(Bytes.toBytes(familyName),
										Bytes.toBytes(columns[j]), null);
							} else if (v instanceof Number) {
								if (NumberUtils.isDigits(v.toString())) {
									put.add(Bytes.toBytes(familyName),
											Bytes.toBytes(columns[j]),
											Bytes.toBytes(Long.valueOf(
													v.toString()).toString()));
								} else if (!NumberUtils.isDigits(v.toString())) {
									put.add(Bytes.toBytes(familyName),
											Bytes.toBytes(columns[j]),
											Bytes.toBytes(Double.valueOf(
													v.toString()).toString()));
								}
							} else if (v instanceof String) {
								put.add(Bytes.toBytes(familyName),
										Bytes.toBytes(columns[j]),
										Bytes.toBytes(v.toString()));
							}else if(v instanceof Clob){
								try {
									Clob c =(Clob)v;
									String cs = clobToString(c);
									put.add(Bytes.toBytes(familyName),
											Bytes.toBytes(columns[j]),
											Bytes.toBytes(cs));
								} catch (Exception e) {
									LOGGER.error("Clob add error",e);
								}
							}
							puts.add(put);
						}
					}
				}
			}
			int num = 10000;
			int time = puts.size() / num;
			int more = puts.size() % num;
			for (int m = 0; m < time; m++) {
				int fromIndex = num * m;
				int toIndex = num * (m + 1);
				long start = System.currentTimeMillis();
				htable.put(puts.subList(fromIndex, toIndex));
				long end = System.currentTimeMillis();
				LOGGER.info("put data from {} to {},size:{},execute in {} ms ",
						new Object[] { fromTable, tableName, num, end - start });
			}
			if (more >= 1) {
				int fromIndex = num * time;
				int toIndex = puts.size();
				long start = System.currentTimeMillis();
				htable.put(puts.subList(fromIndex, toIndex));
				htable.flushCommits();
				long end = System.currentTimeMillis();
				LOGGER.info(
						"put data from {} to {},size:{},execute in {} ms ",
						new Object[] { fromTable, tableName, more, end - start });
			}

		} catch (IOException e) {
			LOGGER.error("add data error", e);
		} finally {
			if (htable != null) {
				if (type == 1)
					htables.add(htable);
				else
					updatehtables.add(htable);
			}
		}
	}

	private String clobToString(Clob clob){
		long start = System.currentTimeMillis();
        String reString = "";
        Reader is = null;
        try {
            is = clob.getCharacterStream();
        } catch (SQLException e) {
        	LOGGER.error("clobToString error", e);
        	return reString;
        }
        // 得到流
        BufferedReader br = new BufferedReader(is);
        String s = null;
        try {
            s = br.readLine();
        } catch (IOException e) {
        	LOGGER.error("clobToString error", e);
        	return reString;
        }
        StringBuffer sb = new StringBuffer();
        while (s != null) {
            //执行循环将字符串全部取出付值给StringBuffer,由StringBuffer转成STRING
            sb.append(s);
            try {
                s = br.readLine();
            } catch (IOException e) {
            	LOGGER.error("clobToString error", e);
            }
        }
        reString = sb.toString();
        long end = System.currentTimeMillis();
        System.out.println("clobToString cast time is "+(end-start));
        return reString;

	}
	
	/*private List<Object[]> cut(Object[] array) {

		List<Object[]> list = new ArrayList<Object[]>();
		if (null != array) {
			final int size = 2;
			int len = array.length;
			if (len <= 10) {
				list.add(array);
			} else {
				String[] str = new String[size];
				int index = 0;
				for (int i = 0; i < len; i++) {
					Object ar = array[i];
					if (null == ar) {
						ar = new String("");
					}
					String a = ar.toString();
					if (index == size) {
						index = 0;
						list.add(str);
						str = new String[size];
					} else {
						str[index++] = a;
					}
				}
			}
		}
		return list;
	}*/

	public List<Map<String, Object>> scan(String tableName, String familyName) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			HTablePool pool = new HTablePool(conf, 10);
			HTableInterface table = pool.getTable(tableName);
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(familyName));
			ResultScanner scanner = table.getScanner(scan);
			for (Result r : scanner) {
				Map<String, Object> map = new HashMap<String, Object>();
				// String rowKey = null;
				for (KeyValue kv : r.list()) {
					// rowKey = Bytes.toString(kv.getRow());
					// String family = Bytes.toString(kv.getFamily());
					String qualifier = Bytes.toString(kv.getQualifier());
					String value = Bytes.toString(kv.getValue());
					map.put(qualifier, value);
				}
				list.add(map);
			}
			pool.close();
		} catch (IOException e) {
			LOGGER.error("", e);
		}
		return list;
	}

	/**
	 * 根据rowkeylist获取hbase表中数据
	 * 
	 * @param rowList
	 * @return
	 */
	public List<Map<String, String>> getByRow(String tableName,
			String familyName, List<String> rowList) {
		List<Map<String, String>> result = new ArrayList<Map<String, String>>();
		if (CollectionUtils.isNotEmpty(rowList)) {

			HTablePool pool = new HTablePool(conf, 1000);
			HTableInterface table = pool.getTable(tableName);
			try {
				for (String row : rowList) {
					Map<String, String> rowMap = new HashMap<String, String>();
					if(StringUtils.isNotBlank(row)){
						Get get = new Get(row.getBytes());// 根据rowkey查询
						if (StringUtils.isNotBlank(familyName))
							get.addFamily(Bytes.toBytes(familyName));
						Result r = table.get(get);
						if (r != null) {
							byte[] rowB = r.getRow();
							if(null!=rowB){
								LOGGER.debug("rowkey:" + new String(r.getRow()));
								for (KeyValue keyValue : r.raw()) {
									String qualifier = new String(keyValue.getQualifier());
									String value = Bytes.toString(keyValue.getValue());
									rowMap.put(qualifier, value);
								}
								result.add(rowMap);
							}else{
								LOGGER.warn("row:"+row+",get no data from hbase");
							}
						}
					}
				}
			} catch (IOException e) {
				LOGGER.error("", e);
			} finally {
				try {
					table.close();
					pool.close();
				} catch (IOException e) {
					LOGGER.error("", e);
				}
			}
		}
		return result;
	}

	/**
	 * 清空表数据
	 * 
	 * @param tableName
	 * @return
	 */
	public boolean truncateTable(String tableName) {
		try {
			// 首先要获得table的建表信息
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor td = admin.getTableDescriptor(Bytes
					.toBytes(tableName));

			// 删除表
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			// 重新建表
			admin.createTable(td);
			admin.close();
		} catch (IOException e) {
			LOGGER.error("truncateTable {} error", tableName, e);
			return false;
		}
		return true;
	}

	public void testGet(String custNo) {
		HTablePool pool = new HTablePool(conf, 1000);
		HTableInterface table = pool.getTable("DIS_CUST_LABLE_INFO");
		try {
			Get get = new Get(custNo.getBytes());// 根据rowkey查询
			Result r = table.get(get);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				String key = new String(keyValue.getQualifier());
				System.out.println("列：" + key + ",值:"
						+ Bytes.toString(keyValue.getValue()));
			}
		} catch (IOException e) {
			LOGGER.error("", e);
		} finally {
			try {
				pool.close();
			} catch (IOException e) {
				LOGGER.error("", e);
			}
		}

	}

	public static void main(String[] args) {
		//BaseHbase b = new BaseHbase();

		// b.scan("DIS_CUST_LABLE_INFO","CUST_LAB_CF");

		/*
		 * System.out.println(NumberUtils.isDigits("75.833433"));
		 * System.out.println(NumberUtils.isDigits("0.2833433"));
		 */
		/*
		 * System.out.println(new
		 * StringBuffer("1000479684").reverse().toString());
		 */
		/*String s = new StringBuffer("1000454126").reverse().toString();
		b.testGet(s);*/
	}

}
