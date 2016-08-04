package com.howbuy.oracle2hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.dao.CustLableDao;
import com.howbuy.hbasetokafka.HbaseToKafka;

public class CustLableDataToHbase {
	
	private Logger logger = LoggerFactory.getLogger(CustLableDataToHbase.class);

	private CustLableDao custLableDao ;

	private HbaseToKafka hbaseToKafka ;

	
	public void initColData(){
		custLableDao.initColData();
	}
	
	public CustLableDataToHbase(){
		
		custLableDao = new CustLableDao();
		
		hbaseToKafka = new HbaseToKafka();
		
		
	}
	
	
	/**
	 * 增量更新
	 * 
	 * @param beginDate
	 * @param endDate
	 * @throws IOException 
	 */
	@SuppressWarnings("serial")
	public void oracleToHbase(final String beginDate,final String endDate) throws IOException {
		// 清空Hbase 临时数据表
		if (custLableDao.truncateTableTemp()) {
			
			long start = System.currentTimeMillis();
			
			ExecutorService execService = Executors.newFixedThreadPool(3);
			
			Collection<Callable<Long>> taskList =new ArrayList<Callable<Long>>(){

				{
					
					add(new Callable<Long>() {

						@Override
						public Long call() throws Exception {
							
							return addToHbaseDimCustStaticInfo(beginDate, endDate);
						}
					});
					
					add(new Callable<Long>() {

						@Override
						public Long call() throws Exception {
							
							return 	addToHbaseDimCustAssetInfo(beginDate, endDate);
						}
					});
					
					add(new Callable<Long>(){

						@Override
						public Long call() throws Exception {
							return addToHbaseMidCustMixedLabel(beginDate, endDate);
						}
						
					});
				}
			};
			
			try {
				
				List<Future<Long>> futures = execService.invokeAll(taskList);
				
				long total = 0;
				
				for(Future<Long> f : futures){
					total += f.get();
				}
				
				logger.info("complete oracle to hbase,total:{} rows in milsec:{}",total,System.currentTimeMillis()-start);
				
				// 推送数据至kafka	// 推送最新数据至kafka
				hbaseToKafka.hbaseToKafka(false);
				
				logger.info("ALL TIME:" + (System.currentTimeMillis()-start));
				
				execService.shutdown();
				
			} catch (Exception e) {
				logger.error("",e);
			}finally{
				
				custLableDao.close();
			}
		
		}
	}

	/**
	 * @throws IOException 
	 * 初始化hbase数据
	 * @throws  
	 */
	@SuppressWarnings("serial")
	public void initHbaseData() throws IOException  {
		
		long start = System.currentTimeMillis();
		
		ExecutorService execService = Executors.newFixedThreadPool(3);
		
		Collection<Callable<Long>> taskList =new ArrayList<Callable<Long>>(){
			{
				
				add(new Callable<Long>() {

					@Override
					public Long call() throws Exception {
						
						return initDimCustStaticInfo();
					}
				});
				
				add(new Callable<Long>() {

					@Override
					public Long call() throws Exception {
						
						return initDimCustAssetInfo();
					}
				});
				
				add(new Callable<Long>(){

					@Override
					public Long call() throws Exception {
						return initMidCustMixedLabel();
					}
					
				});
			}
		};
		
		try {
			
			List<Future<Long>> futures = execService.invokeAll(taskList);
			
			long total = 0;
			
			for(Future<Long> f : futures){
				total += f.get();
			}
			
			logger.info("complete oracle to hbase,total:{} rows in milsec:{}",total,System.currentTimeMillis()-start);
			
			// 推送数据至kafka
			hbaseToKafka.hbaseToKafka(true);
			
			logger.info("ALL TIME:" + (System.currentTimeMillis()-start));
			
			execService.shutdown();
			
		} catch (Exception e) {
			logger.error("",e);
		}finally{
			
			custLableDao.close();
		}
	}

	private Long initDimCustStaticInfo() {
		return custLableDao.initDimCustStaticInfo(null,null);
	}

	private Long initDimCustAssetInfo() {
		return custLableDao.initDimCustAssetInfo();
	}

	private Long initMidCustMixedLabel() {
		return custLableDao.initMidCustMixedLabel(null,null);
	}

	/**
	 * 添加客户投资偏好到hbase
	 * 
	 * @param list
	 */
	public Long addToHbaseMidCustMixedLabel(String beginDate, String endDate) {
		return custLableDao.addMidCustMixedLabel(beginDate, endDate);
	}

	/**
	 * 添加客户资产属性到hbase
	 * 
	 * @param list
	 */
	public Long addToHbaseDimCustAssetInfo(String beginDate, String endDate) {
		return custLableDao.addDimCustAssetInfo(beginDate, endDate);

	}

	/**
	 * 添加客户账户属性和个人属性到hbase
	 * 
	 * @param list
	 */
	public Long addToHbaseDimCustStaticInfo(String beginDate, String endDate) {
		return custLableDao.addDimCustStaticInfo(beginDate, endDate);
	}

}
