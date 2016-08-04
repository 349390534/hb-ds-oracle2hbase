/**
 * 
 */
package com.howbuy.dao;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.common.AppMapDataSingle;
import com.howbuy.common.ConnectionDB;
import com.howbuy.common.HbConstants;
import com.howbuy.hbase.BaseHbase;

/**
 * @author qiankun.li
 *
 */
public class CustLableDao implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7394640469396721804L;

	private static final Logger logger = LoggerFactory.getLogger(CustLableDao.class);

	private ConnectionDB connectionDB;
	
	static final String COL_MID_CUST_MIXED_LABEL="CONSCUSTNO,CODE_OPTION,FIELDS_GROUP";
	
	private BaseHbase baseHbaseApi;
	
	public CustLableDao() {
		 baseHbaseApi = new BaseHbase();
		 baseHbaseApi.initHtables();
		 connectionDB = new ConnectionDB();
	}
	
	public void close() throws IOException{
		
		baseHbaseApi.closeHtables();
	}
	
	

	/**
	 *初始化列对应的表和列对应的别名数据
	 */
	public void initColData(){
		CustLableColResponse colResponse = getCustLableColMap();
		Map<String,String> colAsMap = colResponse.getColAsMap();
		AppMapDataSingle.getDataSource().setColAsMap(colAsMap);
		
		Map<String,String> colTabMap=colResponse.getColTabMap();
		AppMapDataSingle.getDataSource().setTabColMap(colTabMap) ;
		
	}
	
	/**查询客户投资偏好
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public Long addMidCustMixedLabel(String beginDate,String endDate){
		return initMidCustMixedLabel(beginDate, endDate);
	}
	
	
	private void addDataInitCustLable(List<Map<String, Object>> list,String fromTable) throws InterruptedException{
		if(CollectionUtils.isNotEmpty(list)){
			//客户号Map key:CONSCUSTNO值,value:{CONSCUSTNO:targetValue}
			Map<String,Map<String,Object>> custMap = new HashMap<String, Map<String,Object>>();
			
			//客户号Map key:CONSCUSTNO值,value:{key:group,value:[code_option1,code_optio2]}
			Map<String,Map<String,List<String>>> fgMap = new HashMap<String, Map<String,List<String>>>();
			
			for(Map<String, Object> map :list){
				Map<String,Object> colmap = null;
				Object conscustObj=getFromMap(map, HbConstants.CONSCUSTNO);
				Object code=getFromMap(map, HbConstants.CODE_OPTION);
				Object group=getFromMap(map,HbConstants.FIELS_GROUP);
				String conscustno = conscustObj.toString();
				colmap = custMap.get(conscustno.toString());
				if(null==colmap)
					colmap=new HashMap<String, Object>();
				if(colmap.get(HbConstants.CONSCUSTNO)==null){
					colmap.put(HbConstants.CONSCUSTNO, conscustno);
				}
				custMap.put(conscustno, colmap);
				if(null!=code && null!=group){
					String codeVar = code.toString();
					String groupStr = group.toString();
					if(StringUtils.isNotBlank(groupStr) && StringUtils.isNotBlank(codeVar)){
						Map<String,List<String>> fgmapCust = fgMap.get(conscustno);
						if(null==fgmapCust)
							fgmapCust = new HashMap<String,List<String>>();
						List<String> codeList = fgmapCust.get(groupStr);
						if(CollectionUtils.isEmpty(codeList))
							codeList = new ArrayList<String>();
						codeList.add(codeVar);
						fgmapCust.put(groupStr,codeList);
						fgMap.put(conscustno, fgmapCust);
					}else{
						logger.warn("group of code ["+codeVar+"] is null");
					}
				}else{
					logger.warn("group or code is null of conscustno["+conscustno+"]");
				}
			}
			
			List<Map<String,Object>> custList= new ArrayList<Map<String,Object>>();
			Set<Entry<String, Map<String,Object>>> set = custMap.entrySet();
			for(Entry<String, Map<String,Object>> en:set){
				String cust = en.getKey();
				Map<String,Object> map = en.getValue();
				if(StringUtils.isNotBlank(cust)){
					Map<String,Object> codeMap = getCodesFromCustGroup(cust, fgMap);
					//add Group Code
					map.putAll(codeMap);
					custList.add(map);
				}
			}
			addHbase(custList,HbConstants.FAMILY_NAME_LABLE,fromTable);
		}
	}
	
	
	private Map<String,Object> getCodesFromCustGroup(String conscustno,Map<String,Map<String,List<String>>> fgMap){
		Map<String,Object> codeMap = new HashMap<String, Object>();
		if(null!=fgMap && !fgMap.isEmpty()){
			Map<String, List<String>> cmap=fgMap.get(conscustno);
			if(cmap!=null){
				Set<Entry<String, List<String>>> cset= cmap.entrySet();
				for(Entry<String, List<String>> enc:cset){
					if(null!=enc){
						String cgroup = enc.getKey();
						List<String> cCodeList = enc.getValue();
						Collections.sort(cCodeList);
						String codes=StringUtils.join(cCodeList, ",");
						codeMap.put(cgroup, codes);
					}
				}
			}
		
		}
		return codeMap;
	}
	
	private Object getFromMap(Map<String, Object> map,String col){
		if(null==map )
			return null;
			
		Set<Entry<String, Object>> enSet = map.entrySet();
		for(Entry<String, Object> en:enSet){
			String colVar = en.getKey();
			Object val = en.getValue();
			if(colVar.equalsIgnoreCase(col)){
				return val;
			}
		}
		return null;
	}
	
	/**查询客户资产属性
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public Long addDimCustAssetInfo(String beginDate,String endDate){
		StringBuffer sqlCount = new StringBuffer("SELECT count(1) FROM DIM_CUST_ASSET_INFO T WHERE 1=1");
		//Object[] params = null;
		if(StringUtils.isNotBlank(beginDate) && StringUtils.isNotBlank(endDate)){
			sqlCount.append(" AND UPDT >="  +beginDate)
			.append(" AND UPDT <="  +endDate);
			//params = new Object[]{beginDate,beginDate};
		}
		Long count = connectionDB.count(sqlCount.toString(), null);
		logger.info("QUERY DIM_CUST_ASSET_INFO "+beginDate+","+beginDate+" COUNT IS "+count);
		
		ExecutorService ex = Executors.newFixedThreadPool(5);
		
		final BlockingQueue<List<Map<String, Object>>> bq = new LinkedBlockingQueue<List<Map<String,Object>>>(10);
		
		String col = AppMapDataSingle.getDataSource().getTabColMap().get(HbConstants.DIM_CUST_ASSET_INFO);
		if(StringUtils.isBlank(col))col="*";
		
		if(count>HbConstants.PERPAGE){
			Long pages = count/HbConstants.PERPAGE;
			Long more = count%HbConstants.PERPAGE;
			Long time = pages;
			if(more >=1){
				time +=1;
			}
			StringBuffer sbase = new StringBuffer("SELECT "+col+" FROM DIM_CUST_ASSET_INFO WHERE 1=1");
			if(StringUtils.isNotBlank(beginDate) && StringUtils.isNotBlank(endDate)){
				sbase.append(" AND UPDT >="  +beginDate);
				sbase.append(" AND UPDT <="  +endDate);
			}
			for(int page = 0;page<time;page++){
				String sql = "SELECT "+col+" FROM ( SELECT A.*, ROWNUM RN "+
						" FROM ("+sbase.toString()+") A WHERE ROWNUM <= %d ) WHERE RN >= %d";
				int from = HbConstants.PERPAGE*page +1;
				int end = HbConstants.PERPAGE*(page+1);
				sql = String.format(sql, end,from);
				try {
					
					final List<Map<String, Object>> listVar = connectionDB.excuteQuery(sql, null);
					
					bq.put(listVar);
					
					ex.execute(new Runnable() {
						
						@Override
						public void run() {
							
							try {
								addHbase(bq.take(),HbConstants.FAMILY_NAME_ASSET,"DIM_CUST_ASSET_INFO");
							} catch (Exception e) {
								logger.error("addHbase of addDimCustAssetInfo",e);
							}
							
						}
					});
					logger.info("search DIM_CUST_ASSET_INFO page:{},totalpage:{}",page,time);
				} catch (Exception e) {
					logger.error("DIM_CUST_ASSET_INFO error",e);
				}

			}
		}else{
			if(count>=1){
				String sql = "SELECT "+col+" FROM DIM_CUST_ASSET_INFO WHERE 1=1";
				if(StringUtils.isNotBlank(beginDate) && StringUtils.isNotBlank(endDate)){
					sql+=" AND UPDT >="  +beginDate;
					sql+=" AND UPDT <="  +endDate;
				}
				List<Map<String, Object>> result = connectionDB.excuteQuery(sql, null);
				try {
					addHbase(result,HbConstants.FAMILY_NAME_ASSET,"DIM_CUST_ASSET_INFO");
				} catch (Exception e) {
					logger.error("addHbase of addDimCustAssetInfo",e);
				}
				
				logger.info("search DIM_CUST_ASSET_INFO rows:{}",count);
			}
		}
		ex.shutdown();
		
		while(true){
			
			if(ex.isTerminated())
				break;
			
			try {
				TimeUnit.MILLISECONDS.sleep(200);
			} catch (InterruptedException e) {
			}
		}
		return count;
	}   
	
	
	/**查询客户账户属性和个人属性
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public Long addDimCustStaticInfo(String beginDate,String endDate){
		return initDimCustStaticInfo(beginDate, endDate);
	}
	
	
	
	/**
	 * 初始化资产表
	 */
	public Long initDimCustAssetInfo(){
		return addDimCustAssetInfo(null,null);
	}
	
	
	/**
	 * 初始化投资偏好表
	 */
	public Long initMidCustMixedLabel(String beginDate,String endDate){
		final String dt= " T.UP_DTM >= TO_DATE(?,'YYYYMMDD') AND T.UP_DTM <= TO_DATE(?,'YYYYMMDD')";
		//是否更新所有
		boolean isUpdateAll =true;
		if(StringUtils.isNotBlank(beginDate) && StringUtils.isNotBlank(endDate)){
			isUpdateAll = false;
		}
		
		StringBuffer custNoSql = new StringBuffer("SELECT DISTINCT T.CONSCUSTNO FROM MID_CUST_MIXED_LABEL T ");
		Object[] params = null;
		if(!isUpdateAll){
			params=new Object[]{beginDate,endDate};
			custNoSql.append(" WHERE ").append(dt);
		}
		final List<Map<String,Object>> custNoList = connectionDB.excuteQuery(custNoSql.toString(), params);
		if(CollectionUtils.isEmpty(custNoList)){
			logger.warn("no data need to update of table mid_cust_mixed_label");
			return 0l;
		}
		
		StringBuffer countSbSql = new StringBuffer("SELECT COUNT(1) FROM MID_CUST_MIXED_LABEL T ");
		if(!isUpdateAll){
			countSbSql.append(" WHERE ").append(dt);
		}
		
		Long count = connectionDB.count(countSbSql.toString(), params);
		logger.info("INIT MID_CUST_MIXED_LABEL "+beginDate+","+endDate+" COUNT IS "+count);
		long perReqNumMin = 200;//默认每次最少读取200个客户的数据
		long perReqNumMax = 1000;//默认每次最多读取1000个客户的数据
		long perReqNum = 0;
		//以每次处理10000条数据来计算
		final int perPageNum = 10000;
		if(null!=count){
			//计算平均每个客户有多少偏好
			Long avg=count/custNoList.size();
			perReqNum=Math.min(Math.max(perPageNum/avg,perReqNumMin),perReqNumMax);
		}
		String col = COL_MID_CUST_MIXED_LABEL;
		
		StringBuffer sqlSb = new StringBuffer("SELECT "+col+" FROM MID_CUST_MIXED_LABEL T WHERE T.CONSCUSTNO IN(%s)");
		/*
		 if(!isUpdateAll){
			sqlSb.append(" AND ").append(dt);
		}*/
	
		ExecutorService ex = Executors.newFixedThreadPool(5);
		
		final BlockingQueue<List<Map<String, Object>>> bq = new LinkedBlockingQueue<List<Map<String,Object>>>(10);
		int allCustNum = custNoList.size();
		Long pages = allCustNum/perReqNum;
		Long more = allCustNum%perReqNum;
		Long time = pages;
		if(more >=1){
			time +=1;
		}
		for(int page = 0;page<time;page++){
			long from = perReqNum*page;
			long end = perReqNum*(page+1);
			if(end>allCustNum)
				end=allCustNum;
			String custNoStr = getPerCustNo(custNoList, from, end);
			String sql = String.format(sqlSb.toString(),custNoStr);
			try {
				final List<Map<String, Object>> listVar= connectionDB.excuteQuery(sql, null);
				bq.put(listVar);
				//添加至hbase
				ex.execute(new Runnable() {
					@Override
					public void run() {
						try {
							addDataInitCustLable(bq.take(),"MID_CUST_MIXED_LABEL");
						} catch (Exception e) {
							logger.error("addDataInitCustLable of initMidCustMixedLabel",e);
						}
					}
				});
				logger.info("search MID_CUST_MIXED_LABEL page:{},totalpage:{}",page,time);
			} catch (Exception e1) {
				logger.error("search MID_CUST_MIXED_LABEL data error,{} ",e1);
			}
		}
	
		ex.shutdown();
		
		while(true){
			
			if(ex.isTerminated())
				break;
			
			try {
				TimeUnit.MILLISECONDS.sleep(200);
			} catch (InterruptedException e) {
			}
		}
		
		return count;
	}   
	
	
	private String getPerCustNo(List<Map<String,Object>> custMapList,long from,long end){
		String ct = "";
		if(CollectionUtils.isNotEmpty(custMapList)){
			List<Map<String,Object>> listVar = custMapList.subList(Long.valueOf(from).intValue(),Long.valueOf(end).intValue());
			List<String> stList= new ArrayList<String>(listVar.size());
			for(Map<String,Object> m:listVar){
				String cstno=m.get(HbConstants.CONSCUSTNO).toString();
				String cstnoVar = "'"+cstno+"'";
				stList.add(cstnoVar);
			}
			ct=StringUtils.join(stList, ",");
		}
		return ct;
	}
	
	/**
	 * 
	 * @return
	 */
	public Long initDimCustStaticInfo(String beginDate,String endDate){
		StringBuffer sqlCountSb = new StringBuffer("SELECT count(1) FROM DIM_CUST_STATIC_INFO T ");
		Object[] params = null;
		String col = AppMapDataSingle.getDataSource().getTabColMap().get(HbConstants.DIM_CUST_STATIC_INFO);
		if(StringUtils.isBlank(col))col="*";
		String sql = "SELECT "+col+" FROM ( SELECT A.*, ROWNUM RN "+
				" FROM (SELECT "+col+" FROM DIM_CUST_STATIC_INFO) A WHERE ROWNUM <= %d ) WHERE RN >= %d";
		
		StringBuffer sqlOneSb = new StringBuffer("SELECT "+col+" FROM DIM_CUST_STATIC_INFO T ");
		if(StringUtils.isNotBlank(beginDate)  && StringUtils.isNotBlank(endDate)){
			sqlCountSb.append("WHERE T.UPDT >= ? AND T.UPDT <= ?");
			sqlOneSb.append("WHERE T.UPDT >= ? AND T.UPDT <= ?");
			params=new Object[]{beginDate,endDate};
			sql = "SELECT "+col+" FROM ( SELECT A.*, ROWNUM RN "+
					" FROM (SELECT "+col+" FROM DIM_CUST_STATIC_INFO  WHERE UPDT >= ? AND UPDT <= ?) A WHERE ROWNUM <= %d ) WHERE RN >= %d";
		}
		String sqlCount = sqlCountSb.toString();
		Long count = connectionDB.count(sqlCount, params);
		logger.info("QUERY DIM_CUST_STATIC_INFO "+beginDate+","+endDate+" COUNT is "+count);
		
		ExecutorService ex = Executors.newFixedThreadPool(5);
		final BlockingQueue<List<Map<String, Object>>> bq = new LinkedBlockingQueue<List<Map<String,Object>>>(10);
		
		if(count>HbConstants.PERPAGE){
			Long pages = count/HbConstants.PERPAGE;
			Long more = count%HbConstants.PERPAGE;
			Long time = pages;
			if(more>=1){
				time+=1;
			}
			for(int page = 0;page<time;page++){
				int from = HbConstants.PERPAGE*page +1;
				int end = HbConstants.PERPAGE*(page+1);
				String sqlTemp = String.format(sql, end,from);
				try {
					final List<Map<String, Object>> listVar = connectionDB.excuteQuery(sqlTemp, params);
					bq.put(listVar);
					
					//添加至hbase
					ex.execute(new Runnable() {
						
						@Override
						public void run() {
							
							try {
								addHbase(bq.take(),HbConstants.FAMILY_NAME_STATIC,"DIM_CUST_STATIC_INFO");
							} catch (Exception e) {
								
								logger.error("addHbase of initDimCustStaticInfo",e);
							}
						}
					});
					logger.info("search DIM_CUST_STATIC_INFO page:{},totalpage:{}",page,time);
				} catch (Exception e1) {
					logger.error("DIM_CUST_STATIC_INFO data error ",e1.getMessage());
				}
			}
		}else{
			if(count>=1){
				String sql1 = sqlOneSb.toString();
				List<Map<String, Object>> result =connectionDB.excuteQuery(sql1, params);
				try {
					//添加至hbase
					addHbase(result,HbConstants.FAMILY_NAME_STATIC,"DIM_CUST_STATIC_INFO");
				} catch (Exception e1) {
					logger.error("addHbase of initDimCustStaticInfo",e1);
				}
				
				logger.info("search DIM_CUST_STATIC_INFO rows:{}",count);
			}
		}
		
		
		ex.shutdown();
		
		while(true){
			
			if(ex.isTerminated())
				break;
			
			try {
				TimeUnit.MILLISECONDS.sleep(200);
			} catch (InterruptedException e) {
			}
		}
		return count;
	}
	
	
	
	
	private void addHbase(List<Map<String, Object>> list,String family,String fromTable) throws InterruptedException{
		if(CollectionUtils.isNotEmpty(list)){
			String[] rowKeys= new String[list.size()];
			Map<String,String[]> columnMap = new HashMap<String,String[]>();
			Map<String,Object[]> valueMap = new HashMap<String,Object[]>();
			Map<String,String[]> columnMapUpdate = new HashMap<String,String[]>();
			Map<String,Object[]> valueMapUpdate = new HashMap<String,Object[]>();
			int rIndex = 0;
			for(Map<String, Object> map :list){
				Set<Entry<String, Object>> en = map.entrySet();
				String[] columns= new String[map.size()];
				Object[] values= new Object[map.size()];
				int cIndex = 0;
				int vIndex = 0;
				String rowKey = null;
				for(Entry<String, Object> e :en){
					String col = e.getKey();
					Object cv = e.getValue();
					if(HbConstants.CONSCUSTNO.equalsIgnoreCase(col)){
						rowKey=cv.toString();
						rowKeys[rIndex++] = rowKey;
						columns[cIndex++]=col;
						values[vIndex++]=cv;
					}else{
						columns[cIndex++]=col;
						values[vIndex++]=cv;
					}
				}
				if(null!=rowKey){
					columnMap.put(rowKey, columns);
					valueMap.put(rowKey, values);
					// for update
					columnMapUpdate.put(rowKey, new String[]{HbConstants.CONSCUSTNO});
					//将客户号倒序，以便根据rowkey查询
					valueMapUpdate.put(rowKey, new String[]{StringUtils.reverse(rowKey)});
				}
			}
			
			baseHbaseApi.add(rowKeys, HbConstants.TABLE, family, columnMap, valueMap,fromTable);
			
			baseHbaseApi.addupdate(rowKeys, HbConstants.TABLE_UPDATE, HbConstants.FAMILY_NAME_UPDATE, columnMapUpdate, valueMapUpdate,fromTable);
			
		}
	}
	
	
	/**
	 * 获取所有用户标签
	 * @return
	 */
	public CustLableColResponse getCustLableColMap(){
		Map<String,String> colTabMap = new HashMap<String, String>();
		Map<String,String> colAsMap = new HashMap<String, String>();
		Map<String,List<String>> tabColsMap = new HashMap<String, List<String>>();
		
		String sql="SELECT T.FIELD,T.ALIAS_NAME,T.TABLE_NAME FROM META_TAXONOMY_TAG_FIELD T";
		List<Map<String, Object>> result =connectionDB.excuteQuery(sql, null);
		CustLableColResponse colResponse = new CustLableColResponse();
		if(CollectionUtils.isNotEmpty(result)){
			for(Map<String,Object> map :result){
				String field = map.get("FIELD").toString();
				String aliasName = map.get("ALIAS_NAME").toString();
				String tableName = map.get("TABLE_NAME").toString();
				colAsMap.put(field, aliasName);
				List<String> colList = tabColsMap.get(tableName);
				if(CollectionUtils.isEmpty(colList))
					colList = new ArrayList<String>();
				colList.add(field);
				tabColsMap.put(tableName, colList);
			}
			Set<Entry<String, List<String>>> set=tabColsMap.entrySet();
			for(Entry<String, List<String>> en:set){
				String tab = en.getKey();
				List<String> cols = en.getValue();
				String tabcols = StringUtils.join(cols, ",");
				colTabMap.put(tab, tabcols);
			}
			colResponse.setColAsMap(colAsMap);
			colResponse.setColTabMap(colTabMap);
		}
		return colResponse;
	}
	
	/**
	 * 删除临时表数据
	 * @return
	 */
	public boolean truncateTableTemp(){
		return baseHbaseApi.truncateTable(HbConstants.TABLE_UPDATE);
	}
	
	class CustLableColResponse{
		private Map<String,String> colTabMap=new HashMap<String,String>();
		private Map<String,String> colAsMap=new HashMap<String,String>();
		public Map<String, String> getColTabMap() {
			return colTabMap;
		}
		public void setColTabMap(Map<String, String> colTabMap) {
			this.colTabMap = colTabMap;
		}
		public Map<String, String> getColAsMap() {
			return colAsMap;
		}
		public void setColAsMap(Map<String, String> colAsMap) {
			this.colAsMap = colAsMap;
		}
		
	}
}
