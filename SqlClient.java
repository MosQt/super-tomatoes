package com.yucheng.cmis.dao;


import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ecc.emp.data.IndexedCollection;
import com.ecc.emp.data.KeyedCollection;
import com.ecc.emp.log.EMPLog;
import com.yucheng.cmis.dao.builder.SqlBuilderFactory;
import com.yucheng.cmis.dao.config.SqlConfigContext;
import com.yucheng.cmis.dao.util.PageInfo;
import com.yucheng.cmis.pub.CMISDomain;

/**
 * 
 * <p>
 * 		命名SQL客户端
 * </p>
 * 
 * 
 * <p>
 * 	<ul>潜规则一：SQL的resultClass是domain时：数据库表字段名映射到代码中DOMAIN的域名，字段名中下加线 _ 之后的第一个字母大写，其余字母小写;去掉下加线，
 * 			如果domain的字段命名不符合该规则，将不会被赋值
 *  <ul>
 *  <ul>添加命名SQL的日志输出,category=SQL add by yuhq at 2014-4-9 13:06:19</ul>
 *  <ul>修正：insertAuto方法PreparedStatement未关闭的BUG add by yuhq at 2014-5-22 18:00:27</ul>
 *  <ul>修正：updateAuto方法，如果是空值也会更新，用于将原有值的内容更新为空值 add by yuhq at 2014-7-29 9:47:17</ul>
 *  <ul>添加：isPrimaryKey方法，updateAuto方法不再更新主键值 add by yuhq at 2014-7-29 9:47:17</ul>
 *  <ul>修正：executeAuto方法，997、1015、1018、1014、1022、1025、1029行 
 *  		execute方法添加1064、1071、1074行修改1070、1077、1080、1084行
 *  		添加SQL日志输出SQL参数 并修改三项式校验参数是否为空 
 *  	add by di.cui 2015-5-12 9:47:17
 *  <ul>添加防止查询结果集较大，导致OOM错误的机制 add by yuhq at 2015-7-29 9:47:17</ul>
 *  <ul>修正：在DB2数据上更新数据时，字段类型是数值类型,值为null时报错，通过 ParameterMetaData判断当前字段类型，如果是字符串类型，
 *  		   将null值转为""，如果是非字符串则不转换，涉及executeAuto execute两个方法 add by yuhq at 2016-7-7 17:56:06</ul>
 *  <ul>修正：
 *  	1、针对于不同数据库的null值判断时，原使用ParameterMetaData的getParameterTypeName()方法，但Oracle的驱动未实现方法，所以判断逻辑改为：
 *  	      当 oracle数据库时统一将null转为""，其它数据库时将VARCHAR类型的字段null值转为""，数值类型不作转换
 *  	2、对非oracle数据据库的非varchar的特殊处理，如果值为""，则会转为null，防止数据库抛出异常	 add by yuhq at 2016-7-12 10:34:01</ul>
 *  <ul>优化SQL输出日志：1、将实际值替换SQL中的？以便于SQL的查看；2、添加sqlId的输出  add by yuhq at 2016-7-26 16:54:01<ul>
 *  <ul>升级：查询结果集最大记录数可通过cmis-dao.properties中通过limit.max.record.count中设置，默认为5000条，设置为负数不控制；
 *  	新增：queryFirst(String sqlId, Object parameter, boolean limitRecordCount, Connection conn)API,可以绕过最大结果集限制<ul>
 *  <ul>更新updateAuto方法，如果主键值为空，则不允许执行更新操作；add by yuhq at 2016-9-21 10:37:59</ul>
 *  <ul>queryList方法增加可以设置fetchSize的接口，对于分页查询默认设置fetchSize为pageSize</ul>
 *  <ul>getInsertSql方修改为如果值是null时也进行插入的sql的拼接</ul>
 *  <ul>增加方法fixDomain4Batch方法，调用后会将当前domain中不存在但是其他domain中存在的key值设置为空或者null</ul>
 *  <ul>修复批量插入和批量更新在was下不同domain的字段顺序可能不一致的问题，改为后边的domain都使用第一个domain的取值顺序获取value</ul>
 * </p>
 */
public class SqlClient {

	public static final String LOGTYPE = "SQL";
	private static int MAX_RECORD = 5000;// 查询结果集最大不超过MAX_RECORD

	private static final String SQL_TYPE_INSERT = "$insert";
	private static final String SQL_TYPE_UPDATE = "$update";
	private static final String SQL_TYPE_DELETE = "$delete";
	
	/**
	 *简单添加一个main方法
	 */
	 public static void main(String[] args){
	    System.out.println("hi~,github,this is my first change");
		System.out.println("hi~,github,this is my second change");
	 }
	 

	/**
	 * <p>根据实体对象自动组装SQL 进行数据库查询（无需写SQL）</p>
	 * @param domain 更新值 (注：自动时不支持基本型数据)
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws SQLException
	 */
	public static Object queryAuto(CMISDomain domain, Connection conn) throws SQLException {
		long star = System.currentTimeMillis();// 用于计算执行SQL耗时
		// 输出到日志文件中的SQL
		String exportSql = "";
		List fieldValueList = new ArrayList();
		PreparedStatement ps = null;
		ResultSet rs = null;
		/** 拼装SQL */
		StringBuffer sql = new StringBuffer();
		try {

			sql.append(getSelectSql(domain, fieldValueList));
			ps = conn.prepareStatement(sql.toString());

			/** 设置参数 */
			int parameterIndex = 1;
			if (fieldValueList != null && fieldValueList.size() > 0) {
				for (int n = 0; n < fieldValueList.size(); n++) {
					ps.setObject(parameterIndex++, fieldValueList.get(n));
				}
			}
			/** 执行SQL */
			rs = ps.executeQuery();
			Collection data = handleQueryResult(rs, domain.getClass().getName(), false, true, 0);

			// 将SQL中的？替换为实际的值，便于查看
			exportSql = resetExportSqlLog(sql.toString(), fieldValueList);
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, exportSql, (System.currentTimeMillis() - star));

			if (data != null && data.size() >= 0 && data.iterator().hasNext()) {
				return data.iterator().next();
			}
		} catch (SQLException e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, exportSql, -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, exportSql, -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
				}
			if (ps != null)
				try {
					ps.close();
				} catch (SQLException e) {
				}
		}
		return null;
	}

	/**
	 * <p>查询多条记录</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数(String,Map,Domain,KeyedCollection)
	 * @param conn 数据库连接
	 * @return 符合条件记录集
	 * @throws Exception
	 */
	public static Object queryFirst(String sqlId, Object parameter, Connection conn) throws SQLException {
		Collection coData = queryList(sqlId, parameter, 0, 0, conn);
		if (coData != null && coData.size() >= 0 && coData.iterator().hasNext()) {
			return coData.iterator().next();
		}
		return null;
	}

	/**
	 * <p>查询多条记录</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数(String,Map,Domain,KeyedCollection)
	 * @param limitRecrodCount true-启动最大查询条数限制 false-不启动
	 * @param conn 数据库连接
	 * @return 符合条件记录集
	 * @throws Exception
	 */
	public static Object queryList(String sqlId, Object parameter, boolean limitRecordCount, Connection conn) throws SQLException {
		return queryList(sqlId, parameter, null, null, 0, 0, limitRecordCount, 0, conn);
	}

	public static Object queryList(String sqlId, Object parameter, boolean limitRecordCount, int fetchSize, Connection conn) throws SQLException {
		return queryList(sqlId, parameter, null, null, 0, 0, limitRecordCount, fetchSize, conn);
	}

	/**
	 * <p>查询多条记录(用于执行分页查询)</p>
	 * @param sqlId   SQL的配置ID 
	 * @param parameter   输入配置WHERE条件子句参数(Map,KeyedCollection,CMISDomain,简单数据类型)
	 * @param pageInfo  context中翻页数据
	 * @param conn  数据库连接
	 * @return
	 * @throws SQLException
	 */
	public static Collection queryList(String sqlId, Object parameter, PageInfo pageInfo, Connection conn) throws SQLException {
		pageInfo.recordSize = SqlClient.queryCount(sqlId, parameter, conn);
		if (pageInfo.recordSize > 0) {
			return queryList(sqlId, parameter, null, null, pageInfo.pageIdx, pageInfo.pageSize, conn);
		} else {
			return new ArrayList();
		}
	}

	/**
	 * <p>查询多条记录</p>
	 * @param sqlId SQL的配置ID
	 * @param cmisDomain 输入配置WHERE条件子句参数
	 * @param conditionId 备选条件ID(在用于筛选备选条件时与parameter为互斥条件，优先级高于parameter)
	 * @param pubConditionId 公共备选条件ID (用于筛选备选条件，不与上述条件互斥)
	 * @param pageInfo context中翻页数据
	 * @param conn 数据库连接
	 * @return 符合条件的记录集
	 * @throws SQLException
	 */
	public static Collection queryList(String sqlId, CMISDomain cmisDomain, String[] conditionId, String[] pubConditionId, PageInfo pageInfo, Connection conn) throws SQLException {
		pageInfo.recordSize = SqlClient.queryCount(sqlId, cmisDomain, conditionId, pubConditionId, conn);
		if (pageInfo.recordSize > 0) {
			return queryList(sqlId, cmisDomain, conditionId, pubConditionId, pageInfo.pageIdx, pageInfo.pageSize, conn);
		} else {
			return new ArrayList();
		}
	}

	/**
	 * <p>查询多条记录</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数
	 * @param conn 数据库连接
	 * @return 符合条件记录集
	 * @throws Exception
	 */
	public static Collection queryList(String sqlId, Object parameter, Connection conn) throws SQLException {
		return queryList(sqlId, parameter, 0, 0, conn);
	}

	/**
	 * <p>查询多条记录，返回ICOL集合</p>
	 * <p>注：只有返回值类型为com.ecc.emp.data.KeyedCollection</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数
	 * @param conn 数据库连接
	 * @return 返回ICOL集合
	 * @throws SQLException
	 */
	private static IndexedCollection queryList4IColl(String sqlId, Object parameter, Connection conn) throws SQLException {
		Collection col = queryList(sqlId, parameter, 0, 0, conn);
		IndexedCollection iCol = new IndexedCollection();
		iCol.addAll(col);
		return iCol;
	}

	/**
	 * <p>查询多条记录，返回ICOL集合</p>
	 * <p>注：只有返回值类型为com.ecc.emp.data.KeyedCollection</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数
	 * @param pageNumber 当前页号
	 * @param pageSize  每页条数
	 * @param conn 数据库连接
	 * @return 返回ICOL集合
	 * @throws SQLException
	 */
	private static IndexedCollection queryList4IColl(String sqlId, Object parameter, int pageNumber, int pageSize, Connection conn) throws SQLException {
		Collection col = queryList(sqlId, parameter, pageNumber, pageSize, conn);
		IndexedCollection iCol = new IndexedCollection();
		iCol.addAll(col);
		return iCol;
	}

	/**
	 * <p>查询多条记录，返回ICOL集合</p>
	 * <p>注：只有返回值类型为com.ecc.emp.data.KeyedCollection</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数
	 * @param conditionId 可选条件ID
	 * @param pageNumber 当前页号
	 * @param pageSize  每页条数
	 * @param conn 数据库连接
	 * @return 返回ICOL集合
	 * @throws SQLException
	 */
	private static IndexedCollection queryList4IColl(String sqlId, Object parameter, String[] conditionId, int pageNumber, int pageSize, Connection conn) throws SQLException {
		Collection col = queryList(sqlId, parameter, pageNumber, pageSize, conn);
		IndexedCollection iCol = new IndexedCollection();
		iCol.addAll(col);
		return iCol;
	}

	/**
	 * <p>查询多条记录，并将结果存至文件</p>
	 * @param sqlId
	 * @param parameter
	 * @param conditionId
	 * @param start
	 * @param end
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	private static String queryList2File(String sqlId, Object parameter, String[] conditionId, int start, int end, Connection conn) throws SQLException {

		return "";
	}

	/**
	 * 装配分页信息
	 * @param pageNumber
	 * @param pageSize
	 * @return
	 */
	private static Map associatePageInfo(int pageNumber, int pageSize) {
		Map pageInfo = null;
		if (pageNumber > 0 && pageSize > 0) {
			pageInfo = new HashMap();
			pageInfo.put("pageNumber", pageNumber);
			pageInfo.put("pageSize", pageSize);
			int startRow = (pageNumber - 1) * pageSize + 1;
			pageInfo.put("startRow", startRow);
			int endRow = pageNumber * pageSize;
			pageInfo.put("endRow", endRow);
		}
		return pageInfo;
	}

	/**
	 * 封装查询返回结果
	 * @param sqlId  SQL的配置ID 
	 * @param rs  查询结果
	 * @param limitRecordCount true:启用最大结果限制，false：不启用
	 * @deprecated
	 * @return
	 * @throws SQLException
	 */
	private static Collection handleQueryResult(String sqlId, ResultSet rs, boolean limitRecordCount) throws SQLException {

		String resClassname = SqlConfigContext.getResultClass(sqlId);
		boolean isOnlyFirst = SqlConfigContext.getOnlyReturnFirst(sqlId);
		Collection colResult = null; // 结果集
		String[] colNameList = null;
		if (rs == null)
			return null;
		ResultSetMetaData rmeta = rs.getMetaData();
		int colCount = rmeta.getColumnCount();
		colNameList = new String[colCount];
		for (int c = 1; c <= colCount; c++) {
			colNameList[c - 1] = rmeta.getColumnName(c).toLowerCase();
		}
		colResult = new ArrayList();
		int count = 0;
		while (rs.next()) {
			HashMap _TmpData = new HashMap(colCount, 1f);
			for (int c = 1; c <= colCount; c++) {
				_TmpData.put(colNameList[c - 1], rs.getObject(c));
			}
			Object oo = null;
			if (isBasicDataType(resClassname)) {
				colResult.add(rs.getObject(1));
				break;
			} else if (resClassname.equals("com.ecc.emp.data.KeyedCollection")) {
				KeyedCollection kCol = new KeyedCollection();
				kCol.putAll(_TmpData);
				oo = kCol;
			} else if (resClassname.equals("java.util.HashMap") || resClassname.equals("java.util.Map")) {
				oo = _TmpData;
			} else {/** CMISDomain */
				Class cls;
				try {
					cls = Class.forName(resClassname);
					CMISDomain domain = (CMISDomain) cls.newInstance();
					domain.putData(_TmpData);
					oo = domain;
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new SQLException(e.getMessage());
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new SQLException(e.getMessage());
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new SQLException(e.getMessage());
				}
			}
			count++;
			// 添加防OOM机制
			if (isExceedMaxRecord(count, sqlId, limitRecordCount))
				break;
			colResult.add(oo);
			if (isOnlyFirst)
				break;
		}
		return colResult;
	}

	/**
	 * 封装查询返回结果
	 * @param sqlId  SQL的配置ID 
	 * @param rs  查询结果
	 * @param limitRecordCount true:启用最大结果限制，false：不启用
	 * @return
	 * @throws SQLException
	 */
	private static Collection handleQueryResult(ResultSet rs, String resultClassName, boolean isOnlyFirst, boolean limitRecordCount, int fetchSize) throws SQLException {

		Collection colResult = null; // 结果集
		String[] colNameList = null;
		if (rs == null)
			return null;
		// 如果需要限制最大返回数量而且fetchSize大于最大数量时，将fetchSize使用最大数量
//		int maxCount = getMaxCount();
//		if (limitRecordCount && maxCount < fetchSize) {
//			fetchSize = maxCount;
//		}
		rs.setFetchSize(fetchSize);

		ResultSetMetaData rmeta = rs.getMetaData();
		int colCount = rmeta.getColumnCount();
		colNameList = new String[colCount];
		for (int c = 1; c <= colCount; c++) {
			colNameList[c - 1] = rmeta.getColumnName(c).toLowerCase();
		}
		Class cls = null;
		try {
			cls = Class.forName(resultClassName);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			throw new SQLException(e1.getMessage());
		}
		colResult = new ArrayList();
		int count = 0;
		while (rs.next()) {
			HashMap _TmpData = new HashMap(colCount, 1f);
			for (int c = 1; c <= colCount; c++) {
				_TmpData.put(colNameList[c - 1], rs.getObject(c));
			}
			Object oo = null;
			if (isBasicDataType(resultClassName)) {
				colResult.add(rs.getObject(1));
				break;
			} else if (resultClassName.equals("com.ecc.emp.data.KeyedCollection")) {
				KeyedCollection kCol = new KeyedCollection();
				kCol.putAll(_TmpData);
				oo = kCol;
			} else if (resultClassName.equals("java.util.HashMap") || resultClassName.equals("java.util.Map")) {
				oo = _TmpData;
			} else {/** CMISDomain */

				try {
					CMISDomain domain = (CMISDomain) cls.newInstance();
					domain.putData(_TmpData);
					oo = domain;
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new SQLException(e.getMessage());
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new SQLException(e.getMessage());
				}
			}
			count++;
			// 添加防OOM机制
			if (isExceedMaxRecord(count, null, limitRecordCount))
				break;
			colResult.add(oo);
			if (isOnlyFirst)
				break;
		}
		return colResult;
	}

	/**
	 * <p>记录集超过MAX_RECORD，返回true,并输出日志</p>
	 * @param count 记录数
	 * @param sqlId 命名SQL的ID
	 * @param limitRecordCount true:启用最大结果限制，false：不启用
	 * @return true：超过最大记录数， false：没有超过最大记录数
	 * @throws CmisDaoException 
	 */
	private static boolean isExceedMaxRecord(int count, String sqlId, boolean limitRecordCount) throws SQLException {
		if (!limitRecordCount)
			return false;

		int maxCount = -1;

		// 如果设置为负数则不控制
		if (maxCount < 0)
			return false;

		if (count > maxCount) {
			EMPLog.log(LOGTYPE, EMPLog.INFO, 0, "当前SQL" + ((sqlId == null) ? "" : sqlId) + "查询结果集超过" + maxCount + "条，为防止OOM只返回" + maxCount + "条记录");
			return true;
		}

		return false;
	}

//	private static int getMaxCount() throws SQLException {
//		int maxCountInt = MAX_RECORD;
//		try {
//			String maxCount = CMISDaoPropertyManager.getInstance().getPropertyValue("limit.max.record.count");
//			if (maxCount != null && !maxCount.trim().equals("0")) {
//				maxCountInt = Integer.parseInt(maxCount);
//			}
//		} catch (Exception e) {
//			throw new SQLException(e);
//		}
//		return maxCountInt;
//	}
	/**
	 * <p>用于取单行第一列值（前提查询SQL只有一列结果）</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入配置WHERE条件子句参数(Map,KeyedCollection,CMISDomain,简单数据类型)
	 * @param conn 数据库连接
	 * @return 简单类型对象的字符串值
	 * @throws Exception
	 */
	public static Object querySingle(String sqlId, Object parameter, Connection conn) throws SQLException {
		ArrayList result = (ArrayList) queryList(sqlId, parameter, 0, 0, conn);
		Object resObj = null;
		if (result != null && result.size() > 0) {
			resObj = result.get(0);
		}
		if (resObj == null) {
			return null;
		}
		return resObj;
	}

	/**
	 * <p>用于执行分页查询的总记录数统计，其SQL也是配置在XML文件中，通常以"分页SQL标识+Count"标识</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入配置WHERE条件子句参数(Map,KeyedCollection,CMISDomain,简单数据类型)
	 * @param conn 数据库连接
	 * @return 记录总数
	 * @throws Exception
	 */
	public static int queryCount(String sqlId, Object parameter, Connection conn) throws SQLException {
		return queryCount(sqlId, parameter, null, null, conn);
	}

	/**
	 * <p>用于执行分页查询的总记录数统计，其SQL也是配置在XML文件中，通常以"分页SQL标识+Count"标识</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入配置WHERE条件子句参数(Map,KeyedCollection,CMISDomain,简单数据类型)
	 * @param conditionId 条件域的ID数组（xml中OPT_CONDITION的id）
	 * @param pubConditionId 公共条件域的ID数组（例如可用于记录级权限）
	 * @param conn 数据库连接
	 * @return 记录总数
	 * @throws Exception
	 */
	public static int queryCount(String sqlId, Object parameter, String[] conditionId, String[] pubConditionId, Connection conn) throws SQLException {
		int iTotal = 0;
		long star = System.currentTimeMillis();// 用于计算执行SQL耗时

		/** 解释SQL配置 , 得到原始配置*/
		String sqlOrigin = SqlConfigContext.getSqlFromConfig(sqlId, parameter, conditionId, pubConditionId);
		List paramValue = SqlConfigContext.getParamList(sqlId, parameter, conditionId, pubConditionId);

		sqlOrigin = SqlBuilderFactory.getBuilder().generateCountSql(sqlOrigin);

		/** 将OBJECT中的数据放至SQL中 */
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			/** 设置参数并执行SQL */
			ps = conn.prepareStatement(sqlOrigin);
			int paramIdx = 1;
			if (paramValue != null && paramValue.size() > 0) {
				for (int n = 0; n < paramValue.size(); n++) {
					ps.setObject(paramIdx++, paramValue.get(n));
				}
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				iTotal = rs.getInt(1);
				break;
			}

			// 将SQL中的？替换为实际的值，便于查看
			String exportSql = resetExportSqlLog(sqlOrigin, paramValue);

			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, exportSql, (System.currentTimeMillis() - star));
		} catch (SQLException e) {
			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
				}
			if (ps != null)
				try {
					ps.close();
				} catch (SQLException e) {
				}
		}

		return iTotal;
	}

	/**
	 * <p>查询多条记录</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入配置WHERE条件子句参数(Map,KeyedCollection,CMISDomain,简单数据类型)
	 * @param pageNumber 当前页号
	 * @param pageSize  每页条数
	 * @param conn 数据库连接
	 * @return 符合条件记录集
	 * @throws Exception
	 */
	public static Collection queryList(String sqlId, Object parameter, int pageNumber, int pageSize, Connection conn) throws SQLException {
		return queryList(sqlId, parameter, null, null, pageNumber, pageSize, conn);
	}

	/**
	 * <p>查询多条记录</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入配置WHERE条件子句参数(Map,CMISDomain,简单数据类型)
	 * @param conditionId 备选条件ID(在用于筛选备选条件时与parameter为互斥条件，优先级高于parameter)
	 * @param pubConditionId 公共备选条件ID (用于筛选备选条件，不与上述条件互斥)
	 * @param pageNumber 当前页号
	 * @param pageSize  每页条数
	 * @param conn 数据库连接
	 * @return 符合条件记录集
	 * @throws Exception
	 */
	public static Collection queryList(String sqlId, Object parameter, String[] conditionId, String[] pubConditionId, int pageNumber, int pageSize, Connection conn) throws SQLException {
		return queryList(sqlId, parameter, conditionId, pubConditionId, pageNumber, pageSize, true, pageSize, conn);
	}

	/**
	 * <p>查询多条记录</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入配置WHERE条件子句参数(Map,CMISDomain,简单数据类型)
	 * @param conditionId 备选条件ID(在用于筛选备选条件时与parameter为互斥条件，优先级高于parameter)
	 * @param pubConditionId 公共备选条件ID (用于筛选备选条件，不与上述条件互斥)
	 * @param pageNumber 当前页号
	 * @param pageSize  每页条数
	 * @param conn 数据库连接
	 * @return 符合条件记录集
	 * @throws Exception
	 */
	private static Collection queryList(String sqlId, Object parameter, String[] conditionId, String[] pubConditionId, int pageNumber, int pageSize, boolean limitRecordCount, int fetchSize,
			Connection conn) throws SQLException {
		long star = System.currentTimeMillis();
		// 输出到日志文件中的SQL
		String exportSql = "";
		/** 解释SQL配置 , 得到原始配置*/
		String sqlOrigin = SqlConfigContext.getSqlFromConfig(sqlId, parameter, conditionId, pubConditionId);
		List paramValue = SqlConfigContext.getParamList(sqlId, parameter, conditionId, pubConditionId);

		if (pageNumber > 0 && pageSize > 0) {// 如果分页
			sqlOrigin = SqlBuilderFactory.getBuilder().generatePaginateSql(sqlOrigin, pageNumber, pageSize);
		}

		/** 将OBJECT中的数据放至SQL中 */
		Collection colResult = null; // 结果集
		PreparedStatement ps = null;
		ResultSet rs = null;
		// StringBuffer sqlVlaue=new StringBuffer(" 当前SQL的值为<");
		try {
			/** 设置参数并执行SQL add by di.cui 2015/5/12 */
			ps = conn.prepareStatement(sqlOrigin);
			int paramIdx = 1;
			if (paramValue != null && paramValue.size() > 0) {
				for (int n = 0; n < paramValue.size(); n++) {
					ps.setObject(paramIdx++, paramValue.get(n));
					// sqlVlaue.append((paramValue.get(n)==null?"null":paramValue.get(n))+",");
				}
			}
			rs = ps.executeQuery();
			String resultClassName = SqlConfigContext.getResultClass(sqlId);
			boolean isOnlyFirst = SqlConfigContext.getOnlyReturnFirst(sqlId);
			colResult = handleQueryResult(rs, resultClassName, isOnlyFirst, limitRecordCount, fetchSize);

			// sqlVlaue.append(">");
			/** 执行SQL */

			// 将SQL中的？替换为实际的值，便于查看
			exportSql = resetExportSqlLog(sqlOrigin, paramValue);

			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, exportSql, (System.currentTimeMillis() - star));
		} catch (SQLException e) {
			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, exportSql, -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, exportSql, -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
				}
			if (ps != null)
				try {
					ps.close();
				} catch (SQLException e) {
				}
		}

		return colResult;
	}

	/**
	 * <p>执行数据插入操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int insert(String sqlId, Map value, Connection conn) throws SQLException {
		return execute(sqlId, null, value, conn);
	}

	/**
	 * <p>执行数据插入操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int insert(String sqlId, KeyedCollection value, Connection conn) throws SQLException {
		return execute(sqlId, null, value, conn);
	}

	/**
	 * <p>执行数据插入操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int insert(String sqlId, CMISDomain value, Connection conn) throws SQLException {
		return execute(sqlId, null, value, conn);
	}

	/**
	 * <p>执行数据插入操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @deprecated
	 * @return 影响记录条数
	 * @throws Exception
	 */
	private static int insert(String sqlId, Object parameter, Object value, Connection conn) throws SQLException {
		return execute(sqlId, parameter, value, conn);
	}

	/**
	 * <p>执行数据插入操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param value 更新值
	 * @param conditionId 可选条件ID
	 * @param conn 数据库连接
	 * @deprecated
	 * @return 影响记录条数
	 * @throws Exception
	 */
	private static int insert(String sqlId, Object parameter, Object value, String[] conditionId, Connection conn) throws SQLException {
		return execute(sqlId, parameter, value, conn);
	}

	/**
	 * <p>根据实体对象自动组装SQL插入数据库（无需写SQL）</p>
	 * @param sqlId SQL的配置ID
	 * @param value 更新值 (注：自动时不支持基本型数据)
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws SQLException
	 */
	public static int insertAuto(CMISDomain value, Connection conn) throws SQLException {
		return executeAuto(value, SQL_TYPE_INSERT, conn);
	}

	/**
	 * <p>根据配置SQL执行数据变更操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter SQL中只含单参数时，通过该参数传入单参数字符串值
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int update(String sqlId, String parameter, Object value, Connection conn) throws SQLException {
		return execute(sqlId, parameter, value, conn);
	}

	/**
	 * <p>根据配置SQL执行数据变更操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int update(String sqlId, Map parameter, Object value, Connection conn) throws SQLException {
		return execute(sqlId, parameter, value, conn);
	}

	/**
	 * <p>根据配置SQL执行数据变更操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int update(String sqlId, KeyedCollection parameter, Object value, Connection conn) throws SQLException {
		return execute(sqlId, parameter, value, conn);
	}

	/**
	 * <p>根据配置SQL执行数据变更操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param value 更新值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int update(String sqlId, CMISDomain parameter, Object value, Connection conn) throws SQLException {
		return execute(sqlId, parameter, value, conn);
	}

	/**
	 * <p>根据主键字段自动修改数据（无需写SQL）</p>	 
	 * @param value 更新值 (注：自动时不支持基本型数据)	
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws SQLException
	 */
	public static int updateAuto(CMISDomain value, Connection conn) throws SQLException {
		return executeAuto(value, SQL_TYPE_UPDATE, conn);
	}

	/**
	 * <p>根据配置SQL执行数据删除操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter  SQL中只含单参数时，通过该参数传入但参数字符串值
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int delete(String sqlId, String parameter, Connection conn) throws SQLException {
		return execute(sqlId, parameter, null, conn);
	}

	/**
	 * <p>根据配置SQL执行数据删除操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int delete(String sqlId, Map parameter, Connection conn) throws SQLException {
		return execute(sqlId, parameter, null, conn);
	}

	/**
	 * <p>根据配置SQL执行数据删除操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int delete(String sqlId, KeyedCollection parameter, Connection conn) throws SQLException {
		return execute(sqlId, parameter, null, conn);
	}

	/**
	 * <p>根据配置SQL执行数据删除操作</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static int delete(String sqlId, CMISDomain parameter, Connection conn) throws SQLException {
		return execute(sqlId, parameter, null, conn);
	}

	/**
	 * 根据主键执行自动删除操作
	 * @param parameter：参数BEAN
	 * @param conn:数据库连接
	 * @return
	 * @throws SQLException
	 */
	public static int deleteAuto(CMISDomain parameter, Connection conn) throws SQLException {
		return executeAuto(parameter, SQL_TYPE_DELETE, conn);
	}

	/**
	 * 根据数据值对象得到SELECT语句
	 * @param value 数据对象
	 * @return
	 * @throws SQLException 
	 */
	private static String getSelectSql(Object value, List fieldValueList) throws SQLException {
		StringBuffer sql = new StringBuffer();
		String tableName = null;
		String[] primaryKey = null;
		String paramStr = "";
		// List paramValue = new ArrayList();
		if (value instanceof CMISDomain) {
			CMISDomain domain = (CMISDomain) value;
			tableName = domain.getTableName();
			Map data = domain.getDataMap();
			primaryKey = domain.getPrimaryKey();

			if (primaryKey != null && primaryKey.length > 0) {
				for (int i = 0; i < primaryKey.length; i++) {
					Object fieldValue = data.get(primaryKey[i]);
					if (fieldValue != null) {
						fieldValueList.add(fieldValue);
						paramStr = paramStr + " AND " + primaryKey[i] + "=?";
					} else {
						throw new SQLException("查询主键域未设置值，无法获取执行自动查询语句!");
					}
				}
				if (paramStr.length() > 5) {
					paramStr = paramStr.substring(4);
				}
			} else {
				throw new SQLException("查询主键域未设置，无法获取执行自动查询语句!");
			}
		} else if (value instanceof KeyedCollection) {

		}
		// paramStr = paramStr.substring(0, paramStr.length() - 1);
		sql.append("SELECT * FROM ").append(tableName).append(" WHERE ").append(paramStr);
		return sql.toString();
	}
	private static String getInsertSql(Object value, List fieldValueList, List<String> keyList) {
		StringBuffer sql = new StringBuffer();
		String tableName = null;
		String[] primaryKey = null;
		String fieldStr = "";
		String paramStr = "";
		// List paramValue = new ArrayList();
		if (value instanceof CMISDomain) {
			CMISDomain domain = (CMISDomain) value;
			tableName = domain.getTableName();
			Map data = domain.getDataMap();
			if (keyList.isEmpty()) {
				Iterator<String> it = data.keySet().iterator();
				while (it.hasNext()) {
					String fieldName = it.next();
					Object fieldValue = data.get(fieldName);
					// null值也进行插入
					// if (fieldValue != null) {
					fieldValueList.add(fieldValue);
					fieldStr = fieldStr + fieldName + ",";
					paramStr = paramStr + "?,";
					// }
					keyList.add(fieldName);
				}
			} else {
				for (String key : keyList) {
					String fieldName = key;
					Object fieldValue = data.get(fieldName);
					fieldValueList.add(fieldValue);
					fieldStr = fieldStr + fieldName + ",";
					paramStr = paramStr + "?,";
				}
			}
		} else if (value instanceof KeyedCollection) {
			// KeyedCollection data = (KeyedCollection)value;
			// Iterator<String> it = data.keySet().iterator();
			// while (it.hasNext()){
			// String fieldName = it.next();
			// Object fieldValue = data.get(fieldName);
			// if (fieldValue != null){
			// fieldValueList.add(fieldValue);
			// fieldStr = fieldStr + fieldValue +",";
			// paramStr = paramStr + "?,";
			// }
			// }
		}
		fieldStr = fieldStr.substring(0, fieldStr.length() - 1);
		paramStr = paramStr.substring(0, paramStr.length() - 1);
		sql.append("INSERT INTO ").append(tableName).append(" (").append(fieldStr)
				.append(") VALUES (").append(paramStr).append(")");

		return sql.toString();
	}

	/**
	 * 根据数据值对象得到insert语句
	 * @param value 数据对象
	 * @return
	 */
	private static String getInsertSql(Object value, List fieldValueList, String seqName) {
		StringBuffer sql = new StringBuffer();
		String tableName = null;
		String[] primaryKey = null;
		String primaryKeyName = null;
		String fieldStr = "";
		String paramStr = "";
		// List paramValue = new ArrayList();
		if (value instanceof CMISDomain) {
			CMISDomain domain = (CMISDomain) value;
			tableName = domain.getTableName();
			Map data = domain.getDataMap();
			primaryKey = domain.getPrimaryKey();
			if (primaryKey.length > 0) {
				primaryKeyName = primaryKey[0].toString();
			}
			Iterator<String> it = data.keySet().iterator();
			while (it.hasNext()) {
				String fieldName = it.next();
				Object fieldValue = data.get(fieldName);
				if (fieldValue != null) {
					fieldValueList.add(fieldValue);
					fieldStr = fieldStr + fieldName + ",";
					paramStr = paramStr + "?,";
				}
			}
		} else if (value instanceof KeyedCollection) {

		}
		fieldStr = fieldStr.substring(0, fieldStr.length() - 1);
		paramStr = paramStr.substring(0, paramStr.length() - 1);
		sql.append("INSERT INTO ").append(tableName).append(" (").append(primaryKeyName + ",").append(fieldStr)
				.append(") VALUES (").append(seqName + ".nextval,").append(paramStr).append(")");

		return sql.toString();
	}

	/**
	 * 根据数据值对象得到update语句
	 * @param value 数据对象
	 * @return
	 * @throws SQLException 
	 */
	private static String getUpdateSql(Object value, List fieldValueList, List<String> keys) throws SQLException {
		StringBuffer sql = new StringBuffer();
		String tableName = null;
		String[] primaryKey = null;
		String fieldStr = "";
		String paramStr = "";
		if (value instanceof CMISDomain) {
			CMISDomain domain = (CMISDomain) value;
			tableName = domain.getTableName();
			primaryKey = domain.getPrimaryKey();
			Map data = domain.getDataMap();
			if (keys.isEmpty()) {
				Iterator<String> it = data.keySet().iterator();
				while (it.hasNext()) {
					String fieldName = it.next();
					Object fieldValue = data.get(fieldName);

					// 判断如果是主键，则不更新
					if (isPrimaryKey(primaryKey, fieldName))
						continue;
					/**
					 * 为了解决updateAuto方法，修改记录是不能清除已存在数据的缺陷，特做如下修改：
					 * 不对fieldValue的值进行判断，如果fieldValue等于null或者""的时候，也需要执行update语句。
					 */
					// 如果是空值也更新
					// if (fieldValue != null){
					fieldValueList.add(fieldValue);
					fieldStr = fieldStr + fieldName + " = ? , ";
					keys.add(fieldName);
					// }
				}
			} else {
				for (String key : keys) {
					String fieldName = key;
					Object fieldValue = data.get(fieldName);
					if (isPrimaryKey(primaryKey, fieldName)) {
						continue;
					}
					fieldValueList.add(fieldValue);
					fieldStr = fieldStr + fieldName + " = ? , ";
				}
			}

			// 判断主键值如果为空则，则抛出异常，不允许更新操作(考虑主键为多个情况)
			if (primaryKey != null && primaryKey.length > 0) {
				int primaryValueCount = 0;// 记录被赋值的主键
				for (int i = 0; i < primaryKey.length; i++) {
					Object fieldValue = data.get(primaryKey[i]);
					if (fieldValue != null) {
						primaryValueCount++;
						fieldValueList.add(fieldValue);
						paramStr = paramStr + " AND " + primaryKey[i] + " = ?";
					}
				}
				if (primaryValueCount == 0) {
					throw new SQLException("主键值为空，无法执行自动更新操作!!");
				}
			} else {
				throw new SQLException("未设置主键，无法执行自动更新操作!!");
			}
		} else if (value instanceof KeyedCollection) {

		}
		fieldStr = fieldStr.substring(0, fieldStr.length() - 2);
		sql.append(" UPDATE ").append(tableName).append(" SET ").append(fieldStr)
				.append(" WHERE 1 > 0 ").append(paramStr).append("");
		return sql.toString();
	}

	/**
	 * 根据数据值对象得到insert语句
	 * @param value 数据对象
	 * @return
	 * @throws SQLException 
	 */
	private static String getDeleteSql(Object value, List fieldValueList) throws SQLException {
		StringBuffer sql = new StringBuffer();
		String tableName = null;
		String[] primaryKey = null;
		String paramStr = "";
		// List paramValue = new ArrayList();
		if (value instanceof CMISDomain) {
			CMISDomain domain = (CMISDomain) value;
			tableName = domain.getTableName();
			Map data = domain.getDataMap();
			primaryKey = domain.getPrimaryKey();

			if (primaryKey != null && primaryKey.length > 0) {
				for (int i = 0; i < primaryKey.length; i++) {
					Object fieldValue = data.get(primaryKey[i]);
					if (fieldValue != null) {
						fieldValueList.add(fieldValue);
						paramStr = paramStr + " AND " + primaryKey[i] + " = ?";
					} else {
						throw new SQLException("更新条件域存在空值，无法执行自动更新操作!!");
					}
				}
			} else {
				throw new SQLException("更新条件域存在空值，无法执行自动更新操作!!");
			}
		} else if (value instanceof KeyedCollection) {

		}
		// paramStr = paramStr.substring(0, paramStr.length() - 1);
		sql.append("DELETE FROM ").append(tableName).append(" WHERE 1 > 0 ").append(paramStr);
		return sql.toString();
	}

	/**
	 * <p>根据对象自动组装SQL并执行，目前只支持单表以主键值作为条件的场景SQL执行（无需写SQL）</p>
	 * @param value 更新值 (注：自动时只支持CMISDomain类型)
	 * @param sqlType 主键字段名列表（注：为字段名，不是域名，该参数仅仅用于更新时，作为更新的过滤条件）
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws SQLException
	 * @TODO 尚未判断值域是否为数据库字段
	 */
	private static int executeAuto(Object value, String sqlType, Connection conn) throws SQLException {
		long star = System.currentTimeMillis();// 用于计算执行SQL耗时
		int ret = -1;
		List fieldValueList = new ArrayList();
		// 输出到日志文件中的SQL
		String exportSql = "";
		PreparedStatement ps = null;

		/** 拼装SQL */
		StringBuffer sql = new StringBuffer();
		// StringBuffer sqlVlaue=new StringBuffer(" 当前SQL的值为<");
		try {

			if (SQL_TYPE_INSERT.equals(sqlType.trim())) {
				sql.append(getInsertSql(value, fieldValueList, new ArrayList<String>()));
			} else if (SQL_TYPE_UPDATE.equals(sqlType.trim())) {
				sql.append(getUpdateSql(value, fieldValueList, new ArrayList<String>()));
			} else if (SQL_TYPE_DELETE.equals(sqlType.trim())) {
				sql.append(getDeleteSql(value, fieldValueList));
			}

			ps = conn.prepareStatement(sql.toString());
			ParameterMetaData pmd = ps.getParameterMetaData();
			/** 设置参数 */
			if (fieldValueList != null && fieldValueList.size() > 0) {
				int paramIdx = 1;
				// 获取数据库类型，设置参数
				String dataBaseType = "Oracle";
				for (int n = 0; n < fieldValueList.size(); n++) {
					// 判断逻辑：当
					// oracle数据库时统一将null转为""，其它数据库时将VARCHAR类型的字段null值转为""，数值类型不作转换
					// 注：oracle 驱动未实现getParameterTypeName()方法
					// Oracle数据库处理
					if ("Oracle".equalsIgnoreCase(dataBaseType))
						ps.setObject(paramIdx, fieldValueList.get(n) == null ? "" : fieldValueList.get(n));
					// 其它数据库处理,已知DB2 mysql在将数值类型统一转为null值报错
					else {
						String typeName = pmd.getParameterTypeName(paramIdx);
						if (typeName.startsWith("VARCHAR"))
							ps.setObject(paramIdx, fieldValueList.get(n) == null ? "" : fieldValueList.get(n));
						else
							ps.setObject(paramIdx, "".equals(fieldValueList.get(n)) ? null : fieldValueList.get(n));
					}
					paramIdx++;
					// sqlVlaue.append((fieldValueList.get(n)==null?"null":fieldValueList.get(n))+",");
				}
			}
			// sqlVlaue.append(">");
			/** 执行SQL */
			ret = ps.executeUpdate();

			// 将SQL中的？替换为实际的值，便于查看
			exportSql = resetExportSqlLog(sql.toString(), fieldValueList);
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, exportSql, (System.currentTimeMillis() - star));
		} catch (SQLException e) {
			e.printStackTrace();
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sql.toString(), -1);
			throw new SQLException(e.getMessage());
		} catch (Throwable e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sql.toString(), -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (ps != null)
				ps.close();
		}

		return ret;
	}

	/**
	 * <p>执行数据变更操作（包含INSERT、UPDATE、DELETE）</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）(只支持MAP\CMISDomain\KeyedCollection)
	 * @param value 更新值(只支持MAP\CMISDomain\KeyedCollection)
	 * @param conditionId 可选条件ID
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	private static int execute(String sqlId, Object parameter, Object value, Connection conn) throws SQLException {
		int ret = -1;
		long star = System.currentTimeMillis();// 用于计算执行SQL耗时
		/** 解释SQL配置 , 得到原始配置*/
		String sqlOrigin = null;
		List paramValue = null;
		// 输出到日志文件中的SQL
		String exportSql = "";
		try {
			sqlOrigin = SqlConfigContext.getSqlFromConfig(sqlId, parameter, null, value);
			paramValue = SqlConfigContext.getParamList(sqlId, parameter, value);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		PreparedStatement ps = null;
		// StringBuffer sqlVlaue=new StringBuffer(" 当前SQL的值为<");
		try {
			ps = conn.prepareStatement(sqlOrigin);
			ParameterMetaData pmd = ps.getParameterMetaData();
			// 循环设值
			int paramIdx = 1;
			if (paramValue != null && paramValue.size() > 0) {

				// 获取数据库类型，设置参数
				String dataBaseType = "Oracle";
				for (int n = 0; n < paramValue.size(); n++) {
					// 判断逻辑：当
					// oracle数据库时统一将null转为""，其它数据库时将VARCHAR类型的字段null值转为""，数值类型不作转换
					// 注：oracle 驱动未实现getParameterTypeName()方法
					// Oracle数据库处理
					if ("Oracle".equalsIgnoreCase(dataBaseType))
						ps.setObject(paramIdx, paramValue.get(n) == null ? "" : paramValue.get(n));
					// 其它数据库处理,已知DB2 mysql在将数值类型统一转为null值报错
					else {
						String typeName = pmd.getParameterTypeName(paramIdx);
						if (typeName.startsWith("VARCHAR"))
							ps.setObject(paramIdx, paramValue.get(n) == null ? "" : paramValue.get(n));
						else
							ps.setObject(paramIdx, "".equals(paramValue.get(n)) ? null : paramValue.get(n));
					}
					paramIdx++;
					// sqlVlaue.append((paramValue.get(n)==null?"null":paramValue.get(n))+",");
				}
			}
			// sqlVlaue.append(">");
			ret = ps.executeUpdate();
			// 将SQL中的？替换为实际的值，便于查看
			exportSql = resetExportSqlLog(sqlOrigin, paramValue);

			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, exportSql, (System.currentTimeMillis() - star));
		} catch (SQLException e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, exportSql, -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, exportSql, -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (ps != null)
				ps.close();
		}
		return ret;
	}

	/**
	 * <p>是否为基本型数据</p>
	 * @param Classname 类名
	 * @return true是基本型数据
	 */
	private static boolean isBasicDataType(String Classname) {
		if (Classname.equals("java.lang.Double")
				|| Classname.equals("java.lang.Integer")
				|| Classname.equals("java.lang.Float")
				|| Classname.equals("java.lang.String")
				|| Classname.equals("java.math.BigDecimal")) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * <p>判断指定字符串是否在目标数据中</p>
	 * <p>
	 *    场景：这里主要应用于判断指定字符串是否是联合主键之一
	 * </p>
	 * 
	 * @param keys （联合）主键
	 * @param toCheck 指定字
	 * @return true-是联合主键， false-不是
	 */
	private static boolean isPrimaryKey(String[] keys, String toCheck) {
		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];
			if (key != null && key.equals(toCheck))
				return true;
		}

		return false;
	}

	/**
	 * 批量 插入 操作
	 * @param sqlId SQL的配置ID 
	 * @param value 需要更新或插入的值，其类型可为CMISDomain/Map/KeyedCollection/简单数据类型等数组
	 * @param conn 数据库连接
	 * @return
	 * @throws SQLException
	 */
	public static void executeBatch(String sqlId, Object[] value, Connection conn) throws SQLException {
		executeBatch(sqlId, null, value, conn);
	}

	/**
	 * <p>执行批量操作（包含INSERT、UPDATE、DELETE）</p>
	 * <p>注意:当执行插入和更新时只会根据第一个domain中存在的字段进行sql的生成</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param value 更新值,和parameter一样都是对象数组，但其中的数组元素可以为空
	 * @param conditionId 可选条件ID
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static void executeDomainBatch(String sqlType, Object[] parameter, Connection conn) throws SQLException {
		int ret = -1;
		String sqlOrigin = "";
		List fieldValueList = new ArrayList();
		List<String> keyList = new ArrayList<String>();
		if (SQL_TYPE_INSERT.equals(sqlType.trim())) {
			sqlOrigin = getInsertSql(parameter[0], fieldValueList, keyList);
		} else if (SQL_TYPE_UPDATE.equals(sqlType.trim())) {
			sqlOrigin = getUpdateSql(parameter[0], fieldValueList, keyList);
		} else if (SQL_TYPE_DELETE.equals(sqlType.trim())) {
			sqlOrigin = getDeleteSql(parameter[0], fieldValueList);
		}
		PreparedStatement ps = null;
		try {
			// 获取数据库类型，设置参数
			String dataBaseType = "Oracle";
			ps = conn.prepareStatement(sqlOrigin);
			ParameterMetaData pmd = ps.getParameterMetaData();

			conn.setAutoCommit(false);
			long startTime = System.currentTimeMillis();
			int length = 0;
			if (parameter != null)
				length = parameter.length;
			for (int i = 0; i < length; i++) {
				fieldValueList = new ArrayList();
				if (SQL_TYPE_INSERT.equals(sqlType.trim())) {
					getInsertSql(parameter[i], fieldValueList, keyList);
				} else if (SQL_TYPE_UPDATE.equals(sqlType.trim())) {
					getUpdateSql(parameter[i], fieldValueList, keyList);
				} else if (SQL_TYPE_DELETE.equals(sqlType.trim())) {
					getDeleteSql(parameter[i], fieldValueList);
				}
				/** 设置参数 */
				int parameterIndex = 1;
				if (fieldValueList != null && fieldValueList.size() > 0) {
					for (int n = 0; n < fieldValueList.size(); n++) {
						// 判断逻辑：当
						// oracle数据库时统一将null转为""，其它数据库时将VARCHAR类型的字段null值转为""，数值类型不作转换
						// 注：oracle 驱动未实现getParameterTypeName()方法
						// Oracle数据库处理
						if ("Oracle".equalsIgnoreCase(dataBaseType))
							ps.setObject(parameterIndex, fieldValueList.get(n) == null ? "" : fieldValueList.get(n));
						// 其它数据库处理,已知DB2 mysql在将数值类型统一转为null值报错
						else {
							String typeName = pmd.getParameterTypeName(parameterIndex);
							if (typeName.startsWith("VARCHAR"))
								ps.setObject(parameterIndex, fieldValueList.get(n) == null ? "" : fieldValueList.get(n));
							else
								ps.setObject(parameterIndex, "".equals(fieldValueList.get(n)) ? null : fieldValueList.get(n));
						}
						parameterIndex++;
					}
				}
				ps.addBatch();
				if (i % 1000 == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch();
			long endTime = System.currentTimeMillis();
			// System.out.println("消耗时间："+(endTime-startTime)+"ms");
			EMPLog.log(LOGTYPE, EMPLog.DEBUG, 0, "批量操作,耗时:" + (endTime - startTime) + ",执行SQL:" + sqlOrigin);

		} catch (SQLException e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (ps != null)
				ps.close();
		}

	}

	/**
	 * <p>
	 * <h2>简述</h2>
	 *     <ol>对批量插入的Domain进行修复.</ol>
	 * <h2>功能描述</h2>
	 *     <ol>将domain中当前domain不存在但是其他domain存在的字段设置为null.</ol>
	 * </p>
	 * @param parameter 
	 */
	public static void fixDomain4Batch(CMISDomain[] parameter) {
		fixDomain4Batch(parameter, true);
	}

	/**
	 * <p>
	 * <h2>简述</h2>
	 *     <ol>对批量插入的Domain进行修复.</ol>
	 * <h2>功能描述</h2>
	 *     <ol>将domain中当前domain不存在但是其他domain存在的字段设置为空或者null.</ol>
	 * </p>
	 * @param parameter
	 * @param fixByNull 是否将不存在的字段设置为null，false时将设置为""
	 */
	public static void fixDomain4Batch(CMISDomain[] parameter, boolean fixByNull) {
		if (parameter == null) {
			return;
		}
		Set<String> keySet = new HashSet<String>();
		for (CMISDomain domain : parameter) {
			Map dataMap = domain.getDataMap();
			Set domainKeys = dataMap.keySet();
			keySet.addAll(domainKeys);
		}

		for (CMISDomain domain : parameter) {
			Map dataMap = domain.getDataMap();
			for (String key : keySet) {
				if (!dataMap.containsKey(key) || dataMap.get(key) == null) {
					if (fixByNull) {
						dataMap.put(key, null);
					} else {
						dataMap.put(key, "");
					}
				}
			}
		}

	}

	/**
	 * <p>执行批量操作（包含INSERT、UPDATE、DELETE）</p>
	 * @param sqlId  SQL的配置ID 
	 * @param parameter 输入参数（查询过滤条件）
	 * @param value 更新值,和parameter一样都是对象数组，但其中的数组元素可以为空
	 * @param conditionId 可选条件ID
	 * @param conn 数据库连接
	 * @return 影响记录条数
	 * @throws Exception
	 */
	public static void executeBatch(String sqlId, Object[] parameter, Object[] value, Connection conn) throws SQLException {
		int ret = -1;

		/** 解释SQL配置 , 得到原始配置*/
		String sqlOrigin = null;
		List paramValue = null;

		try {
			Object oParam = null;
			if (parameter != null && parameter.length > 0)
				oParam = parameter[0];
			Object oValue = null;
			if (value != null && value.length > 0)
				oValue = value[0];
			sqlOrigin = SqlConfigContext.getSqlFromConfig(sqlId, oParam, null, oValue);
			// paramValue = SqlConfigContext.getParamList(sqlId,
			// parameter[0],value[0], conditionId);
		} catch (Exception e) {
			e.printStackTrace();
		}

		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sqlOrigin);
			ParameterMetaData pmd = ps.getParameterMetaData();
			conn.setAutoCommit(false);
			long startTime = System.currentTimeMillis();
			int length = 0;
			if (value != null)
				length = value.length;
			if (parameter != null)
				length = parameter.length;
			// 获取数据库类型，设置参数
			String dataBaseType = "Oracle";
			for (int i = 0; i < length; i++) {
				Object oParam1 = null;
				if (parameter != null && parameter.length > 0)
					oParam1 = parameter[i];
				Object oValue1 = null;
				if (value != null && value.length > 0)
					oValue1 = value[i];
				paramValue = SqlConfigContext.getParamList(sqlId, oParam1, oValue1);
				int paramIdx = 1;
				if (paramValue != null && paramValue.size() > 0) {
					for (int n = 0; n < paramValue.size(); n++) {
						// 判断逻辑：当
						// oracle数据库时统一将null转为""，其它数据库时将VARCHAR类型的字段null值转为""，数值类型不作转换
						// 注：oracle 驱动未实现getParameterTypeName()方法
						// Oracle数据库处理
						if ("Oracle".equalsIgnoreCase(dataBaseType))
							ps.setObject(paramIdx, paramValue.get(n) == null ? "" : paramValue.get(n));
						// 其它数据库处理,已知DB2 mysql在将数值类型统一转为null值报错
						else {
							String typeName = pmd.getParameterTypeName(paramIdx);
							if (typeName.startsWith("VARCHAR"))
								ps.setObject(paramIdx, paramValue.get(n) == null ? "" : paramValue.get(n));
							else
								ps.setObject(paramIdx, "".equals(paramValue.get(n)) ? null : paramValue.get(n));
						}
						paramIdx++;
					}
				}
				ps.addBatch();
				if (i % 1000 == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch();
			long endTime = System.currentTimeMillis();
			// System.out.println("消耗时间："+(endTime-startTime)+"ms");
			EMPLog.log(LOGTYPE, EMPLog.DEBUG, 0, "sqlId:" + sqlId + " 批量操作,耗时:" + (endTime - startTime) + ",执行SQL:" + sqlOrigin);

		} catch (SQLException e) {
			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog(sqlId, EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (ps != null)
				ps.close();
		}

	}

	/**
	 * <p>SQLClient的日志输出</p>
	 * 
	 * <p>
	 * 	 <ul>
	 * 		输出格式：yyyy-MM-dd HHmmss,sss SQL 耗时:..., 执行SQL:...
	 * 		格式固定用于工具分析日志找到耗时较长的SQL
	 * 	 </ul>
	 * </p>
	 * 
	 * @param level 日志级别 EMPLog.Debug  EMPLog.Info ...
	 * @param sql 执行的SQL
	 * @param useTime 该SQL耗时
	 */
	private static void sqlClientLog(String sqlId, int level, String sql, long useTime) {
		if (useTime < 0 && sql != null)
			EMPLog.log(LOGTYPE, level, 0, "sqlId:" + sqlId + "执行SQL:" + sql);
		else if (sql != null)
			EMPLog.log(LOGTYPE, level, 0, "耗时:" + useTime + "ms sqlId:" + sqlId + " 执行SQL:" + sql);
	}

	/**
	 * <p>执行批量插入操作，可不给domain对象对应的sequence赋值，方法自动拼对应sequence名称的nextval作为主键进行插入</p> 
	 * @param parameter 插入数据的domain结合
	 * @param seqName 对应序列名称
	 * @param conn 数据库连接
	 * @throws Exception
	 */
	@Deprecated
	public static void executeDomainInsertBatch(Object[] parameter, String seqName, Connection conn) throws SQLException {
		int ret = -1;
		String sqlOrigin = "";
		List fieldValueList = new ArrayList();
		sqlOrigin = getInsertSql(parameter[0], fieldValueList, seqName);
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sqlOrigin);
			ParameterMetaData pmd = ps.getParameterMetaData();
			conn.setAutoCommit(false);
			long startTime = System.currentTimeMillis();
			int length = 0;
			if (parameter != null)
				length = parameter.length;
			// 获取数据库类型，设置参数
			String dataBaseType = "Oracle";
			for (int i = 0; i < length; i++) {
				fieldValueList = new ArrayList();
				getInsertSql(parameter[i], fieldValueList, seqName);
				/** 设置参数 */
				int parameterIndex = 1;
				if (fieldValueList != null && fieldValueList.size() > 0) {
					for (int n = 0; n < fieldValueList.size(); n++) {
						// 判断逻辑：当
						// oracle数据库时统一将null转为""，其它数据库时将VARCHAR类型的字段null值转为""，数值类型不作转换
						// 注：oracle 驱动未实现getParameterTypeName()方法
						// Oracle数据库处理
						if ("Oracle".equalsIgnoreCase(dataBaseType))
							ps.setObject(parameterIndex, fieldValueList.get(n) == null ? "" : fieldValueList.get(n));
						// 其它数据库处理,已知DB2 mysql在将数值类型统一转为null值报错
						else {
							String typeName = pmd.getParameterTypeName(parameterIndex);
							if (typeName.startsWith("VARCHAR"))
								ps.setObject(parameterIndex, fieldValueList.get(n) == null ? "" : fieldValueList.get(n));
							else
								ps.setObject(parameterIndex, "".equals(fieldValueList.get(n)) ? null : fieldValueList.get(n));
						}
						parameterIndex++;
					}
				}
				ps.addBatch();
				if (i % 1000 == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch();
			long endTime = System.currentTimeMillis();
			// System.out.println("消耗时间："+(endTime-startTime)+"ms");
			EMPLog.log(LOGTYPE, EMPLog.DEBUG, 0, "自动拼接SQL 批量操作,耗时:" + (endTime - startTime) + ",执行SQL:" + sqlOrigin);

		} catch (SQLException e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sqlOrigin.toString(), -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (ps != null)
				ps.close();
		}

	}

	/**
	 * 
	 * <p>
	 * <h2>简述</h2>
	 * 		<ol>数据库的DDL操作</ol>
	 * <h2>功能描述</h2>
	 * 		<ol>数据库的DDL操作，谨慎使用</ol>
	 * </p>
	 * @param sqlId SQL的配置ID
	 * @param connection 数据库连接
	 */
	public static void executeDdl(String sqlId, Connection conn) throws SQLException {
		long star = System.currentTimeMillis();
		/** 解释SQL配置 , 得到原始配置*/
		String sqlOrigin = null;
		List paramValue = null;
		try {
			sqlOrigin = SqlConfigContext.getSqlFromConfig(sqlId, null, null, null);
			// paramValue = SqlConfigContext.getParamList(sqlId, null,null);
		} catch (Exception e) {
			throw new SQLException(sqlId + "的SQL配置有误!", e);
		}

		sqlOrigin = sqlOrigin.trim().toLowerCase();

		if (sqlOrigin.startsWith("select") || sqlOrigin.startsWith("update") || sqlOrigin.startsWith("delete") || sqlOrigin.startsWith("insert"))
			throw new SQLException("executeDdl方法只允许执行DDL相关的SQL，DML及DQL类的SQL不允许执行");

		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sqlOrigin);
			ps.execute();
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sqlOrigin, (System.currentTimeMillis() - star));
		} catch (SQLException e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sqlOrigin, -1);
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			// 输出日志
			sqlClientLog("自动拼接SQL", EMPLog.DEBUG, sqlOrigin, -1);
			throw new SQLException(e.getMessage());
		} finally {
			if (ps != null)
				ps.close();
		}
	}

	/**
	 * 
	 * <p>
	 * <h2>简述</h2>
	 * 		<ol>重置输出到日志文件中的SQL,并将输入的值更新到原SQL中的'?'号，以便更加直观的查看SQL</ol>
	 * <h2>功能描述</h2>
	 * 		<ol></ol>
	 * </p>
	 * @param sql 执行的SQL
	 * @param paramValue 待替换的值
	 */
	private static String resetExportSqlLog(String sql, List<Object> paramValue) {
		StringBuffer expLog = new StringBuffer();
		if (paramValue == null || paramValue.size() == 0) {
			return sql;
		}
		String[] split = sql.split("\\?");
		for (int i = 0; i < split.length; i++) {
			expLog.append(split[i]);
			if (i < paramValue.size()) {
				Object value = paramValue.get(i);
				if (value instanceof String || value instanceof Character) {
					expLog.append("'" + value + "'");
				} else {
					expLog.append(value);
				}
			} else {
				expLog.append("?");
			}
		}
		return expLog.toString();
	}

	// /**
	// *
	// * <p>
	// * <h2>简述</h2>
	// * <ol>校验SQL中的?与paramValue中的个数是否一致</ol>
	// * <h2>功能描述</h2>
	// * <ol></ol>
	// * </p>
	// * @param sql 执行的SQL
	// * @param paramValue 待替换的值
	// */
	// private static boolean checkSqlPramValueLength(String sql, List<Object>
	// paramValue){
	// String[] split = sql.split("[?]",-1);
	// if (split.length-1 != paramValue.size()) {
	// EMPLog.log(LOGTYPE, EMPLog.ERROR, 0,
	// "sql参数个数不一致："+sql+"的参数个数为"+(split.length-1)+",传入的参数个数为"+paramValue.size());
	// return false;
	// }
	// return true;
	// }

	public static void main(String[] args) {
		String sql = "select *from s_user where id=? and name=?  and  value=? or dd = '11' and ss = ?";
		List<Object> paramValue = new ArrayList<Object>();
		paramValue.add("aa$a");
		paramValue.add(123);
		paramValue.add("c?cc");
		System.out.println(SqlClient.resetExportSqlLog(sql, paramValue));

		/*	String[] split = sql.split("[?]",-1);
			for (int i = 0; i < split.length; i++) {
				System.out.println(split[i]);
			}
			System.out.println(split.length);*/

	}

	class SqlExecutor {

	}
}
