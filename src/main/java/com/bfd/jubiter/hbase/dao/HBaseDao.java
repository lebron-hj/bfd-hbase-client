package com.bfd.jubiter.hbase.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * 
 * 0.90以后版本hbase-client-api使用
 * 
 * @author BFD_357
 *
 */
public class HBaseDao {

	private static Logger logger = Logger.getLogger(HBaseDao.class);

	static Configuration conf = HBaseConfiguration.create();
	static Connection connection;

	static {
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * create a table :table_name(columnFamily)
	 * 
	 * @param tablename
	 * @param columnFamily
	 * @throws Exception
	 */
	public static void createTable(String tablename, String[] columnFamilys,
			int maxLineVersion) throws Exception {
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		if (admin.tableExists(tablename)) {
			logger.error("Table exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf(tablename));
			for (int i = 0; i < columnFamilys.length; i++) {
				String columnFamily = columnFamilys[i];
				HColumnDescriptor desc = new HColumnDescriptor(columnFamily);
				desc.setMaxVersions(maxLineVersion);
				tableDesc.addFamily(desc);
			}
			admin.createTable(tableDesc);
			logger.debug("create table(version muti version) success!");
		}
		admin.close();

	}

	/**
	 * create a table :table_name(columnFamily)
	 * 
	 * @param tablename
	 * @param columnFamily
	 * @throws Exception
	 */
	public static void createTable(String tablename, String[] columnFamilys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			logger.error("Table exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf(tablename));
			for (int i = 0; i < columnFamilys.length; i++) {
				String columnFamily = columnFamilys[i];
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			admin.createTable(tableDesc);
			logger.debug("create table success!");
		}
		admin.close();

	}

	/**
	 * delete table ,caution!!!!!! ,dangerous!!!!!!
	 * 
	 * @param tablename
	 * @return
	 * @throws IOException
	 */
	public static boolean deleteTable(String tablename) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			try {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				admin.close();
				return false;
			}
		}
		admin.close();
		return true;
	}

	/**
	 * put a cell data into a row identified by rowKey,columnFamily,identifier
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param rowKey
	 * @param columnFamily
	 * @param identifier
	 * @param data
	 * @throws Exception
	 */
	public static void putCell(HTable table, String rowKey,
			String columnFamily, String identifier, String data)
			throws Exception {
		Put p1 = new Put(Bytes.toBytes(rowKey));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier),
				Bytes.toBytes(data));
		table.put(p1);
		logger.debug("put '" + rowKey + "', '" + columnFamily + ":"
				+ identifier + "', '" + data + "'");
	}

	/**
	 * put a cell data into a row identified by rowKey,columnFamily,identifier
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param rowKey
	 * @param columnFamily
	 * @param identifier
	 * @param data
	 * @param ts
	 *            column version
	 * @throws Exception
	 */
	public static void putCell(HTable table, String rowKey,
			String columnFamily, String identifier, String data, long ts)
			throws Exception {
		Put p1 = new Put(Bytes.toBytes(rowKey));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier), ts,
				Bytes.toBytes(data));
		table.put(p1);
		logger.debug("put '" + rowKey + "', '" + columnFamily + ":"
				+ identifier + "', '" + data + "'");
	}

	/**
	 * put a cell data into a row identified by rowKey,columnFamily,identifier
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param rowKey
	 * @param columnFamily
	 * @param identifier
	 * @param data
	 * @param ts
	 *            column version
	 * @throws Exception
	 */
	public static void putCell(String tableName, String rowKey,
			String columnFamily, String identifier, String data, long ts)
			throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put p1 = new Put(Bytes.toBytes(rowKey));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier), ts,
				Bytes.toBytes(data));
		table.put(p1);
		table.close();
		logger.debug("put '" + rowKey + "', '" + columnFamily + ":"
				+ identifier + "', '" + data + "'");
	}

	/**
	 * put a new row version data into a row identified by
	 * rowKey,Map<columnFamily:identifier,data>
	 * 
	 * @param tablename
	 * @param rowKey
	 * @param map
	 * @throws Exception
	 */
	public static void putRow(String tablename, String rowKey,
			Map<String, String> map) throws Exception {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(TableName
				.valueOf(tablename));
		try {
			putRow(table, rowKey, map);
		} finally {
			table.close();
			connection.close();
		}
	}

	/**
	 * put a new row version data into a row identified by
	 * rowKey,Map<columnFamily:identifier,data>
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param rowKey
	 * @param columnFamily
	 * @param identifier
	 * @param data
	 * @throws Exception
	 */
	public static void putRow(HTableInterface table, String rowKey,
			Map<String, String> map) throws Exception {
		Put p1 = new Put(Bytes.toBytes(rowKey));
		for (Iterator<Map.Entry<String, String>> iterator = map.entrySet()
				.iterator(); iterator.hasNext();) {
			Map.Entry<String, String> entry = iterator.next();
			String[] keys = entry.getKey().split(":");
			if (keys.length == 1) {// 指定默认列族
				keys = new String[] { "0", keys[0] };
			}
			if (keys.length != 2) {
				throw new Exception(
						"集合map中key格式错误，key格式为：【columnFamily:identifier】");
			}
			String columnFamily = keys[0];
			String identifier = keys[1];
			String value = entry.getValue();
			p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier),
					Bytes.toBytes(value));
		}
		table.put(p1);
	}

	/**
	 * 获取同一个rowKey对应的多个版本（多行记录）
	 * 
	 * @param rowKey
	 * @param tablename
	 * @param startTime
	 *            包含
	 * @param endTime
	 *            包含
	 * @return Map<String（时间戳）, Map<String（字段名）, String（字段值）>>
	 * @throws Exception
	 */
	public static Map<String, Map<String, String>> getRow(String tablename,
			String rowKey, long startTime, long endTime) throws Exception {

		Table table = connection.getTable(TableName.valueOf(tablename));
		try {
			Result result = getRow(table, rowKey, startTime, endTime);
			int line = 0;
			logger.debug(++line
					+ "行=======================================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			Map<String, Map<String, String>> map = new LinkedHashMap<String, Map<String, String>>();
			if (cellList != null) {
				for (Cell cell : cellList) {
					// System.err.println("tableName='"
					// + Bytes.toString(table.getTableName()) + "',rowKey='"
					// + Bytes.toString(CellUtil.cloneRow(cell))
					// + "',column='"
					// + Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
					// + Bytes.toString(CellUtil.cloneQualifier(cell))
					// + "',timestamp='" + cell.getTimestamp() + "',value='"
					// + Bytes.toString(CellUtil.cloneValue(cell)) + "'");
					String cellName = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					String cellValue = Bytes
							.toString(CellUtil.cloneValue(cell));
					String ts = String.valueOf(cell.getTimestamp());
					Map<String, String> rowMap = map.get(ts);

					if (rowMap == null) {
						rowMap = new HashMap<String, String>();
						map.put(ts, rowMap);
					}
					rowMap.put(cellName, cellValue);
				}
			}
			return map;
		} finally {
			table.close();
			connection.close();
		}
	}

	/**
	 * get a row identified by rowkey,startTime,endTime
	 * 
	 * @param HTableInterface
	 *            , create by : HConnection.getTable...
	 * @param rowKey
	 * @param tablename
	 * @param startTime
	 *            startTime与endTime均为0，则取最新一个单元格版本,时间点包含startTime
	 * @param endTime
	 *            startTime与endTime均为0，则取最新一个单元格版本,时间点包含endTime
	 * @throws Exception
	 */
	public static Result getRow(Table table, String rowKey, long startTime,
			long endTime) throws Exception {

		Get get = new Get(Bytes.toBytes(rowKey));
		if (startTime < endTime && startTime >= 0) {
			get.setTimeRange(startTime, endTime + 1);
			get.setMaxVersions();
		}
		Result result = table.get(get);
		logger.debug("Get: " + result);
		return result;
	}

	/**
	 * delete a row identified by rowkey and timeStamp
	 * 
	 * @param tablename
	 * @param rowKey
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRowEqualsTime(String tableName, String rowKey,
			long timestamp) throws Exception {
		deleteRowEqualsTime(tableName, rowKey, "0", timestamp);
	}

	/**
	 * delete a row identified by rowkey and columnFamily and timeStamp
	 * 
	 * @param tablename
	 * @param rowKey
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRowEqualsTime(String tableName, String rowKey,
			String columnFamily, long timestamp) throws Exception {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(TableName
				.valueOf(tableName));
		try {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.deleteFamilyVersion(Bytes.toBytes(columnFamily), timestamp);
			table.delete(delete);
			logger.debug("Delete row: " + rowKey);
		} finally {
			table.close();
			connection.close();
		}
	}

	/**
	 * delete a row identified by rowkey with timeStamp with a timestamp less
	 * than or equal to the specified timestamp.
	 * 
	 * @param tablename
	 * @param rowKey
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRowBeforeTime(String tableName, String rowKey,
			long timestamp) throws Exception {
		deleteRowBeforeTime(tableName, rowKey, "0", timestamp);
	}

	/**
	 * delete a row identified by rowkey and columnFamily with timeStamp with a
	 * timestamp less than or equal to the specified timestamp.
	 * 
	 * @param tablename
	 * @param rowKey
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRowBeforeTime(String tableName, String rowKey,
			String columnFamily, long timestamp) throws Exception {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(TableName
				.valueOf(tableName));
		try {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.deleteFamily(Bytes.toBytes(columnFamily), timestamp);
			table.delete(delete);
			logger.debug("Delete row: " + rowKey);
		} finally {
			table.close();
			connection.close();
		}
	}

	/**
	 * delete a row identified by rowkey
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRow(String tableName, String rowKey)
			throws Exception {
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(TableName
				.valueOf(tableName));
		try {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			table.delete(delete);
			logger.debug("Delete row: " + rowKey);
		} finally {
			table.close();
			connection.close();
		}
	}

	/**
	 * delete a row identified by rowkey
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRow(HTableInterface table, String rowKey)
			throws Exception {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		logger.debug("Delete row: " + rowKey);
	}

	/**
	 * return all row from a table
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @throws Exception
	 */
	public static ResultScanner scanAll(HTable table) throws Exception {
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	/**
	 * return a range of rows specified by startrow and endrow
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param startrow
	 * @param endrow
	 * @throws Exception
	 */
	public static ResultScanner scanRange(HTable table, String startrow,
			String endrow) throws Exception {
		Scan s = new Scan(Bytes.toBytes(startrow), Bytes.toBytes(endrow));
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	/**
	 * return a range of rows filtered by specified condition
	 * 
	 * @param HTable
	 *            , create by : HTable table = new HTable(conf, "tablename")
	 * @param startrow
	 * @param filter
	 * @throws Exception
	 */
	public static ResultScanner scanFilter(HTable table, String startrow,
			Filter filter) throws Exception {
		Scan s = new Scan(Bytes.toBytes(startrow), filter);
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		HTable table = new HTable(conf, "t");

		// ResultScanner rs = HBaseDAO.scanRange(table, "2013-07-10*",
		// "2013-07-11*");
		// ResultScanner rs = HBaseDAO.scanRange(table, "100001", "100003");
		ResultScanner rs = HBaseDao.scanAll(table);

		for (Result r : rs) {
			logger.debug("Scan: " + r);
		}
		table.close();

		// HBaseDAO.createTable("apitable", "testcf");
		// HBaseDAO.putRow("apitable", "100001", "testcf", "name", "liyang");
		// HBaseDAO.putRow("apitable", "100003", "testcf", "name", "leon");
		// HBaseDAO.deleteRow("apitable", "100002");
		// HBaseDAO.getRow("apitable", "100003");
		// HBaseDAO.deleteTable("apitable");

	}

	/**
	 * addColumnFamily :table_name(columnFamily)
	 * 
	 * @param tablename
	 * @param columnFamily
	 * @throws Exception
	 */
	public static void addColumnFamily(String tableName, String columnFamily,
			int maxLineVersion) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			HColumnDescriptor desc = new HColumnDescriptor(columnFamily);
			desc.setMaxVersions(maxLineVersion);
			admin.addColumn(tableName, desc);
			logger.debug("create ColumnFamily success!");
		} else {
			logger.error("Table not exists!");
		}
		admin.close();
	}

	/**
	 * modifyColumnFamily :table_name(columnFamily)
	 * 
	 * @param tablename
	 * @param columnFamily
	 * @throws Exception
	 */
	public static void modifyColumnFamily(String tableName,
			String columnFamily, int maxLineVersion) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			HColumnDescriptor desc = new HColumnDescriptor(columnFamily);
			desc.setMaxVersions(maxLineVersion);
			admin.modifyColumn(tableName, desc);
			logger.info("modify ColumnFamily success!");
		} else {
			logger.error("Table not exists!");
		}
		admin.close();
	}

	/**
	 * deleteColumnFamily :table_name(columnFamily)
	 * 
	 * @param tablename
	 * @param columnFamily
	 * @throws Exception
	 */
	public static void deleteColumnFamily(String tablename, String columnFamily)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			admin.deleteColumn(tablename, columnFamily);
			logger.debug("delete ColumnFamily success!");
		} else {
			logger.error("Table not exists!");
		}
		admin.close();
	}

	/**
	 * get qualifier Latest Value
	 * 
	 * @param table
	 * @param rowKey
	 * @param columnFamily
	 * @param qualifier
	 * @return
	 * @throws Exception
	 */
	public static String getColumnLatestCellValue(HTable table, String rowKey,
			String columnFamily, String qualifier) throws Exception {
		String rst;
		Result result = getRow(table, rowKey, 0, 0);
		Cell cell = result.getColumnLatestCell(Bytes.toBytes(columnFamily),
				Bytes.toBytes(qualifier));
		if (cell == null) {
			logger.error("spcific cell not exist!qualifier=" + columnFamily
					+ ":" + qualifier);
			return null;
		}
		rst = Bytes.toString(CellUtil.cloneValue(cell));
		logger.debug("getColumnLatestCellValue success! tableName='"
				+ Bytes.toString(table.getTableName()) + "',rowKey='" + rowKey
				+ "',column='" + columnFamily + ":" + qualifier + "',value='"
				+ rst + "'");
		return rst;
	}

	/**
	 * get qualifier Latest Value
	 * 
	 * @param table
	 * @param rowKey
	 * @param columnFamily
	 * @param qualifier
	 * @return
	 * @throws Exception
	 */
	public static String getColumnLatestCellValue(String tableName,
			String rowKey, String columnFamily, String qualifier)
			throws Exception {
		String rst;
		Cell cell = getColumnLatestCell(tableName, rowKey, columnFamily,
				qualifier);
		if (cell == null) {
			return null;
		}
		rst = Bytes.toString(CellUtil.cloneValue(cell));
		return rst;
	}

	/**
	 * get qualifier Latest Value
	 * 
	 * @param table
	 * @param rowKey
	 * @param columnFamily
	 * @param qualifier
	 * @return
	 * @throws Exception
	 */
	public static Cell getColumnLatestCell(String tableName, String rowKey,
			String columnFamily, String qualifier) throws Exception {
		String rst;
		Table table = connection.getTable(TableName.valueOf(tableName));
		Result result = getRow(table, rowKey, 0, 0);
		Cell cell = result.getColumnLatestCell(Bytes.toBytes(columnFamily),
				Bytes.toBytes(qualifier));
		if (cell == null) {
			logger.error("spcific cell not exist!qualifier=" + columnFamily
					+ ":" + qualifier);
			return null;
		}
		rst = Bytes.toString(CellUtil.cloneValue(cell));
		logger.debug("getColumnLatestCellValue success! tableName='"
				+ tableName + "',rowKey='" + rowKey + "',column='"
				+ columnFamily + ":" + qualifier + "',value='" + rst + "'");
		return cell;
	}

}
