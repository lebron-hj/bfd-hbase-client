package com.bfd.jubiter.hbase.service;

import net.sf.json.JSONObject;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.bfd.jubiter.hbase.dao.HBaseDao;
import com.bfd.jubiter.hbase.util.StringUtil;

public class JsonDataHandleService {
	/**
	 * 增加一条数据到hbase，只有ts大于hbase中的tsVersion才更新，否则不更新也不报错
	 * 
	 * @param tableName
	 *            表名
	 * @param jsonStr
	 *            格式如下：{item_id :1,num :1,modify_time:'2015-05-11 16:47:30'}
	 * @throws Exception
	 */
	public static void addMessage(String jsonStr) throws Exception {

		addMessage("bfd_item", "item", "num", jsonStr);

	}

	/**
	 * 增加一条数据到hbase，只有ts大于hbase中的tsVersion才更新，否则不更新也不报错
	 * 
	 * @param tableName
	 *            表名
	 * @param columnFamily
	 * @param column
	 * @param jsonStr
	 *            格式如下：{item_id :1,num :1,modify_time:'2015-05-11 16:47:30'}
	 * @throws Exception
	 */
	public static void addMessage(String tableName, String columnFamily,
			String column, String jsonStr) throws Exception {

		if (jsonStr == null) {
			return;
		}
		JSONObject jsonObj = JSONObject.fromObject(jsonStr);
		String item_id = jsonObj.getString("item_id");
		String num = jsonObj.getString("num");
		String modify_time = jsonObj.getString("modify_time");
		long ts = StringUtil.parseTimeStamp(modify_time, "yyyy-MM-dd HH:mm:ss");
		HBaseDao.putCell(tableName, item_id, columnFamily, column, num, ts);

	}

	/**
	 * 增加一条数据到hbase，只有ts大于hbase中的tsVersion才更新，否则不更新也不报错
	 * 
	 * @param tableName
	 *            表名
	 * @param item_id
	 * @return 返回格式如下：{item_id :1,num :1,modify_time:'2015-05-11 16:47:30'}
	 * @throws Exception
	 */
	public static String getMessage(String item_id) throws Exception {

		return getMessage("bfd_item", "item", "num", item_id);

	}

	/**
	 * 从hbase读取一条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param columnFamily
	 * @param column
	 * @param item_id
	 * @return 返回格式如下 ：{item_id :1,num :1,modify_time:'2015-05-11 16:47:30'}
	 * @throws Exception
	 */
	public static String getMessage(String tableName, String columnFamily,
			String column, String item_id) throws Exception {

		if (item_id == null) {
			return null;
		}
		Cell cell = HBaseDao.getColumnLatestCell(tableName, item_id,
				columnFamily, column);

		if (cell == null) {
			return null;
		}

		String num = Bytes.toString(CellUtil.cloneValue(cell));
		String modify_time = StringUtil.formatTime(cell.getTimestamp(),
				"yyyy-MM-dd HH:mm:ss");
		JSONObject jsonObj = new JSONObject();
		jsonObj.put("item_id", item_id);
		jsonObj.put("num", num);
		jsonObj.put("modify_time", modify_time);
		return jsonObj.toString();
	}

	/**
	 * 创建消息表
	 * 
	 * @throws Exception
	 */
	public static void createTable(String tableName, int versions)
			throws Exception {
		HBaseDao.createTable(tableName, new String[] { "0" }, versions);
	}
}
