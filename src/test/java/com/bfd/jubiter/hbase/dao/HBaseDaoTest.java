package com.bfd.jubiter.hbase.dao;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import com.bfd.jubiter.hbase.util.StringUtil;

public class HBaseDaoTest {

	@Test
	public void testCreateTable() throws Exception {
		HBaseDao.createTable("t1", new String[] { "cf1", "cf2", "cf3" });
	}

	@Test
	public void testAddColumnFamily() throws Exception {
		HBaseDao.addColumnFamily("t", "0", Integer.MAX_VALUE);
	}

	@Test
	public void testModifyColumnFamily() throws Exception {
		HBaseDao.modifyColumnFamily("t", "user", Integer.MAX_VALUE);
	}

	@Test
	public void testDeleteColumnFamily() throws Exception {
		HBaseDao.deleteColumnFamily("t", "cf3");
	}

	@Test
	public void testGetColumnLatestCellValue() throws Exception {
		HTable table = new HTable(HBaseDao.conf, "t");
		String rowKey = "r1";
		String columnFamily = "cf1";
		String qualifier = "c1";
		System.err.println(HBaseDao.getColumnLatestCellValue(table, rowKey,
				columnFamily, qualifier));
		table.close();
	}

	@Test
	public void testputCell() throws Exception {
		HTable table = new HTable(HBaseDao.conf, "hj");
		String rowKey = "r1";
		String columnFamily = "item";
		String qualifier = "id";
		String value = "r1,cf1:c1-》我爱北京1";
		HBaseDao.putCell(table, rowKey, columnFamily, qualifier, value);
		table.close();
	}

	@Test
	public void testputRow() throws Exception {
		String tableName = "t";
		String rowKey = "vo0";
		Map<String, String> map = new HashMap<String, String>();
		String key_id = "user:id";
		String key_name = "user:name";
		String key_sex = "user:sex";
		String key_age = "user:age";
		String value_id = "000";
		String value_name = "张三";
		String value_sex = "男";
		String value_age = "21";

		for (int i = 5; i <= 10; i++) {
			map.put(key_id, value_id + i);
			map.put(key_name, value_name + i);
			map.put(key_sex, value_sex + i);
			map.put(key_age, value_age + i);
			HBaseDao.putRow(tableName, rowKey, map);
		}

	}

	@Test
	public void testgetRow() throws Exception {
		String tableName = "t";
		String rowKey = "vo0";
		Map<String, Map<String, String>> map = new LinkedHashMap<String, Map<String, String>>();
		map = HBaseDao
				.getRow(tableName, rowKey, 0L, System.currentTimeMillis());
		int count = 1;
		for (Iterator<Map.Entry<String, Map<String, String>>> iterator = map
				.entrySet().iterator(); iterator.hasNext();) {
			System.err
					.println("==========start=======================================");
			Map.Entry<String, Map<String, String>> entry = iterator.next();
			System.err.println("第" + count++ + "行数据，ts=" + entry.getKey());
			Map<String, String> subMap = entry.getValue();
			for (Iterator<Map.Entry<String, String>> iterator2 = subMap
					.entrySet().iterator(); iterator2.hasNext();) {
				Map.Entry<String, String> subEntry = iterator2.next();
				System.err.print("字段名：" + subEntry.getKey() + "，字段值："
						+ subEntry.getValue() + ";");
			}
			System.err.println();
			System.err
					.println("============end=====================================");
		}
	}

	@Test
	public void testBatchPutCell() throws Exception {
		HTable table = new HTable(HBaseDao.conf, "t");
		String rowKey = "r";
		String columnFamily = "cf";
		String qualifier = "c";
		String value = "-》我爱北京,爱个毛线5";
		int rowCount = 10000;
		int columnFamilyCount = 2;// 不能变，除非alter table columnFamily
		int qualifierCount = 10;
		for (int i = 0; i < rowCount; i++) {
			for (int j = 1; j <= columnFamilyCount; j++) {
				for (int k = 0; k < qualifierCount; k++) {
					String qualifiervalue = String
							.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s",
									rowKey + i, columnFamily + j,
									qualifier + k, value);
					HBaseDao.putCell(table, rowKey + i, columnFamily + j,
							qualifier + k, qualifiervalue);
				}
			}
		}
		table.close();
	}

	/**
	 * By Scan iterate lastest version value of a cell,this use two
	 * matheds:Cell/KeyValue
	 */
	@Test
	public void testScanAll() throws Exception {
//		HTable table = new HTable(HBaseDao.conf, "social_relation");
		HTable table = new HTable(HBaseDao.conf, "base_label");
		//		HTable table = new HTable(HBaseDao.conf, "bfd_item");
		ResultScanner resultScanner = HBaseDao.scanAll(table);
		int line = 0;
		for (Result result : resultScanner) {
			System.err.println(++line
					+ "行=======================================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + StringUtil.formatTime(cell.getTimestamp(), "yyyy-MM-dd HH:mm:ss") + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
				// KeyValue kv = result.list().get(i);
				// System.err
				// .println(String
				// .format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
				// Bytes.toString(kv.getRow()),
				// Bytes.toString(kv.getFamily()),
				// Bytes.toString(kv.getQualifier()),
				// kv.getTimestamp(),
				// Bytes.toString(kv.getValue())));
			}
		}
		table.close();
	}

	/**
	 * By Scan iterate all version value of a cell(or familyColumn),this use two
	 * matheds:Cell/KeyValue
	 */
	@Test
	public void testHBaseScanAllVersion() throws Exception {
		// 下面的配置不管用：必须修改服务器端hbase-site.xml,修改完后重启server。
		// HBaseDao.conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,1000);
		HTable table = new HTable(HBaseDao.conf, "t");
		Scan scan = new Scan();
		// scan.setMaxVersions(3);
		scan.setMaxVersions();
		// scan.setTimeRange(222, 223);//[):大于等于222，小于223
		scan.setTimeRange(0, System.currentTimeMillis());
		// scan.setRaw(true);//可查询出所有已经被打上删除标记但尚未被真正删除的数据，但不能指定任意的列(只能指定列族)，否则会报错，即和下面的行一起使用：
		// String[] columns = {"cf1"};
		// String[] columns = {};
		String[] columns = {
		// "cf1", "cf2"
		"user" };
		// String[] columns = {"cf1:c1","cf1:c2"};

		for (String column : columns) {
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			if (colkey.length > 1) {
				scan.addColumn(colkey[0], colkey[1]);
			} else {
				scan.addFamily(colkey[0]);
			}
		}
		ResultScanner resultScanner = table.getScanner(scan);
		// 必须放在resultScanner行下面：
		// long scannerTimeOut =
		// HBaseDao.conf.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
		// -1);
		// Thread.sleep(scannerTimeOut+1);
		int line = 0;
		for (Result result : resultScanner) {
			System.err.println(++line
					+ "行=======================================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
				// KeyValue kv = result.list().get(i);
				// System.err
				// .println(String
				// .format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
				// Bytes.toString(kv.getRow()),
				// Bytes.toString(kv.getFamily()),
				// Bytes.toString(kv.getQualifier()),
				// kv.getTimestamp(),
				// Bytes.toString(kv.getValue())));
			}
		}
		table.close();
	}

	/**
	 * By Get iterate all version value of a cell,this use two
	 * matheds:Cell/KeyValue
	 */
	@Test
	public void testHBaseGet() throws Exception {
		HTable table = new HTable(HBaseDao.conf, "t");
		String rowKey = "r1";
		String columnFamily = "cf1";
		String qualifier = "c1";
		Get get = new Get(Bytes.toBytes(rowKey));
		// get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		// get.addFamily(Bytes.toBytes(columnFamily));
		// get.setMaxVersions();
		Result result = table.get(get);
		System.err.println("=======================================共有列数："
				+ result.size());
		for (int i = 0; i < result.size(); i++) {
			Cell cell = result.listCells().get(i);
			System.err.println("tableName='"
					+ Bytes.toString(table.getTableName()) + "',rowKey='"
					+ Bytes.toString(CellUtil.cloneRow(cell)) + "',column='"
					+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
					+ Bytes.toString(CellUtil.cloneQualifier(cell))
					+ "',timestamp='" + cell.getTimestamp() + "',value='"
					+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			// KeyValue kv = result.list().get(i);
			// System.err
			// .println(String
			// .format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
			// Bytes.toString(kv.getRow()),
			// Bytes.toString(kv.getFamily()),
			// Bytes.toString(kv.getQualifier()),
			// kv.getTimestamp(),
			// Bytes.toString(kv.getValue())));
		}
		table.close();
	}

	/**
	 * delete specific row value: whole/qualifier/columnFamily
	 */
	@Test
	public void testHBaseDelete() throws Exception {
		HTable table = new HTable(HBaseDao.conf, "bfd_item");

		String rowKey = "22";
		String columnFamily = "cf1";
		String qualifier = "c1";

		Delete del = new Delete(Bytes.toBytes(rowKey));
//		del.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		// del.deleteColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),1);
		// del.deleteColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));
		// del.deleteColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),1);
		// del.deleteFamily(Bytes.toBytes(columnFamily));

		table.delete(del);

		System.err.println(HBaseDao.getColumnLatestCellValue(table, rowKey,
				columnFamily, qualifier));
		table.close();
	}

	/**
	 * query specific row value by PageFilter
	 */
	@Test
	public void testHBasePageFilterScan() throws Exception {
		HTable table = new HTable(HBaseDao.conf, "t");
		Filter filter = new PageFilter(3);
		int totalRows = 0;
		byte[] lastRow = null;
		while (true) {
			Scan scan = new Scan();
			scan.setFilter(filter);
			if (lastRow != null) {
				byte[] POSTFIX = new byte[1];
				POSTFIX[0] = 0b0;
				byte[] startRow = Bytes.add(lastRow, POSTFIX);
				System.err.println("start row: "
						+ Bytes.toStringBinary(startRow));
				scan.setStartRow(startRow);
			}
			ResultScanner scanner = table.getScanner(scan);
			int localRows = 0;
			Result result;
			while ((result = scanner.next()) != null) {
				System.err.println(localRows++ + ":" + result);
				totalRows++;
				lastRow = result.getRow();
			}
			scanner.close();
			if (localRows == 0) {
				break;
			}
			System.err.println("totalRows: " + totalRows);
		}

		String rowKey = "r1";
		String columnFamily = "cf1";
		String qualifier = "c1";

		Delete del = new Delete(Bytes.toBytes(rowKey));
		del.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		// del.deleteColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),1);
		// del.deleteColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));
		// del.deleteColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),1);
		// del.deleteFamily(Bytes.toBytes(columnFamily));

		table.delete(del);

		System.err.println(HBaseDao.getColumnLatestCellValue(table, rowKey,
				columnFamily, qualifier));
		table.close();
	}

	/**
	 * query specific row value by RowFilter
	 */
	@Test
	public void testHBaseRowFilterScan() throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String compareRowKey = "r1";
		int line = 0;
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes(compareRowKey)));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by RowFilter(SubstringComparator)
	 */
	@Test
	public void testHBaseRowFilterScanUseSubstringComparator() throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String compareRowKey = "2";
		int line = 0;
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(compareRowKey));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by RowFilter(RegexStringComparator)
	 */
	@Test
	public void testHBaseRowFilterScanUseRegexStringComparator()
			throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String compareRowKey = "r*2";
		int line = 0;
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new RegexStringComparator(compareRowKey));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by FamilyFilter FamilyFilter:列族（名字）过滤器
	 */
	@Test
	public void testHBaseFamilyFilterScan() throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String comparColumnFamily = "cf2";
		int line = 0;
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(comparColumnFamily)));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by DependentFilter
	 * DependentFilter:参考列过滤器，ValueFilter+时间戳过滤器
	 * 尝试找到该列所在的每一行，并返回该行具有相同时间戳的全部键值对。如果某一行不包含指定的列，则该行的任何键值对都不返回。
	 */
	@Test
	public void testHBaseDependentFilterScan() throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String compareValue = "row:r9, family:cf1, qualifier:c1, qualifiervalue:-》我爱北京";
		int line = 0;
		Scan scan = new Scan();
		// 获取整个columnFamily列族当前Version中的所有timestamp等于参照列"columnFamily:qualifier"的数据:
		// 第三个参数为true时，filter表示对qualifier列以外的所有columnFamily列族的数据做filter操作
		// 第三个参数为false时,表示对所有columnFamily列族的数据做filter操作
		// Filter filter = new
		// DependentColumnFilter(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),false);
		// 获取整个columnFamily列族当前Version中的所有timestamp等于参照列"columnFamily:qualifier"的数据,value以"compareValue"开头的所有数据
		Filter filter = new DependentColumnFilter(Bytes.toBytes(columnFamily),
				Bytes.toBytes(qualifier), false, CompareFilter.CompareOp.EQUAL,
				new BinaryPrefixComparator(Bytes.toBytes(compareValue)));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by SingleColumnValueFilter
	 * 单列值过滤器（SingleColumnValueFilter）：用一列的值决定是否一行数据被过滤掉，不符合该列值条件的行将被过滤。
	 */
	@Test
	public void testHBaseSingleColumnValueFilterScan() throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String compareValue = "北京";
		int line = 0;
		Scan scan = new Scan();
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				CompareFilter.CompareOp.EQUAL, new SubstringComparator(
						compareValue));
		filter.setFilterIfMissing(true);// 当参考列不存在时如何处理改行，默认值false，这一行被包含在结果中。
		filter.setLatestVersionOnly(false);// 默认值true，此时过滤器只检查参考列的最新版本，否则检查所有版本。
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by SingleColumnValueExcludeFilter
	 * 单列排除过滤器（SingleColumnValueExcludeFilter）：和SingleColumnValueFilter一样，
	 * 只是符合条件的行的列中，永远不包含该参考列。
	 */
	@Test
	public void testHBaseSingleColumnValueExcludeFilterScan() throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String compareValue = "北京";
		int line = 0;
		Scan scan = new Scan();
		SingleColumnValueFilter filter = new SingleColumnValueExcludeFilter(
				Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				CompareFilter.CompareOp.EQUAL, new SubstringComparator(
						compareValue));
		filter.setFilterIfMissing(true);// 当参考列不存在时如何处理改行，默认值false，这一行被包含在结果中。
		filter.setLatestVersionOnly(false);// 默认值true，此时过滤器只检查参考列的最新版本，否则检查所有版本。
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by ValueFilter ValueFilter:列值过滤器，不能指定具体的那个列
	 */
	@Test
	public void testHBaseValueFilterScan() throws Exception {

		String columnFamily = "cf1";
		String qualifier = "c1";
		String compareValue = "北京1";
		int line = 0;
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(compareValue));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by PrefixFilter PrefixFilter:所有与前缀匹配的行都会被返回到客户端。
	 */
	@Test
	public void testHBasePrefixFilterScan() throws Exception {

		String prifixRowKey = "r9";
		int line = 0;
		Scan scan = new Scan();
		Filter filter = new PrefixFilter(Bytes.toBytes(prifixRowKey));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by FirstKeyOnlyFilter
	 * 4、首次行键过滤器（FirstKeyOnlyFilter）
	 * 41、使用场景1：用户需要访问一行中的第一列（测试结果：按hbase字典顺序排列，即时间戳最新（值最大）的列【权威指南说：最旧值最小列】）
	 * 42、使用场景2：行数统计 43、特性：它在检查完第一列之后会通知region结束对当前行的扫描，与全表扫描相比，性能更优。
	 */
	@Test
	public void testHBaseFirstKeyOnlyFilterScan() throws Exception {

		int line = 0;
		Scan scan = new Scan();
		Filter filter = new FirstKeyOnlyFilter();
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by InclusiveStopFilter
	 * InclusiveStopFilter:包含结束过滤器，指定具体的结束行(包含该结束行)
	 */
	@Test
	public void testHBaseInclusiveStopFilterScan() throws Exception {

		String startRowKey = "r1";
		String endRowKey = "r3";
		int line = 0;
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRowKey));
		// scan.setStopRow(Bytes.toBytes(endRowKey));
		Filter filter = new InclusiveStopFilter(Bytes.toBytes(endRowKey));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by TimestampsFilter
	 * 时间戳过滤器（TimestampsFilter）：需要传入一个装载了时间戳的list实例。可以在不指定版本的情况下，
	 * 通过list中的时间戳找到老版本的列数据。
	 */
	@Test
	public void testHBaseTimestampsFilterScan() throws Exception {

		List<Long> tsList = new ArrayList<Long>();
		tsList.add(1421287488147L);// 可以在不指定版本的情况下，通过list中的时间戳找到老版本的列数据。
		tsList.add(1421894021867L);
		tsList.add(1421894021873L);
		int line = 0;
		Scan scan = new Scan();
		Filter filter = new TimestampsFilter(tsList);
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by ColumnCountGetFilter
	 * 列计数过滤器（ColumnCountGetFilter）：限制每行最多取回多少列。构造要传入列数n。
	 * 当有某行r1某列族的cf1的列记录数>n时，该过滤器会使整个扫描操作停止扫描其他未扫描的的列族数据，所以该filter适合在get方法中。
	 * 如：扫描到r1的cf2时，该行结果集列记录数>n,则r1行返回n条列记录，同时其他行也只扫描<cf2的列族，即cf1
	 * 如果扫描到r1的cf1时，该行结果集列记录数>n,则r1行返回n条列记录，同时其他行也只扫描<cf1的列族，即无记录返回。
	 */
	@Test
	public void testHBaseColumnCountGetFilterScan() throws Exception {

		int line = 0;
		Scan scan = new Scan();
		Filter filter = new ColumnCountGetFilter(15);
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * use Get query specific row value by ColumnCountGetFilter
	 * 列计数过滤器（ColumnCountGetFilter）：限制每行最多取回多少列。构造要传入列数n。
	 * 当有某行r1某列族的cf1的列记录数>n时，该过滤器会使整个扫描操作停止扫描其他未扫描的的列族数据，所以该filter适合在get方法中。
	 * 如：扫描到r1的cf2时，该行结果集列记录数>n,则r1行返回n条列记录，同时其他行也只扫描<cf2的列族，即cf1
	 * 如果扫描到r1的cf1时，该行结果集列记录数>n,则r1行返回n条列记录，同时其他行也只扫描<cf1的列族，即无记录返回。
	 */
	@Test
	public void testHBaseColumnCountGetFilterByGet() throws Exception {
		HTable table = new HTable(HBaseDao.conf, "t");
		int line = 0;
		String rowKey = "r1";
		String columnFamily = "cf1";
		String qualifier = "c1";
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		get.setMaxVersions();// 和下面的addFamily+ColumnCountGetFilter一块使用就不好用,不知为何
		// get.addFamily(Bytes.toBytes(columnFamily));
		Filter filter = new ColumnCountGetFilter(3);
		get.setFilter(filter);
		Result result = table.get(get);
		System.err.println(++line + "行=================共有列数：" + result.size());
		List<Cell> cellList = result.listCells();
		for (Cell cell : cellList) {
			System.err.println("tableName='"
					+ Bytes.toString(table.getTableName()) + "',rowKey='"
					+ Bytes.toString(CellUtil.cloneRow(cell)) + "',column='"
					+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
					+ Bytes.toString(CellUtil.cloneQualifier(cell))
					+ "',timestamp='" + cell.getTimestamp() + "',value='"
					+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
		}
		table.close();
	}

	/**
	 * query specific row value by ColumnPaginationFilter
	 * 列分页过滤器（ColumnPaginationFilter）：对一行的所有列进行分页。 构造器需要两个参数：
	 * offset（每行的列记录中，第offset个记录及其以前的都将被过滤掉）
	 * limit（从offset+1个列记录开始，依次limit个列记录将被返回到client）
	 */
	@Test
	public void testHBaseColumnPaginationFilterScan() throws Exception {

		int line = 0;
		Scan scan = new Scan();
		Filter filter = new ColumnPaginationFilter(3, 15);
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by ColumnPrefixFilter
	 * 列(名)前缀过滤器（ColumnPrefixFilter）：构造器需要1个参数：列前缀
	 */
	@Test
	public void testHBaseColumnPrefixFilterScan() throws Exception {
		String prfixQualifier = "c1";
		int line = 0;
		Scan scan = new Scan();
		Filter filter = new ColumnPrefixFilter(Bytes.toBytes(prfixQualifier));
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by RandomRowFilter 随机行过滤器（RandomRowFilter）：
	 * 构造器需要1个参数：float chance chance小于0会导致所有结果被过滤掉 chance大于1会导致返回所有行
	 * 0<chance<1,会随即使部分行被过滤掉（某行用Random.nextFloat>=chance判断，为true则不被过滤掉）,
	 * chance越接近1返回的行越多
	 */
	@Test
	public void testHBaseRandomRowFilterScan() throws Exception {
		// float chance = -1.1f;//chance小于0会导致所有结果被过滤掉
		// float chance = 1.1f;chance大于1会导致返回所有行
		float chance = 0.3f;// 0<chance<1,会随即使部分行被过滤掉,chance越接近1返回的行越多
		int line = 0;
		Scan scan = new Scan();
		Filter filter = new RandomRowFilter(chance);
		scan.setFilter(filter);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by SkipFilter SkipFilter:列值过滤器，不能指定具体的那个列
	 */
	@Test
	public void testHBaseSkipFilterScan() throws Exception {

		String compareValue = "row:r0, family:cf1, qualifier:c0";
		int line = 0;
		Scan scan = new Scan();
		Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(compareValue));
		Filter filter2 = new SkipFilter(filter1);
		scan.setFilter(filter2);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by WhileMatchFilter
	 * 全匹配过滤器:包装过滤器，当一条数据被过滤掉时，它就会直接放弃以后的所有扫描，直接返回当前的结果。
	 */
	@Test
	public void testHBaseWhileMatchFilterScan() throws Exception {

		String compareRowKey = "r1";
		int line = 0;
		Scan scan = new Scan();
		// 只过滤1行：只跳过RowKey='r1'的行
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL,
				new BinaryComparator(Bytes.toBytes(compareRowKey)));
		// 只输出1行：RowKey='r0'的行，因为到r1时，扫描就停止了，因而也扫描不到r2/r3/r4...
		Filter filter2 = new WhileMatchFilter(filter1);
		scan.setFilter(filter2);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * query specific row value by FilterList 3、FilterList：需要多个过滤器共同限制返回结果时使用。
	 * 31、参数1：FilterList.Operator,可选枚举值有MUST_PASS_ALL(当所有过滤器都允许包含这个值时，
	 * 这个值才会被包含到返回的结果中)/MUST_PASS_ONE（只要有一个过滤器允许包含这个值）
	 * 32、参数2：List<Filter>,可通过list中过滤器的顺序来进一步精确控制过滤器的执行顺序；
	 * 33、多级过滤器实现1:由于FilterList也实现了Filter接口，所以List<Filter>中也可有FilterList实例作为元素，
	 * 从而实现多级过滤器。 34、多级过滤器实现2:也可通过实例FilterList.addFilter(Filter)添加。
	 */
	@Test
	public void testHBaseFilterListScan() throws Exception {

		String compareRowKey1 = "r1";
		String compareRowKey2 = "r2";
		String compareRowKey3 = "r1";

		int line = 0;
		Scan scan = new Scan();
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(compareRowKey1)));
		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(compareRowKey2)));
		Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(compareRowKey3)));

		List<Filter> l1 = new ArrayList<Filter>();
		l1.add(filter1);
		l1.add(filter2);
		// 取或集：f1或f2
		Filter filter4 = new FilterList(FilterList.Operator.MUST_PASS_ONE, l1);

		List<Filter> l2 = new ArrayList<Filter>();
		l2.add(filter4);
		// 默认值取并集：实现多级filter过滤，（f1或f2）并f3
		FilterList filter5 = new FilterList(l2);
		filter5.addFilter(filter3);

		scan.setFilter(filter5);
		HTable table = new HTable(HBaseDao.conf, "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.err.println(++line + "行=================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
			}
		}
		scanner.close();
		table.close();
	}

	/**
	 * incr column 单计数器
	 */
	@Test
	public void testHBaseSingleIncr() throws Exception {

		String rowKey = "r10";
		String columnFamily = "cf2";
		String qualifier = "c2";
		int amount = 1;
		HTable table = new HTable(HBaseDao.conf, "t");

		long incr1 = table.incrementColumnValue(Bytes.toBytes(rowKey),
				Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), amount);
		System.err.println("====当前列增" + amount + "后值为=====>" + incr1);

		incr1 = table
				.incrementColumnValue(Bytes.toBytes(rowKey),
						Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
						++amount);
		System.err.println("====当前列增" + amount + "后值为=====>" + incr1);

		incr1 = table.incrementColumnValue(Bytes.toBytes(rowKey),
				Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), 0);
		System.err.println("====得到当前计数器的值为=====>" + incr1);

		incr1 = table.incrementColumnValue(Bytes.toBytes(rowKey),
				Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), -incr1);
		System.err.println("====当前列计数器值清零，值为=====>" + incr1);

		table.close();
	}

	/**
	 * ？？？？mutiIncr column 多计数器
	 */
	@Test
	public void testHBaseMutiIncr() throws Exception {

		String rowKey = "r11";
		String columnFamily = "cf1";
		String qualifier = "c1";
		int amount = 1;
		Increment incr1 = new Increment(Bytes.toBytes(rowKey));
		incr1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				amount);
		qualifier = "c2";
		incr1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				amount);

		HTable table = new HTable(HBaseDao.conf, "t");
		Result res = table.increment(incr1);
		System.err.println(rowKey + "行共有列数=======>" + res.size());
		for (Cell cell : res.rawCells()) {
			System.err.println("tableName='"
					+ Bytes.toString(table.getTableName()) + "',rowKey='"
					+ Bytes.toString(CellUtil.cloneRow(cell)) + "',column='"
					+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
					+ Bytes.toString(CellUtil.cloneQualifier(cell))
					+ "',timestamp='" + cell.getTimestamp() + "',value='"
					+ Bytes.toLong(CellUtil.cloneValue(cell)) + "'");
		}

		amount = 10;
		qualifier = "c1";
		incr1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				amount);
		qualifier = "c2";
		incr1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				amount);

		res = table.increment(incr1);
		System.err.println(rowKey + "行共有列数=======>" + res.size());
		for (Cell cell : res.rawCells()) {
			System.err.println("tableName='"
					+ Bytes.toString(table.getTableName()) + "',rowKey='"
					+ Bytes.toString(CellUtil.cloneRow(cell)) + "',column='"
					+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
					+ Bytes.toString(CellUtil.cloneQualifier(cell))
					+ "',timestamp='" + cell.getTimestamp() + "',value='"
					+ Bytes.toLong(CellUtil.cloneValue(cell)) + "'");
		}

		columnFamily = "cf2";
		qualifier = "c1";
		amount = 10;
		incr1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				amount);
		qualifier = "c2";
		incr1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
				amount);

		res = table.increment(incr1);
		System.err.println(rowKey + "行共有列数=======>" + res.size());
		for (Cell cell : res.rawCells()) {
			System.err.println("tableName='"
					+ Bytes.toString(table.getTableName()) + "',rowKey='"
					+ Bytes.toString(CellUtil.cloneRow(cell)) + "',column='"
					+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
					+ Bytes.toString(CellUtil.cloneQualifier(cell))
					+ "',timestamp='" + cell.getTimestamp() + "',value='"
					+ Bytes.toLong(CellUtil.cloneValue(cell)) + "'");
		}

		table.close();
	}

	/**
	 * 协处理器测试
	 * 
	 * @throws IOException
	 */
	@Test
	public void testLoadWithTableDescriptor() throws IOException {
		// Configuration conf = HBaseConfiguration.create();
		// FileSystem fs = FileSystem.get(conf);
		HTableDescriptor htd = new HTableDescriptor("USER");
		String s = htd.getValue("TABLE_ATTRIBUTES");
		System.err.println(htd.getCoprocessors().get(0));
	}

	public void scan(int caching, int batch, int rpc) throws Exception {
		// Logger log = Logger.getLogger("org.apache.hadoop");
		final int[] counters = { 0, 0 };
		// Appender appender = new AppenderSkeleton() {
		//
		// @Override
		// public boolean requiresLayout() {
		// return false;
		// }
		//
		// @Override
		// public void close() {
		// }
		//
		// @Override
		// protected void append(LoggingEvent event) {
		// String msg = event.getMessage().toString();
		// if (msg != null && msg.contains("Call:next")) {
		// counters[0]++;
		// }
		// }
		// };
		// log.removeAllAppenders();
		// log.setAdditivity(false);
		// log.addAppender(appender);
		// log.setLevel(Level.DEBUG);

		Scan scan = new Scan();
		scan.setCaching(caching);
		scan.setBatch(batch);

		HTable table = new HTable(HBaseConfiguration.create(), "t");
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			counters[1]++;
			System.err.println(counters[1]
					+ "行=======================================共有列数："
					+ result.size());
			List<Cell> cellList = result.listCells();
			for (Cell cell : cellList) {
				System.err.println("tableName='"
						+ Bytes.toString(table.getTableName()) + "',rowKey='"
						+ Bytes.toString(CellUtil.cloneRow(cell))
						+ "',column='"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "',timestamp='" + cell.getTimestamp() + "',value='"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "'");
				// KeyValue kv = result.list().get(i);
				// System.err
				// .println(String
				// .format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
				// Bytes.toString(kv.getRow()),
				// Bytes.toString(kv.getFamily()),
				// Bytes.toString(kv.getQualifier()),
				// kv.getTimestamp(),
				// Bytes.toString(kv.getValue())));
			}
		}
		scanner.close();
		String format = "cache:%s,batch:%s,results:%s,RPCs:%s,预测RPCs:%s.";
		System.err.println(String.format(format, caching, batch, counters[1],
				counters[1] / caching, rpc));
	}

	/**
	 * hbase客户端设置缓存优化查询:batch/cache
	 * 
	 * @throws Exception
	 */
	@Test
	public void testClientCacheAndBatch() throws Exception {
		// scan(1, 1,208);
		// scan(200, 1,208);
		// scan(200, 10,208);
		// scan(2000, 100,208);
		// scan(2, 100,208);
		// scan(1, 10,208);
		// scan(5, 100,208);
		// scan(5, 20,208);
		scan(10, 1, 208);
	}

	@Test
	public void testHTablePool() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("table1"));
		try {
			// Use the table as needed, for a single operation and a single
			// thread
			String rowKey = "testHTablePool-r0";
			String columnFamily = "cf1";
			String qualifier = "c1";
			String value = "-》我爱北京,爱个毛线testHTablePool";
			Put p1 = new Put(Bytes.toBytes(rowKey));
			p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(p1);
			System.out.println("put '" + rowKey + "', '" + columnFamily + ":"
					+ qualifier + "', '" + value + "'");
		} finally {
			table.close();
			connection.close();
		}
	}

	@Test
	public void createTableACL() {
		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser("hbase");
		ugi.doAs(new PrivilegedAction<Void>() {
			@Override
			public Void run() {
				Configuration conf = HBaseConfiguration.create();
				 conf.set("hbase.zookeeper.quorum", "172.18.1.46");
				 conf.set("zookeeper.znode.parent", "/bfdhbase");
				HBaseAdmin admin;
				try {
					admin = new HBaseAdmin(conf);
					HTableDescriptor hd = new HTableDescriptor("bfd_item");
					hd.addFamily(new HColumnDescriptor("item"));
					admin.createTable(hd);
				} catch (Exception e) {
					e.printStackTrace();
				}

				return null;
			}
		});

	}

}
