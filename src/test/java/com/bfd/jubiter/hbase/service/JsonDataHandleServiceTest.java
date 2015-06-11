package com.bfd.jubiter.hbase.service;

import org.junit.Test;

import com.bfd.jubiter.hbase.util.StringUtil;

public class JsonDataHandleServiceTest {

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			addMessageTest(i);
			getMessageTest(String.valueOf(i));
		}
		long end = System.currentTimeMillis();
		System.err.println("耗时："+(end-start)/1000+"s");
	}

	public static void addMessageTest(int item_id) throws Exception {
		String jsonStr = "{item_id :"
				+ item_id
				+ ",num :"
				+ item_id
				+ ",modify_time:'"
				+ StringUtil.formatTime(System.currentTimeMillis(),
						"yyyy-MM-dd HH:mm:ss") + "'}";
		JsonDataHandleService.addMessage(jsonStr);
	}

	public static void getMessageTest(String item_id) throws Exception {
		String jsonStr = JsonDataHandleService.getMessage(item_id);
//		System.out.println(jsonStr);
	}

	@Test
	public void createTableTest() throws Exception {
		JsonDataHandleService.createTable("bfd_item", 1);
	}
}
