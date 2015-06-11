package com.bfd.jubiter.hbase.util;

import org.junit.Test;

public class StringUtilTest {
	
	@Test
	public void parseTimestampTest() {
		long t = StringUtil.parseTimeStamp("2015-05-12 16:47:30", "yyyy-MM-dd HH:mm:ss");
		System.out.println(t);
	}
	
	@Test
	public void formatTimeTest() {
		String t = StringUtil.formatTime(1431334050000l, "yyyy-MM-dd HH:mm:ss");
		System.out.println(t);
	}
}
