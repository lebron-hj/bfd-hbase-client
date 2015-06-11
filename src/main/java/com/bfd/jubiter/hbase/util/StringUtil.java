package com.bfd.jubiter.hbase.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StringUtil {
	public static long parseTimeStamp(String timeStr, String formatStr) {
		SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
		try {
			Date d = sdf.parse(timeStr);
			return d.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public static String formatTime(long time, String formatStr) {
		SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
		Date d = new Date(time);
		return sdf.format(d);
	}
}
