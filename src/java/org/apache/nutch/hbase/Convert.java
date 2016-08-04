package org.apache.nutch.hbase;

/**
 * @author ��Զ��
 */
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

;
public class Convert {
	public static double ToDouble(Object value) {
		if (value == null) {
			return 0.0;
		} else {
			if (value instanceof Double)
				return ((Double) value).doubleValue();
			return ToDouble(value.toString());
		}
	}

	public static double ToDouble(String value) {
		try {
			Double Dbl = Double.parseDouble(value.trim());
			return Dbl.doubleValue();
		} catch (Exception e) {
			return 0.0;
		}
	}

	public static float ToFloat(Object value) {
		if (value == null) {
			return 0;
		} else {
			if (value instanceof Float)
				return ((Float) value).floatValue();
			return ToFloat(value.toString());
		}
	}

	public static float ToFloat(String value) {
		try {
			Float f = Float.parseFloat(value.trim());
			return f.floatValue();
		} catch (Exception e) {
			return 0;
		}
	}

	public static int ToInt(Object value) {
		if (value == null) {
			return 0;
		} else {
			if (value instanceof Integer)
				return ((Integer) value).intValue();
			return ToInt(value.toString());
		}
	}

	public static int ToInt(String value) {
		try {
			Integer i = Integer.parseInt(value.trim());
			return i.intValue();
		} catch (Exception e) {
			return 0;
		}
	}

	public static int ToInt(String value, int type) {
		try {
			if (type == 16) {
				int temp = 0;
				if (value.startsWith("0x"))// ��ɫֵ
				{
					value = value.substring(2);
				}
				for (int i = 0; i < value.length(); i++) {
					char c = value.charAt(i);
					int t = 0;
					if (c <= '9') {
						t = ToInt(c + "");
					} else {
						switch (c) {
						case 'a':
						case 'A':
							t = 10;
							break;
						case 'b':
						case 'B':
							t = 11;
							break;
						case 'c':
						case 'C':
							t = 12;
							break;
						case 'd':
						case 'D':
							t = 13;
							break;
						case 'e':
						case 'E':
							t = 14;
							break;
						case 'F':
						case 'f':
							t = 15;
							break;
						}
					}
					java.math.BigDecimal bg = java.math.BigDecimal.valueOf(type);
					bg = bg.pow(value.length() - i - 1);
					temp += t * bg.intValue();
				}
				// System.out.println(temp);
				return temp;
			} else {
				return ToInt(value);
			}
		} catch (Exception e) {
			return 0;
		}
	}

	public static int ToInt(char value) {
		try {
			Byte bt = Byte.parseByte(value + "");
			return bt.intValue();
		} catch (Exception e) {
			return 0;
		}
	}

	public static int ToInt(boolean value) {
		if (value)
			return 1;
		else
			return 0;
	}

	public static boolean ToBool(Object value) {
		if (value instanceof Boolean)
			return ((Boolean) value).booleanValue();
		return ToBool(value.toString());
	}

	public static boolean ToBool(String value) {
		try {
			Boolean b = Boolean.parseBoolean(value.trim());
			return b.booleanValue();
		} catch (Exception e) {
			return false;
		}
	}

	public static boolean ToBool(int value) {
		if (value != 0) {
			return true;
		} else {
			return false;
		}
	}

	public static long ToLong(Object value) {
		if (value == null) {
			return 0;
		} else {
			if (value instanceof Long)
				return ((Long) value).longValue();
			return ToLong(value.toString());
		}
	}

	public static long ToLong(String value) {
		try {
			Long l = Long.parseLong(value.trim());
			return l.longValue();
		} catch (Exception e) {
			return 0;
		}
	}

	public static String ToString(Object value) {
		return value == null ? "" : value.toString();
	}

	public static String ToString(boolean value) {
		if (value) {
			return "true";
		} else {
			return "false";
		}
	}

	public static String ToString(Date d, String format) {
		java.util.Calendar c = Calendar.getInstance();
		c.setTime(d);
		String result = "";
		format = format.toLowerCase();
		if (format.equals("yyyy-mm-dd hh:mi:ss")) {
			java.text.DateFormat df = java.text.DateFormat.getDateTimeInstance();
			// (java.text.DateFormat.SECOND_FIELD);
			// df.setTimeZone(java.util.TimeZone.getTimeZone("GMT+8"));
			// java.text.DateFormat.FULL,java.text.DateFormat.FULL,java.util.Locale.CHINESE
			result = df.format(d);
		} else if (format.equals("yyyy-mm-dd") || format.equals("yyyy/mm/dd") || format.equals("dd/mm/yyyy") || format.equals("mm/dd/yyyy")) {
			java.text.DateFormat df = java.text.DateFormat.getDateInstance();
			result = df.format(d);
		} else if (format.equals("hh:mi:ss")) {
			java.text.DateFormat df = java.text.DateFormat.getTimeInstance();
			result = df.format(d);
		} else if (format.equals("yyyy��mm��dd��")) {
			result = (d.getYear() + 1900) + "��" + d.getMonth() + "��" + d.getDate() + "��";
		} else {
			result = d.toLocaleString();
		}
		return result;
	}

	public static Date ToDate(Object date) {
		if (date == null) {
			return null;
		} else {
			return ToDate(date.toString());
		}
	}

	public static Date ToDate(String date) {
		java.util.Calendar c = Calendar.getInstance();
		java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2} +\\d{1,2}:\\d{1,2}:\\d{1,2}");// yyyy-mm-dd
		java.util.regex.Matcher m = p.matcher(date);
		boolean isSet = false;
		if (m.find()) {
			String match = date.subSequence(m.start(), m.end()).toString();
			String[] temp = match.split("-| |:");
			c.set(ToInt(temp[0]), ToInt(temp[1]) - 1, ToInt(temp[2]), ToInt(temp[3]), ToInt(temp[4]), ToInt(temp[5]));
			isSet = true;
		} else {
			p = java.util.regex.Pattern.compile("(\\d{4}-\\d{1,2}-\\d{1,2})|(\\d{4}/\\d{1,2}/\\d{1,2})");// yyyy-mm-dd
			// yyyy/mm/dd
			m = p.matcher(date);
			if (m.find()) {
				String match = date.subSequence(m.start(), m.end()).toString();
				String[] temp = match.split("-|/");
				c.set(ToInt(temp[0]), ToInt(temp[1]) - 1, ToInt(temp[2]));
				isSet = true;
			}
		}
		if (isSet) {
			Date d = c.getTime();
			return d;
		} else {
			try {
				Date d = new Date(date);
				return d;
			} catch (Exception e) {
				return null;
			}
		}
	}

	public static Date ToDate(String date, String format) {
		format = format.toLowerCase();
		java.util.Calendar c = Calendar.getInstance();
		if (format.equals("yyyy-mm-dd hh:mm:ss")) {
			String[] temp = date.split("/-| |:/g");
			c.set(ToInt(temp[0]), ToInt(temp[1]), ToInt(temp[2]), ToInt(temp[3]), ToInt(temp[4]), ToInt(temp[5]));
		} else if (format.equals("yyyy-mm-dd") || format.equals("yyyy/mm/dd")) {
			String[] temp = date.split("-|/");
			c.set(ToInt(temp[0]), ToInt(temp[1]), ToInt(temp[2]));
		} else if (format.equals("dd/mm/yyyy")) {
			String[] temp = date.split("/");
			c.set(ToInt(temp[2]), ToInt(temp[1]), ToInt(temp[0]));
		} else if (format.equals("mm/dd/yyyy")) {
			String[] temp = date.split("/");
			c.set(ToInt(temp[2]), ToInt(temp[0]), ToInt(temp[1]));
		} else {
			try {
				java.text.DateFormat df = null;
				if (date.indexOf(" ") > 0)
					df = java.text.DateFormat.getDateTimeInstance();
				else
					df = java.text.DateFormat.getDateInstance();
				c.setTime(df.parse(date));
			} catch (Exception e) {
				return null;
			}
		}
		Date d = c.getTime();
		return d;
	}

	public static String join(Object[] o, String sp) {
		StringBuffer str = new StringBuffer();
		if (o != null && o.length > 0) {
			str.append(o[0]);
			for (int i = 1; i < o.length; i++) {
				str.append(sp + o[i]);
			}
		}
		return str.toString();
	}

	public static String join(int[] o, String sp) {
		StringBuffer str = new StringBuffer();
		if (o != null && o.length > 0) {
			str.append(o[0]);
			for (int i = 1; i < o.length; i++) {
				str.append(sp + o[i]);
			}
		}
		return str.toString();
	}

	public static String join(String[] o) {
		return join(o, ",");
	}

	public static String join(String[] o, String sp) {
		StringBuffer str = new StringBuffer();
		if (o != null && o.length > 0) {
			str.append(o[0]);
			for (int i = 1; i < o.length; i++) {
				str.append(sp + o[i]);
			}
		}
		return str.toString();
	}

	public static String join(ArrayList o, String sp) {
		StringBuffer str = new StringBuffer();
		if (o != null && o.size() > 0) {
			str.append(o.get(0));
			for (int i = 1; i < o.size(); i++) {
				str.append(sp + o.get(i));
			}
		}
		return str.toString();
	}

	public static boolean orderData(Object data[][], int index) {
		ObjArrCmp c = new ObjArrCmp();
		c.index = index;
		java.util.Arrays.sort(data, c);
		return true;
	}

	public static class ObjArrCmp implements java.util.Comparator<Object[]> {
		public int index = 0;

		public int compare(Object[] a, Object[] b) {
			if (a[index] == null && b[index] == null)
				return 0;
			else if (a[index] == null)
				return -1;
			else if (b[index] == null)
				return 1;
			return a[index].toString().compareTo(b[index].toString());
		}
	}
}
