//package utils;
//
//import java.text.DateFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.Calendar;
//
//public class TimeDateUtils {
//
//    public static final String YYYYMMDD = "yyyy-MM-dd";
//    public static final String YYYYMMDD_ZH = "yyyyMMdd";
//    public static final int FIRST_DAY_OF_WEEK = Calendar.MONDAY;
//    public static final String YYYYMM = "yyyy-MM";
//
//
//    public static void main(String args[]) throws ParseException {
////        String str_begin = "2018-05-01 05:23:41.277";
////        String str_end = "2020-08-29 08:28:41.277";
//        String timemin = "2020-06-30 00:03:09";
//        String timemax = "2020-08-06 00:03:09";
//
//        System.out.println(getWeeks(timemin,timemax));
//        //getMonths(str_begin, str_end) ;
//        //getQuarter(str_begin,str_end);
//        //getYears(str_begin, str_end) ;
//    }
//    public static void getYears(String begins, String ends) throws ParseException {
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        Date begin = new Date();
//        Date end = new Date();
//        try {
//            begin = sdf.parse(begins);
//            end = sdf.parse(ends);
//        } catch (ParseException e) {
//            System.out.println("日期输入格式不对");
//            return;
//        }
//        Calendar cal_begin = Calendar.getInstance();
//        cal_begin.setTime(begin);
//        Calendar cal_end = Calendar.getInstance();
//        cal_end.setTime(end);
//        while (true) {
//
//            if (cal_begin.get(Calendar.YEAR) == cal_end.get(Calendar.YEAR)) {
//                System.out.println(sdf.format(cal_begin.getTime())+"~"+sdf.format(cal_end.getTime()));
//                break;
//            }else{
//                String str_begin = sdf.format(cal_begin.getTime());
//                String str_end = getMonthEnd(cal_begin.getTime());
//                int years=getYear(str_begin);
//                String year=years+"-12"+"-31";
//                System.out.println(str_begin+"~"+year);
//                cal_begin.add(Calendar.YEAR, 1);
//                cal_begin.set(Calendar.DAY_OF_YEAR, 1);
//
//            }
//
//        }
//
//    }
//
//    public static Map<String,String> getQuarter(String begins, String ends) {
//
//        Map<String, String> map = new LinkedHashMap<String, String>();
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        Date begin = new Date();
//        Date end = new Date();
//        try {
//            begin = sdf.parse(begins);
//            end = sdf.parse(ends);
//        } catch (ParseException e) {
//            System.out.println("日期输入格式不对");
//            return map;
//        }
//        Calendar cal_begin = Calendar.getInstance();
//        cal_begin.setTime(begin);
//        Calendar cal_end = Calendar.getInstance();
//        cal_end.setTime(end);
//        while (true) {
//
//            String str_begin = sdf.format(cal_begin.getTime());
//            String str_end = getMonthEnd(cal_begin.getTime());
//            Date begin_date = parseDate(str_end);
//            Date end_date = parseDate(str_end);
//            String Quarter_begin=formatDate(getFirstDateOfSeason(begin_date));
//            String Quarter_end=formatDate(getLastDateOfSeason(end_date));
//            Date Quarter_begin_date = parseDate(Quarter_begin);
//            Date Quarter_end_date = parseDate(Quarter_end);
//
//
//            if(Quarter_end_date.getTime()==end_date.getTime()){
//
//                if(Quarter_begin_date.getTime()<=begin.getTime()){
//                    Quarter_begin=begins;
//                }
//                if(Quarter_end_date.getTime()>=end.getTime()){
//                    Quarter_end=ends;
//                }
//                map.put(Quarter_begin,Quarter_end);
//                //System.out.println(Quarter_begin+"~"+Quarter_end);
//                if (end.getTime() <=end_date.getTime()) {
//                    break;
//                }
//            }else if(Quarter_begin_date.getTime()==begin_date.getTime()){
//                if(Quarter_begin_date.getTime()<=begin.getTime()){
//                    Quarter_begin=begins;
//                }
//                if(Quarter_end_date.getTime()>=end.getTime()){
//                    Quarter_end=ends;
//                }
//                map.put(Quarter_begin,Quarter_end);
//                //System.out.println(Quarter_begin+"~"+Quarter_end);
//            }
//
//            cal_begin.add(Calendar.MONTH, 1);
//            cal_begin.set(Calendar.DAY_OF_MONTH, 1);
//        }
//        return map;
//    }
//
//
//
//    public static Map<String,String> getMonths(String begins, String ends) {
//
//        Map<String, String> map = new LinkedHashMap<String, String>();
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        Date begin = new Date();
//        Date end = new Date();
//        try {
//            begin = sdf.parse(begins);
//            end = sdf.parse(ends);
//        } catch (ParseException e) {
//            System.out.println("日期输入格式不对");
//            return map;
//        }
//
//        Calendar cal_begin = Calendar.getInstance();
//        cal_begin.setTime(begin);
//        Calendar cal_end = Calendar.getInstance();
//        cal_end.setTime(end);
//        while (true) {
//            if (cal_begin.get(Calendar.YEAR) == cal_end.get(Calendar.YEAR)&& cal_begin.get(Calendar.MONTH) == cal_end.get(Calendar.MONTH)) {
//                //System.out.println(sdf.format(cal_begin.getTime())+"~"+sdf.format(cal_end.getTime()));
//                //新增
//                map.put(sdf.format(cal_begin.getTime()),sdf.format(cal_end.getTime()));
//                break;
//            }
//            String str_begin = sdf.format(cal_begin.getTime());
//            String str_end = getMonthEnd(cal_begin.getTime());
//            //新增
//            map.put(str_begin,str_end);
//            //System.out.println(str_begin+"~"+str_end);
//            cal_begin.add(Calendar.MONTH, 1);
//            cal_begin.set(Calendar.DAY_OF_MONTH, 1);
//        }
//        return map;
//    }
//
//    public static Map<String,String> getWeeks(String begins, String ends) throws ParseException {
//
//        Map<String, String> map = new LinkedHashMap<String, String>();
//
//        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
//        SimpleDateFormat sdw = new SimpleDateFormat("E");
//        String begin_date =begins;
//        String end_date =ends;
//        //String begin_date_fm = begin_date.substring(0, 4) + "-" + begin_date.substring(4,6) + "-" + begin_date.substring(6,8);
//        // String end_date_fm =  end_date.substring(0, 4) + "-" + end_date.substring(4,6) + "-" + end_date.substring(6,8);
//        String begin_date_fm =  begins;
//        String end_date_fm = ends;
//        Date b = null;
//        Date e = null;
//        try {
//            b = sd.parse(begin_date_fm);
//            e = sd.parse(end_date_fm);
//        } catch (ParseException ee) {
//            ee.printStackTrace();
//        }
//        Calendar rightNow = Calendar.getInstance();
//        rightNow.setTime(b);
//        Date time = b;
//        String year = begin_date_fm.split("-")[0];
//        String mon = Integer.parseInt(begin_date_fm.split(" ")[0].split("-")[1])<10?begin_date_fm.split("-")[1]:begin_date_fm.split("-")[1];
//        String day = Integer.parseInt(begin_date_fm.split(" ")[0].split("-")[2])<10?begin_date_fm.split("-")[2]:begin_date_fm.split("-")[2];
//        String timeb = year+mon+day;
//        String timee = null;
//        if(begin_date==end_date){
//            // System.out.println(begin_date+"~"+end_date);
//            map.put(begin_date,end_date);
//        }else{
//            while(time.getTime()<=e.getTime()){
//                rightNow.add(Calendar.DAY_OF_YEAR,1);
//                time = sd.parse(sd.format(rightNow.getTime()));
//                if(time.getTime()>e.getTime()){break;}
//                String timew = sdw.format(time);
//                if(("星期一").equals(timew)){
//                    timeb = (sd.format(time)).replaceAll("-", "");
//                }
//                if(("星期日").equals(timew) || ("星期七").equals(timew) || time.getTime() == e.getTime()){
//                    timee = (sd.format(time)).replaceAll("-", "");
//                    String begindate=fomaToDatas(timeb);
//                    String enddate=fomaToDatas(timee);
//                    //  System.out.println(begindate+"~"+enddate);
//                    map.put(begindate,enddate);
//                }
//            }
//
//        }
//        return map;
//    }
//
//
//    public static String quarterOfYear(int month){   //获取当前日期的季度
//        String jd;
//        if(month>=1&&month<=3){
//            jd="Q1";
//            return jd;
//        }
//        else if(month>=4&&month<=6){
//            jd="Q2";
//            return jd;
//        }
//        else  if(month>=7&&month<=9){
//            jd="Q3";
//            return jd;
//        }
//        else{
//            jd="Q4";
//            return jd;
//        }
//    }
//
//
//    public static String fomaToDatas(String data){
//        DateFormat fmt=new SimpleDateFormat("yyyyMMdd");
//        try {
//            Date parse=fmt.parse(data);
//            DateFormat fmt2=new SimpleDateFormat("yyyy-MM-dd");
//            return fmt2.format(parse);
//        } catch (ParseException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//            return null;
//        }
//
//
//
//    }
//
//
//
//
//
//
//
//    /**
//     * 日期解析
//     *
//     * @param strDate
//     * @param pattern
//     * @return
//     */
//    public static Date parseDate(String strDate, String pattern) {
//        Date date = null;
//        try {
//            if (pattern == null) {
//                pattern = YYYYMMDD;
//            }
//            SimpleDateFormat format = new SimpleDateFormat(pattern);
//            date = format.parse(strDate);
//        } catch (Exception e) {
//
//        }
//        return date;
//    }
//    public static int getYear(String date) throws ParseException {
//        Calendar c = Calendar.getInstance();
//        c.setTime(parseDate(date));
//        int year = c.get(Calendar.YEAR);
//        return year;
//    }
//    public String getYearMonth (Date date) {
//        return formatDateByFormat(date, "yyyy-MM")  ;
//    }
//    /**
//     * 取得指定月份的第一天
//     *
//     * String
//     * @return String
//     */
//    public String getMonthBegin(Date date) {
//        return formatDateByFormat(date, "yyyy-MM") + "-01";
//    }
//
//    /**
//     * 取得指定月份的最后一天
//     *
//     * String
//     * @return String
//     */
//    public static String getMonthEnd(Date date) {
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTime(date);
//        calendar.add(Calendar.MONTH, 1);
//        calendar.set(Calendar.DAY_OF_MONTH, 1);
//        calendar.add(Calendar.DAY_OF_YEAR, -1);
//        return formatDateByFormat(calendar.getTime(), "yyyy-MM-dd");
//    }
//    /**
//     * 以指定的格式来格式化日期
//     *
//     * @param date
//     * Date
//     * @param format
//     * String
//     * @return String
//     */
//    public static String formatDateByFormat(Date date, String format) {
//        String result = "";
//        if (date != null) {
//            try {
//                SimpleDateFormat sdf = new SimpleDateFormat(format);
//                result = sdf.format(date);
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        }
//        return result;
//    }
//    /**
//     *
//     * @param strDate
//     * @return
//     */
//    public static Date parseDate(String strDate) {
//        return parseDate(strDate, null);
//    }
//
//
//    /**
//     * format date
//     *
//     * @param date
//     * @return
//     */
//    public static String formatDate(Date date) {
//        return formatDate(date, null);
//    }
//
//    /**
//     * format date
//     *
//     * @param date
//     * @param pattern
//     * @return
//     */
//    public static String formatDate(Date date, String pattern) {
//        String strDate = null;
//        try {
//            if (pattern == null) {
//                pattern = YYYYMMDD;
//            }
//            SimpleDateFormat format = new SimpleDateFormat(pattern);
//            strDate = format.format(date);
//        } catch (Exception e) {
//
//        }
//        return strDate;
//    }
//
//    /**
//     * 取得日期：年
//     *
//     * @param date
//     * @return
//     */
//    public static int getYear(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        int year = c.get(Calendar.YEAR);
//        return year;
//    }
//
//    /**
//     * 取得日期：年
//     *
//     * @param date
//     * @return
//     */
//    public static int getMonth(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        int month = c.get(Calendar.MONTH);
//        return month + 1;
//    }
//
//    /**
//     * 取得日期：年
//     *
//     * @param date
//     * @return
//     */
//    public static int getDay(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        int da = c.get(Calendar.DAY_OF_MONTH);
//        return da;
//    }
//
//    /**
//     * 取得当天日期是周几
//     *
//     * @param date
//     * @return
//     */
//    public static int getWeekDay(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        int week_of_year = c.get(Calendar.DAY_OF_WEEK);
//        return week_of_year - 1;
//    }
//
//    /**
//     * 取得一年的第几周
//     *
//     * @param date
//     * @return
//     */
//    public static int getWeekOfYear(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        int week_of_year = c.get(Calendar.WEEK_OF_YEAR);
//        return week_of_year;
//    }
//
//    /**
//     * getWeekBeginAndEndDate
//     *
//     * @param date
//     * @param pattern
//     * @return
//     */
//    public static String getWeekBeginAndEndDate(Date date, String pattern) {
//        Date monday = getMondayOfWeek(date);
//        Date sunday = getSundayOfWeek(date);
//        return formatDate(monday, pattern) + " - "
//                + formatDate(sunday, pattern);
//    }
//
//    /**
//     * 根据日期取得对应周周一日期
//     *
//     * @param date
//     * @return
//     */
//    public static Date getMondayOfWeek(Date date) {
//        Calendar monday = Calendar.getInstance();
//        monday.setTime(date);
//        monday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
//        monday.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
//        return monday.getTime();
//    }
//
//    /**
//     * 根据日期取得对应周周日日期
//     *
//     * @param date
//     * @return
//     */
//    public static Date getSundayOfWeek(Date date) {
//        Calendar sunday = Calendar.getInstance();
//        sunday.setTime(date);
//        sunday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
//        sunday.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
//        return sunday.getTime();
//    }
//
//    /**
//     * 取得月的剩余天数
//     *
//     * @param date
//     * @return
//     */
//    public static int getRemainDayOfMonth(Date date) {
//        int dayOfMonth = getDayOfMonth(date);
//        int day = getPassDayOfMonth(date);
//        return dayOfMonth - day;
//    }
//
//    /**
//     * 取得月已经过的天数
//     *
//     * @param date
//     * @return
//     */
//    public static int getPassDayOfMonth(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        return c.get(Calendar.DAY_OF_MONTH);
//    }
//
//    /**
//     * 取得月天数
//     *
//     * @param date
//     * @return
//     */
//    public static int getDayOfMonth(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        return c.getActualMaximum(Calendar.DAY_OF_MONTH);
//    }
//
//    /**
//     * 取得月第一天
//     *
//     * @param date
//     * @return
//     */
//    public static Date getFirstDateOfMonth(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        c.set(Calendar.DAY_OF_MONTH, c.getActualMinimum(Calendar.DAY_OF_MONTH));
//        return c.getTime();
//    }
//
//    /**
//     * 取得月最后一天
//     *
//     * @param date
//     * @return
//     */
//    public static Date getLastDateOfMonth(Date date) {
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
//        return c.getTime();
//    }
//
//    /**
//     * 取得季度第一天
//     *
//     * @param date
//     * @return
//     */
//    public static Date getFirstDateOfSeason(Date date) {
//        return getFirstDateOfMonth(getSeasonDate(date)[0]);
//    }
//
//    /**
//     * 取得季度最后一天
//     *
//     * @param date
//     * @return
//     */
//    public static Date getLastDateOfSeason(Date date) {
//        return getLastDateOfMonth(getSeasonDate(date)[2]);
//    }
//
//    /**
//     * 取得季度天数
//     *
//     * @param date
//     * @return
//     */
//    public static int getDayOfSeason(Date date) {
//        int day = 0;
//        Date[] seasonDates = getSeasonDate(date);
//        for (Date date2 : seasonDates) {
//            day += getDayOfMonth(date2);
//        }
//        return day;
//    }
//
//    /**
//     * 取得季度剩余天数
//     *
//     * @param date
//     * @return
//     */
//    public static int getRemainDayOfSeason(Date date) {
//        return getDayOfSeason(date) - getPassDayOfSeason(date);
//    }
//
//    /**
//     * 取得季度已过天数
//     *
//     * @param date
//     * @return
//     */
//    public static int getPassDayOfSeason(Date date) {
//        int day = 0;
//
//        Date[] seasonDates = getSeasonDate(date);
//
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        int month = c.get(Calendar.MONTH);
//
//        if (month == Calendar.JANUARY || month == Calendar.APRIL
//                || month == Calendar.JULY || month == Calendar.OCTOBER) {// 季度第一个月
//            day = getPassDayOfMonth(seasonDates[0]);
//        } else if (month == Calendar.FEBRUARY || month == Calendar.MAY
//                || month == Calendar.AUGUST || month == Calendar.NOVEMBER) {// 季度第二个月
//            day = getDayOfMonth(seasonDates[0])
//                    + getPassDayOfMonth(seasonDates[1]);
//        } else if (month == Calendar.MARCH || month == Calendar.JUNE
//                || month == Calendar.SEPTEMBER || month == Calendar.DECEMBER) {// 季度第三个月
//            day = getDayOfMonth(seasonDates[0]) + getDayOfMonth(seasonDates[1])
//                    + getPassDayOfMonth(seasonDates[2]);
//        }
//        return day;
//    }
//
//    /**
//     * 取得季度月
//     *
//     * @param date
//     * @return
//     */
//    public static Date[] getSeasonDate(Date date) {
//        Date[] season = new Date[3];
//
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//
//        int nSeason = getSeason(date);
//        if (nSeason == 1) {// 第一季度
//            c.set(Calendar.MONTH, Calendar.JANUARY);
//            season[0] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.FEBRUARY);
//            season[1] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.MARCH);
//            season[2] = c.getTime();
//        } else if (nSeason == 2) {// 第二季度
//            c.set(Calendar.MONTH, Calendar.APRIL);
//            season[0] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.MAY);
//            season[1] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.JUNE);
//            season[2] = c.getTime();
//        } else if (nSeason == 3) {// 第三季度
//            c.set(Calendar.MONTH, Calendar.JULY);
//            season[0] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.AUGUST);
//            season[1] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.SEPTEMBER);
//            season[2] = c.getTime();
//        } else if (nSeason == 4) {// 第四季度
//            c.set(Calendar.MONTH, Calendar.OCTOBER);
//            season[0] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.NOVEMBER);
//            season[1] = c.getTime();
//            c.set(Calendar.MONTH, Calendar.DECEMBER);
//            season[2] = c.getTime();
//        }
//        return season;
//    }
//
//    /**
//     *
//     * 1 第一季度 2 第二季度 3 第三季度 4 第四季度
//     *
//     * @param date
//     * @return
//     */
//    public static int getSeason(Date date) {
//
//        int season = 0;
//
//        Calendar c = Calendar.getInstance();
//        c.setTime(date);
//        int month = c.get(Calendar.MONTH);
//        switch (month) {
//            case Calendar.JANUARY:
//            case Calendar.FEBRUARY:
//            case Calendar.MARCH:
//                season = 1;
//                break;
//            case Calendar.APRIL:
//            case Calendar.MAY:
//            case Calendar.JUNE:
//                season = 2;
//                break;
//            case Calendar.JULY:
//            case Calendar.AUGUST:
//            case Calendar.SEPTEMBER:
//                season = 3;
//                break;
//            case Calendar.OCTOBER:
//            case Calendar.NOVEMBER:
//            case Calendar.DECEMBER:
//                season = 4;
//                break;
//            default:
//                break;
//        }
//        return season;
//    }
//
//}
