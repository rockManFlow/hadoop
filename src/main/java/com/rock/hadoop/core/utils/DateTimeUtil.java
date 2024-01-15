package com.rock.hadoop.core.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeUtil {
    public static final String UTC_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static LocalDateTime parseStrToUtcTime(String time){
        DateTimeFormatter df = DateTimeFormatter.ofPattern(UTC_PATTERN);
        return LocalDateTime.parse(time, df);
    }

    public static LocalDateTime parseStrToTime(String time, String format){
        DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
        return LocalDateTime.parse(time, df);
    }
}
