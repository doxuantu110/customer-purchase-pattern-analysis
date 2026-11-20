package com.retail;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimePatternMapperTest {

    @Test
    public void tuesdayMidnight() {
        // 2019-10-01 là thứ Ba
        assertEquals("2\t00", TimePatternMapper.timeKey("2019-10-01 00:02:14"));
    }

    @Test
    public void sundayEvening() {
        // 2019-10-06 là Chủ nhật
        assertEquals("7\t23", TimePatternMapper.timeKey("2019-10-06 23:59:16"));
    }

    @Test(expected = java.time.format.DateTimeParseException.class)
    public void badDateThrows() {
        TimePatternMapper.timeKey("not-a-date at all");
    }
}
