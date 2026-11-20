package com.retail;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class CustomerStatsMapperTest {

    private static final String PURCHASE =
            "2019-10-01 00:00:04,purchase,555,1001,2053,electronics.smartphone,apple,100.50,sess1";

    @Test
    public void validPurchaseKeyedByUser() {
        String[] r = CustomerStatsMapper.parseLine(PURCHASE, 2);
        assertArrayEquals(new String[] {"555", "100.5,1,2019-10-01 00:00:04,2019-10-01 00:00:04"}, r);
    }

    @Test
    public void validPurchaseKeyedByProduct() {
        String[] r = CustomerStatsMapper.parseLine(PURCHASE, 3);
        assertEquals("1001", r[0]);
    }

    @Test
    public void nonPurchaseIsSkipped() {
        String[] r = CustomerStatsMapper.parseLine(PURCHASE.replace("purchase", "view"), 2);
        assertEquals(0, r.length);
    }

    @Test
    public void wrongColumnCountIsInvalid() {
        assertNull(CustomerStatsMapper.parseLine(PURCHASE + ",extra", 2));
        assertNull(CustomerStatsMapper.parseLine("a,b,c", 2));
    }

    @Test
    public void badOrNonPositivePriceIsInvalid() {
        assertNull(CustomerStatsMapper.parseLine(PURCHASE.replace("100.50", "abc"), 2));
        assertNull(CustomerStatsMapper.parseLine(PURCHASE.replace("100.50", "0.00"), 2));
        assertNull(CustomerStatsMapper.parseLine(PURCHASE.replace("100.50", "-5"), 2));
    }

    @Test
    public void emptyUserIsInvalid() {
        assertNull(CustomerStatsMapper.parseLine(PURCHASE.replace(",555,", ",,"), 2));
    }
}
