package com.retail;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class StatsAccumulatorTest {

    private static final String V1 = "10.5,1,2019-10-05 10:00:00,2019-10-05 10:00:00";
    private static final String V2 = "20.0,1,2019-10-01 08:00:00,2019-10-01 08:00:00";
    private static final String V3 = "29.5,2,2019-10-03 09:00:00,2019-10-09 09:00:00";

    @Test
    public void sumCountMinMax() {
        StatsAccumulator a = new StatsAccumulator();
        a.add(V1);
        a.add(V2);
        a.add(V3);
        assertEquals(60.0, a.getSum(), 1e-9);
        assertEquals(4, a.getCount());
        assertEquals("2019-10-01 08:00:00", a.getMin());
        assertEquals("2019-10-09 09:00:00", a.getMax());
    }

    @Test
    public void finalValueFormat() {
        StatsAccumulator a = new StatsAccumulator();
        a.add(V1);
        a.add(V2);
        a.add(V3);
        assertEquals("60.00\t4\t15.00\t2019-10-01 08:00:00\t2019-10-09 09:00:00", a.toFinalValue());
    }

    @Test
    public void combinerThenReducerEqualsDirect() {
        StatsAccumulator direct = new StatsAccumulator();
        direct.add(V1);
        direct.add(V2);
        direct.add(V3);

        StatsAccumulator c1 = new StatsAccumulator();
        c1.add(V1);
        c1.add(V2);
        StatsAccumulator c2 = new StatsAccumulator();
        c2.add(V3);

        StatsAccumulator reducer = new StatsAccumulator();
        reducer.add(c1.toPartialValue());
        reducer.add(c2.toPartialValue());

        assertEquals(direct.toFinalValue(), reducer.toFinalValue());
    }
}
