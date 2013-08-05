package com.fujitsu.ca.fic.caissepop.util;

import java.math.BigDecimal;

public class Util {
    public static double round(double unrounded, int precision) {
        BigDecimal bd = new BigDecimal(unrounded);
        BigDecimal rounded = bd.setScale(precision, BigDecimal.ROUND_HALF_EVEN);
        return rounded.doubleValue();
    }
}
