package com.eki.util;

import java.util.Random;

public class NormalDistributionGenerator {
    public static double getNormalDirtribution (double mean, double std) {
        Random random = new Random();
        return mean + random.nextGaussian() * std;
    }
}
