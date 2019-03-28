package com.eki;

import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

public class AvgCount implements Serializable {
    public int total;
    public int num;

    public AvgCount (int total, int num) {
        this.total = total;
        this.num = num;
    }

    public double avg () {
        return total / (double) num;
    }

}
