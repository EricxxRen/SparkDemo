package com.eki;

import com.eki.util.NormalDistributionGenerator;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * 生成身高的csv文件
 */
public class InfoFileGenerator {

    public static void main(String[] args) {
        File file = new File ("/home/xiaoxing/IdeaProjects/HightStat/info.csv");

        try {
            Random random = new Random();
            FileWriter fileWriter = new FileWriter(file);

            for (int i = 1; i <= 10000; i++) {
                String gender = getGender();

                if ("M".equals(gender)) {
                    double hight = NormalDistributionGenerator.getNormalDirtribution(180D,10D);
                    fileWriter.write(i + "," + gender + "," + (int) hight);
                    fileWriter.write(System.getProperty("line.separator"));
                } else {
                    double hight = NormalDistributionGenerator.getNormalDirtribution(160D,10D);
                    fileWriter.write(i + "," + gender + "," + (int) hight);
                    fileWriter.write(System.getProperty("line.separator"));
                }

            }
            fileWriter.flush();
            fileWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getGender () {
        Random random = new Random();
        int rNum = random.nextInt(2) + 1;
        if (rNum % 2 == 0) {
            return "M";
        } else {
            return "F";
        }
    }
}
