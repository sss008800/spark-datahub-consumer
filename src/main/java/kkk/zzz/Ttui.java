package kkk.zzz;

import org.apache.spark.sql.SparkSession;


/**
 * @创建人 dingxl
 * @创建时间 2024/1/17
 * @描述  main入口
 */
public class Ttui {

    public static void main(String[] args) {
        for (int i = 0; i < 3; i++) {
            try {
                Thread.sleep(3000);
                System.out.println("sleep ........................");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {

            }
        }

        DatahubJavaDemo.getDStream();
        sparkSave.stop();
    }
}
