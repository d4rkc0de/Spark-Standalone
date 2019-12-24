import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception {
        File workaround = new File(".");
        System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
        new File("./bin").mkdirs();
        new File("./bin/winutils.exe").createNewFile();

        SparkSession session = SparkSession.builder().appName("spark-standalone").config("spark.master", "local")
                .getOrCreate();

        Spark spark = new Spark();
        spark.run(session);
    }

}
