import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Spark {

    void run(SparkSession session) {

        String path = "src/test/resources/test.csv";
        String path2 = "src/test/resources/test2.csv";
        Dataset<Row> ds = loadDataset(session, path);
        ds.show();

        Dataset<Row> ds2 = loadDataset(session, path2);
        ds2.show();

        // {"inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti"}
        ds.join(ds2, ds.col("code").equalTo(ds2.col("code")), "inner").show();
        ds.join(ds2, ds.col("code").equalTo(ds2.col("code")), "left_semi").show();
    }

    private Dataset<Row> loadDataset(SparkSession session, String path) {
        Dataset<Row> tmp = session.read().option("header", "true").text(path);
        String header = tmp.first().mkString();
        String separateur = Utils.getSeparateur(header);
        return session.read().option("header", "true").option("quote", "\"").option("delimiter", separateur).csv(path);
    }
}
