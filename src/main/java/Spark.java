import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class Spark {
    long n = 0;

    void run(SparkSession session) {
        q_75496490(session);
    }

    private void test(SparkSession session) {
        session.sqlContext().udf().register("seqUdf", o -> (1 + (long) Math.sqrt(1 + (8 * n++))) / 2, DataTypes.LongType);
        List<String> data = Arrays.asList("abc", "klm", "xyz", "abc", "klm", "xyz", "abc", "klm", "xyz", "abc", "klm", "xyz");
        Dataset<String> dataDs = session.createDataset(data, Encoders.STRING());
        Dataset<Row> results = dataDs.withColumn("newColumn",
                functions.callUDF("seqUdf", dataDs.col("value")));
        results.show(false);
    }

    private void test1(SparkSession session) {
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

    private void q_75496490(SparkSession spark) {
        List<String> stringList = Arrays.asList("foo", "bar", "baz");


// Create a Dataset from the list of strings
        Dataset<String> ds = spark.createDataset(stringList, Encoders.STRING());

// Convert the Dataset of strings to a Dataset of arrays of strings

// Print the contents of the Dataset
        ds.show();
        List<Integer> salt_array = Arrays.asList(0, 1, 2);

        ds.withColumn(
                "salt_array",
                functions.array(salt_array.stream().map(functions::lit).toArray(Column[]::new))
        ).show();

    }

    private void q_75091725(SparkSession session) {
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        List<Person> personList = Arrays.asList(
                new Person("John", "Doe", 25),
                new Person("Jane", "Doe", 30)
        );
        Dataset<Person> personListDs = session.createDataset(personList, personEncoder);

        int age = 12;
        Broadcast<Integer> broadcastAge = new JavaSparkContext(session.sparkContext()).broadcast(age);

        personListDs.show();
        Dataset<Person> updatedPersonListDs = personListDs.map((MapFunction<Person, Person>) person -> {
            person.setAge(broadcastAge.value());
            return person;
        }, personEncoder);

        updatedPersonListDs.show();
    }

    private Dataset<Row> loadDataset(SparkSession session, String path) {
        Dataset<Row> tmp = session.read().option("header", "true").text(path);
        String header = tmp.first().mkString();
        String separateur = Utils.getSeparateur(header);
        return session.read().option("header", "true").option("quote", "\"").option("delimiter", separateur).csv(path);
    }


    class UpdateAge implements MapFunction<Person, Person> {
        public Person call(Person person) {
            person.setAge(30);
            return person;
        }
    }
}