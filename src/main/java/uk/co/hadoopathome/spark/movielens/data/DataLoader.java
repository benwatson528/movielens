package uk.co.hadoopathome.spark.movielens.data;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class DataLoader {
  private final SparkSession sparkSession;
  private final String baseDataDir;

  public DataLoader(String baseDataDir, SparkSession sparkSession) {
    this.baseDataDir = baseDataDir;
    this.sparkSession = sparkSession;
  }

  public Dataset<Row> createRatingsDataset() {
    JavaRDD<String> stringJavaRDD =
        this.sparkSession.sparkContext().textFile(this.baseDataDir + "ratings.dat", 1).toJavaRDD();
    JavaRDD<Row> splitRowRDD =
        stringJavaRDD.map(
            (Function<String, Row>)
                row -> {
                  String[] split = row.split("::");
                  return RowFactory.create(
                      Integer.parseInt(split[0]),
                      Integer.parseInt(split[1]),
                      Integer.parseInt(split[2]),
                      Long.parseLong(split[3]));
                });

    return this.sparkSession.sqlContext().createDataFrame(splitRowRDD, Schemas.getRatingsSchema());
  }

  public Dataset<Row> createMoviesDataset() {
    JavaRDD<String> stringJavaRDD =
        this.sparkSession.sparkContext().textFile(this.baseDataDir + "movies.dat", 1).toJavaRDD();
    JavaRDD<Row> splitRowRDD =
        stringJavaRDD.map(
            (Function<String, Row>)
                row -> {
                  String[] split = row.split("::");
                  return RowFactory.create(Integer.parseInt(split[0]), split[1], split[2]);
                });

    return this.sparkSession.sqlContext().createDataFrame(splitRowRDD, Schemas.getMoviesSchema());
  }

  public Dataset<Row> createUsersDataset() {
    JavaRDD<String> stringJavaRDD =
        this.sparkSession.sparkContext().textFile(this.baseDataDir + "users.dat", 1).toJavaRDD();
    JavaRDD<Row> splitRowRDD =
        stringJavaRDD.map(
            (Function<String, Row>)
                row -> {
                  String[] split = row.split("::");
                  return RowFactory.create(
                      Integer.parseInt(split[0]),
                      split[1],
                      Integer.parseInt(split[2]),
                      Integer.parseInt(split[3]),
                      split[4]);
                });

    return this.sparkSession.sqlContext().createDataFrame(splitRowRDD, Schemas.getUsersSchema());
  }
}
