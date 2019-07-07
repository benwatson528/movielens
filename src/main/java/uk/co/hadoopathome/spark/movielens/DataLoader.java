package uk.co.hadoopathome.spark.movielens;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

class DataLoader {
  private final SparkSession sparkSession;
  private final String baseDataDir;

  DataLoader(String baseDataDir, SparkSession sparkSession) {
    this.baseDataDir = baseDataDir;
    this.sparkSession = sparkSession;
  }

  Dataset<Row> createRatingsDataset() {
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

    StructType structType =
        new StructType()
            .add("userID", DataTypes.IntegerType)
            .add("movieID", DataTypes.IntegerType)
            .add("rating", DataTypes.IntegerType)
            .add("timestamp", DataTypes.LongType);

    return this.sparkSession.sqlContext().createDataFrame(splitRowRDD, structType);
  }

  Dataset<Row> createMoviesDataset() {
    JavaRDD<String> stringJavaRDD =
        this.sparkSession.sparkContext().textFile(this.baseDataDir + "movies.dat", 1).toJavaRDD();
    JavaRDD<Row> splitRowRDD =
        stringJavaRDD.map(
            (Function<String, Row>)
                row -> {
                  String[] split = row.split("::");
                  return RowFactory.create(Integer.parseInt(split[0]), split[1], split[2]);
                });

    StructType structType =
        new StructType()
            .add("movieID", DataTypes.IntegerType)
            .add("title", DataTypes.StringType)
            .add("genre", DataTypes.StringType);

    return this.sparkSession.sqlContext().createDataFrame(splitRowRDD, structType);
  }

  Dataset<Row> createUsersDataset() {
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

    StructType structType =
        new StructType()
            .add("userID", DataTypes.IntegerType)
            .add("gender", DataTypes.StringType)
            .add("age", DataTypes.IntegerType)
            .add("occupation", DataTypes.IntegerType)
            .add("zipCode", DataTypes.StringType);

    return this.sparkSession.sqlContext().createDataFrame(splitRowRDD, structType);
  }
}
