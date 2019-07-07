package uk.co.hadoopathome.spark.movielens.data;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Schemas {
  static StructType getRatingsSchema() {
    return new StructType()
        .add("userID", DataTypes.IntegerType)
        .add("movieID", DataTypes.IntegerType)
        .add("rating", DataTypes.IntegerType)
        .add("timestamp", DataTypes.LongType);
  }

  public static StructType getMoviesSchema() {
    return new StructType()
        .add("movieID", DataTypes.IntegerType)
        .add("title", DataTypes.StringType)
        .add("genre", DataTypes.StringType);
  }

  static StructType getUsersSchema() {
    return new StructType()
        .add("userID", DataTypes.IntegerType)
        .add("gender", DataTypes.StringType)
        .add("age", DataTypes.IntegerType)
        .add("occupation", DataTypes.IntegerType)
        .add("zipCode", DataTypes.StringType);
  }
}
