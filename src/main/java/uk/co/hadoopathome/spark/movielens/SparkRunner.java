package uk.co.hadoopathome.spark.movielens;

import org.apache.spark.sql.SparkSession;

class SparkRunner {
  private SparkRunner() {}

  static SparkSession createSparkSession() {
    return SparkSession.builder().appName("MovieLens Analysis").master("local[*]").getOrCreate();
  }
}
