package uk.co.hadoopathome.spark.movielens;

import org.apache.spark.sql.SparkSession;

public class MovieLensMain {
  public static void main(String[] args) {
    SparkSession sparkSession = SparkRunner.createSparkSession();
    MovieLensProcessor movieLensProcessor = new MovieLensProcessor(sparkSession);
    movieLensProcessor.calculateAvgRatingPerUser();
    movieLensProcessor.calculateMoviesPerGenre();
    movieLensProcessor.calculateHighestRatedMovies();
  }
}
