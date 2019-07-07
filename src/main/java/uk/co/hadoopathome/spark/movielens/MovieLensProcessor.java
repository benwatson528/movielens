package uk.co.hadoopathome.spark.movielens;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import scala.Function1;
import scala.collection.TraversableOnce;

import java.util.Iterator;

class MovieLensProcessor {
  private static final String BASE_DATA_DIR = "src/main/resources/data/";
  public static final String GENRE_SPLIT = "|";
  private final Dataset<Row> moviesDS;
  private final Dataset<Row> ratingsDS;
  private final Dataset<Row> usersDS;

  MovieLensProcessor(SparkSession sparkSession) {
    DataLoader dataLoader = new DataLoader(BASE_DATA_DIR, sparkSession);
    this.moviesDS = dataLoader.createMoviesDataset();
    this.ratingsDS = dataLoader.createRatingsDataset();
    this.usersDS = dataLoader.createUsersDataset();
  }

  void calculateAvgRatingPerUser() {
    Dataset<Row> avgRatingPerUser =
        this.usersDS
            .join(this.ratingsDS, "userID")
            .groupBy("userID")
            .agg(
                functions.count("rating").as("numRatings"),
                functions.bround(functions.avg("rating"), 2).as("avgRating"));

    avgRatingPerUser
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .csv("build/output/avg-rating-per-user");

    avgRatingPerUser.show(false);
  }

  void calculateMoviesPerGenre() {
    Dataset<Row> splitGenresMoviesDS =
        this.moviesDS.withColumn(
            "genre", functions.explode(functions.split(functions.col("genre"), GENRE_SPLIT)));
    Dataset<Row> moviesPerGenre = splitGenresMoviesDS.groupBy("genre").count();

    moviesPerGenre
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .csv("build/output/movies-per-genre");

    moviesPerGenre.show(false);
  }
}
