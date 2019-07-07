package uk.co.hadoopathome.spark.movielens;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;
import uk.co.hadoopathome.spark.movielens.data.DataLoader;
import uk.co.hadoopathome.spark.movielens.data.Schemas;

import java.util.ArrayList;
import java.util.List;

class MovieLensProcessor {
  public static final String GENRE_SPLIT = "\\|";
  private static final String BASE_DATA_DIR = "src/main/resources/data/";
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
                functions.bround(functions.avg("rating"), 2).as("avgRating")).persist();

    avgRatingPerUser
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .csv("build/output/avg-rating-per-user");

    avgRatingPerUser.show(false);
  }

  void calculateMoviesPerGenre() {
    Dataset<Row> splitGenresMoviesDS = flattenGenres(Schemas.getMoviesSchema());
    Dataset<Row> moviesPerGenre = splitGenresMoviesDS.groupBy("genre").count().persist();

    moviesPerGenre
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .csv("build/output/movies-per-genre");

    moviesPerGenre.show(false);
  }

  void calculateHighestRatedMovies() {
    Column over = functions.rank().over(Window.orderBy("avgRating"));

    Dataset<Row> ratedMovies =
        this.moviesDS
            .join(this.ratingsDS, "movieID")
            .groupBy("movieID")
            .avg("rating")
            .join(this.moviesDS, "movieID")
            .drop("genre")
            .limit(100)
            .orderBy(functions.desc("avg(rating)"))
            .withColumn("avgRating", functions.bround(functions.col("avg(rating)"), 2))
            .drop("avg(rating)")
            .coalesce(1)
            .withColumn("rank", over).persist();

    ratedMovies.write().mode(SaveMode.Overwrite).parquet("build/output/movies-per-genre");

    ratedMovies.show(false);
  }

  private Dataset<Row> flattenGenres(StructType structType) {
    return this.moviesDS.flatMap(
        (FlatMapFunction<Row, Row>)
            row -> {
              List<Row> rows = new ArrayList<>();
              String genres = row.getAs("genre");
              for (String splitGenre : genres.split(GENRE_SPLIT)) {
                rows.add(RowFactory.create(row.getAs("movieID"), row.getAs("title"), splitGenre));
              }
              return rows.iterator();
            },
        RowEncoder.apply(structType));
  }
}
