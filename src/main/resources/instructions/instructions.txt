Spark Test
===========
Use the MovieLens 1 Million Dataset for this exercise. It can be downloaded from http://grouplens.org/datasets/movielens/.
Dataset is available under http://files.grouplens.org/datasets/movielens/ml-1m.zip.
Please refer to the Read Me file at http://files.grouplens.org/datasets/movielens/ml-1m-README.txt for details about the data.

Use Spark and Scala with SBT(or Maven) to complete the following test. Your code should be in production quality.

1. Define the project folder structure and dependencies using either SBT or Maven. Add Apache Spark dependency to the project.
2. Write Spark code to generate the following outputs in the target directory inside the project.
A. A CSV file containing list of users with no of movies they rated and average rating per user.
   CSV file should contain 3 columns, ie: UserId, No of Movies, Average Rating. Column headers are not required. Use RDDs for this tasks.(No datasets, No data frames)
B. A CSV file containing list of unique Genres and no of movies under each genres
    CSV file should contain 2 columns, ie: Genres, No of Movies. Column headers are not required. Use RDDs for this tasks.(No datasets, No data frames)
C. Generate a parquet file that contain the top 100 movies based on their ratings. This should have fields, Rank (1-100), Movie Id, Title, Average Rating. Rank 1 is the most popular movie.
3. Write unit tests for part 2.B.
4. Write a simple Read Me file that contains instructions on how to run each part in 2.
