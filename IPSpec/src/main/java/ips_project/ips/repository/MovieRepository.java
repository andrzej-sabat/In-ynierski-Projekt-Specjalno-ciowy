package ips_project.ips.repository;

import ips_project.ips.model.Movie;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;

import java.util.Collection;

public interface MovieRepository extends Neo4jRepository<Movie, Integer> {


    @Query("LOAD CSV WITH HEADERS FROM 'file:///ratings.csv' AS line\n" +
            "CREATE (:Rating { userId: toInteger(line.UserId), movieId: toInteger(line.MovieId), rate: toInteger(line.Rate), timestamp: toInteger( line.Timestamp)})")
    void loadMoviesCsv();

    @Query("LOAD CSV WITH HEADERS FROM 'file:///movies.csv' AS line\n" +
            "CREATE (:Movie {movieId: toInteger(line.MovieId), title: line.Title, genres: line.Genres})")
    void loadRatingCsv();

    @Query("MATCH (n:Movie)\n" +
            "WHERE ID(n) = {0}\n" +
            "RETURN n")
    Movie findById(Long id);















    @Query("MATCH (n:Movie) RETURN n")
    Collection<Movie> getAllMovies();

    Movie findByMovieId(Long movieId);



}
