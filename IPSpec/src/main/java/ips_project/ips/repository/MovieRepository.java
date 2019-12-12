package ips_project.ips.repository;

import ips_project.ips.model.Movie;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;

public interface MovieRepository extends Neo4jRepository<Movie, Integer> {

    @Query("CALL apoc.periodic.iterate('\n" +
            "CALL apoc.load.csv(\"file:///ck_movies.csv\") yield map as line\n" +
            "','\n" +
            "CREATE (p:Movie {movieId: toInteger(line.MovieId), title: line.Title, genres: line.Genres}) SET p.id = line.id\n" +
            "RETURN p\n" +
            "', {batchSize:10000, iterateList:true, parallel:true});")
    void loadMoviesCsvFromClickHouse();

    @Query("CALL apoc.periodic.iterate('\n" +
            "CALL apoc.load.csv(\"file:///ratings.csv\") yield map as line\n" +
            "','\n" +
            "CREATE (p:Rating {userId: toInteger(line.UserId), movieId: toInteger(line.MovieId), timestamp:  line.Timestamp, rate: toFloat(line.Rate)}) SET p.id = line.id\n" +
            "RETURN p\n" +
            "', {batchSize:10000, iterateList:true, parallel:true});")
    void loadRatingCsv();

    @Query("CALL apoc.periodic.iterate('\n" +
            "CALL apoc.load.csv(\"file:///movies.csv\") yield map as line\n" +
            "','\n" +
            "CREATE (p:Movie {movieId: toInteger(line.MovieId), title: line.Title, genres: line.Genres}) SET p.id = line.id\n" +
            "RETURN p\n" +
            "', {batchSize:10000, iterateList:true, parallel:true});")
    void loadMoviesCsv();

    @Query("MATCH (n:Movie)\n" +
            "WHERE ID(n) = {0}\n" +
            "RETURN n")
    Movie findById(Long id);


    @Query("MATCH (n:Movie) RETURN n limit 1000")
    Collection<Movie> getAllMovies();

    Movie findByMovieId(Long movieId);



}
