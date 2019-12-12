package ips_project.ips.repository;
import ips_project.ips.model.Rating;
import org.neo4j.driver.v1.*;
import org.neo4j.ogm.session.Neo4jSession;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.Collection;
import java.util.List;

public interface RatingRepository extends Neo4jRepository<Rating, Long> {

    @Query("CALL apoc.periodic.iterate('\n" +
            "CALL apoc.load.csv(\"file:///ck_ratings.csv\") yield map as line\n" +
            "','\n" +
            "CREATE (p:Rating {userId: toInteger(line.UserId), movieId: toInteger(line.MovieId), timestamp:  line.Timestamp, rate: toFloat(line.Rate)}) SET p.id = line.id\n" +
            "RETURN p\n" +
            "', {batchSize:10000, iterateList:true, parallel:true});")
    void loadRatingCsvFromClickHouse();

    @Query("MATCH (n:Rating) RETURN n limit 1000")
    Collection<Rating> getAllRatings();

    @Query("MATCH (n:Rating) WHERE ID(n) = {0} RETURN n")
    Rating findById(Long id);

    // QUERY 1
    @Query("MATCH (n:Rating) WHERE n.rate > {0} RETURN n")
    List<Rating> findByRate(Integer rate);

    // QUERY 2
    @Query("MATCH (n:Rating) WHERE n.rate >= {0} RETURN n.movieId,COUNT(n) ORDER BY COUNT(n)")
    List<Rating> findByRateAndCount(Integer rate);

    // QUERY 3
    @Query("MATCH (n:Rating) RETURN n.movieId,SUM(n.rate) ORDER BY SUM(n.rate)")
    List<Rating> sumRates();




}

