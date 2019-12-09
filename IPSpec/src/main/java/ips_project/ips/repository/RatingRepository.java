package ips_project.ips.repository;
import ips_project.ips.model.Rating;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;

import java.util.Collection;
import java.util.List;

public interface RatingRepository extends Neo4jRepository<Rating, Long> {


    @Query("MATCH (n:Rating) RETURN n")
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
