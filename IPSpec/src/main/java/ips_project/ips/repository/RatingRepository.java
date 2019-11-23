package ips_project.ips.repository;
import ips_project.ips.model.Rating;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;

import java.util.Collection;

public interface RatingRepository extends Neo4jRepository<Rating, Long> {


    @Query("MATCH (n:Rate) RETURN n LIMIT 100")
    Collection<Rating> getAllRatings();


}
