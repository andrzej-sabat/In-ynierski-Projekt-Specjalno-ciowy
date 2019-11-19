package ips_project.ips.repository;

import ips_project.ips.model.Movie;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;

import java.util.Collection;

public interface MovieRepository extends Neo4jRepository<Movie, Long> {

    @Query("MATCH (n:Movie) RETURN n limit 100")
    Collection<Movie> getAllMovies();

    Movie findById(Long id);

    Movie deleteById(Long id);
}
