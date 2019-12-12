package ips_project.ips.service;

import ips_project.ips.repository.MovieRepository;
import ips_project.ips.model.Movie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;

@Service
public class MovieService {
    private final static Logger LOG = LoggerFactory.getLogger(MovieService.class);


    private final MovieRepository movieRepository;

    public MovieService(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    @Transactional
    public Collection<Movie> findAllMovies() {
        return movieRepository.getAllMovies();
    }

    public Movie findById(Long id) { return movieRepository.findById(id);}
    public Movie findByMovieId(Long movieId) {
        return movieRepository.findByMovieId(movieId);}

    public void deleteMovie(Movie movie){
         movieRepository.delete(movie);
    }


    @Transactional(readOnly = true)
    public void loadData() { movieRepository.loadMoviesCsv(); movieRepository.loadRatingCsv();}

    public void loadMoviesCsvFromClickHouse() { movieRepository.loadMoviesCsvFromClickHouse();}
}
