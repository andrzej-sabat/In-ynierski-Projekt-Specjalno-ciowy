package ips_project.ips.service;

import ips_project.ips.repository.MovieRepository;
import ips_project.ips.model.Movie;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;

@Service
public class MovieService {

    @Autowired
    MovieRepository movieRepository;

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


    @Transactional
    public void loadData() { movieRepository.loadMoviesCsv(); movieRepository.loadRatingCsv();}
}
