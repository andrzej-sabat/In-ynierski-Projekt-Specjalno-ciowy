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
    public Collection<Movie> getAll() {
        return movieRepository.getAllMovies();
    }

    @Transactional
    public void addMovie(Movie Movie) {
        movieRepository.save(Movie);
    }

    @Transactional
    public Movie getMovie(Long id) {
        return movieRepository.findById(id);
    }
    public Movie getMovieByTitle(String title) { return movieRepository.findMovieByTitle(title);}
    @Transactional
    public Movie updateMovie(Long movieId,Movie Movie) {
        return movieRepository.save(Movie);
    }

    @Transactional
    public void deleteMovie(Movie movie) {
        movieRepository.delete(movie);
    }
    public void deleteByTitle(String title) { movieRepository.deleteByTitle(title);}
}
