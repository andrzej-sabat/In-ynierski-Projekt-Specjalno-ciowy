package ips_project.ips.service;

import ips_project.ips.repository.MovieRepository;
import ips_project.ips.model.Movie;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;

@Service
public class MovieService {

    @Autowired
    MovieRepository movieRepository;

    public Collection<Movie> getAll() {
        return movieRepository.getAllMovies();
    }

    public void addMovie(Movie Movie) {
        movieRepository.save(Movie);
    }


    public Movie getMovie(Long id) {
        return movieRepository.findById(id);
    }


    public Movie updateMovie(Long id,Movie Movie) {
        return movieRepository.save(Movie);
    }


    public void deleteMovie(Long id) {
        movieRepository.deleteById(id);
    }
}
