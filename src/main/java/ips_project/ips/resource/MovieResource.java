package ips_project.ips.resource;

import ips_project.ips.model.Movie;
import ips_project.ips.service.MovieService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

@RestController
public class MovieResource {

    @Autowired
    MovieService movieService;

    /*@GetMapping
    public Collection<Movie> getAll() {
        return movieService.getAll();
    }*/

    @RequestMapping(value="/moviesList" , method = RequestMethod.GET)
    public ModelAndView MoviesList(ModelAndView model) throws IOException {
        List<Movie> moviesList = (List<Movie>) movieService.getAll();
        model.addObject("moviesList", moviesList);
        model.setViewName("/movies");

        return model;
    }

}
