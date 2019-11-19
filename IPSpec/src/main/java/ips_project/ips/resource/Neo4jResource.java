package ips_project.ips.resource;

import ips_project.ips.model.Movie;
import ips_project.ips.repository.MovieRepository;
import ips_project.ips.service.MovieService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

@RestController
public class Neo4jResource {

    @Autowired
    MovieService movieService;
    @Autowired
    MovieRepository movieRepository;

   /* @GetMapping(value="/lista")
    public Collection<Movie> getAll() {
        return movieService.getAll();
    }*/

    @RequestMapping(value="/neo4j_moviesList" , method = RequestMethod.GET)
    public ModelAndView neo4j_moviesList(ModelAndView model) throws IOException {
        List<Movie> moviesList = (List<Movie>) movieService.getAll();
        model.addObject("moviesList", moviesList);
        model.setViewName("/neo4j_moviesList");
        return model;
    }

    @RequestMapping(value="/neo4j_addMovie", method = RequestMethod.GET)
    public ModelAndView neo4j_addMovie(){
        ModelAndView modelAndView = new ModelAndView();
        Movie movie = new Movie();
        modelAndView.addObject("movie",movie);
        modelAndView.setViewName("/neo4j_addMovie");
        return modelAndView;
    }

    @PostMapping("/neo4j_saveMovie")
    public ModelAndView neo4j_saveMovie(@ModelAttribute Movie movie) {

        movieRepository.save(movie);
        ModelAndView modelAndView = new ModelAndView();
        List<Movie> moviesList = (List<Movie>) movieService.getAll();
        modelAndView.addObject("moviesList", moviesList);
        modelAndView.setViewName("/neo4j_moviesList");
        modelAndView.addObject("movie",movie);
        return modelAndView;
    }

    @RequestMapping(value = "/neo4j_deleteMovie", method = RequestMethod.GET)
    public ModelAndView neo4j_deleteMovie(HttpServletRequest request) {
        int id = Integer.parseInt(request.getParameter("id"));
        movieService.deleteMovie((long) id);
        ModelAndView modelAndView = new ModelAndView();
        List<Movie> movieList = (List<Movie>) movieService.getAll();
        modelAndView.addObject("movieList", movieList);
        modelAndView.setViewName("/neo4j_moviesList");
        return modelAndView;
    }

}
