package ips_project.ips.resource;

import ips_project.ips.model.Movie;
import ips_project.ips.model.Rating;
import ips_project.ips.repository.MovieRepository;
import ips_project.ips.repository.RatingRepository;
import ips_project.ips.service.MovieService;
import ips_project.ips.service.RatingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;


import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

@RestController
public class Neo4jResource {
    private long time;

    @Autowired
    MovieService movieService;
    @Autowired
    MovieRepository movieRepository;
    @Autowired
    RatingRepository ratingRepository;
    @Autowired
    RatingService ratingService;

    @RequestMapping(value = "/neo4j_loadData",method = RequestMethod.GET)
    public ModelAndView loadData(ModelAndView model) throws IOException {
        long startTime = System.nanoTime();
        movieService.loadData();
        long endTime = System.nanoTime();
        time = (endTime - startTime)/1000000;
        model.addObject("loadTime", time);
        model.setViewName("neo4j_loadDataResult");
        return model;

    }

    @RequestMapping(value = "/neo4j_queries",method = RequestMethod.GET)
    public ModelAndView queries(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/neo4j_queries");
        return modelAndView;
    }


    // MOVIES CONTOLLERS

    @RequestMapping(value="/neo4j_moviesList" , method = RequestMethod.GET)
    public ModelAndView neo4j_moviesList(ModelAndView model) throws IOException {
        List<Movie> moviesList = (List<Movie>) movieService.findAllMovies();
        model.addObject("moviesList", moviesList);
        model.setViewName("neo4j_moviesList");
        return model;
    }

    @RequestMapping(value="/neo4j_addMovie", method = RequestMethod.GET)
    public ModelAndView neo4j_addMovie(){
        ModelAndView modelAndView = new ModelAndView();
        Movie movie = new Movie();
        modelAndView.addObject("movie",movie);
        modelAndView.setViewName("neo4j_addMovie");
        return modelAndView;
    }

    @PostMapping("/neo4j_saveMovie")
    public ModelAndView neo4j_saveMovie(@ModelAttribute Movie movie) {

        movieRepository.save(movie);
        ModelAndView modelAndView = new ModelAndView();
        List<Movie> moviesList = (List<Movie>) movieService.findAllMovies();
        modelAndView.addObject("moviesList", moviesList);
        modelAndView.setViewName("neo4j_moviesList");
        modelAndView.addObject("movie",movie);
        return modelAndView;
    }

    @RequestMapping(value = "/neo4j_deleteMovie", method = RequestMethod.GET)
    public ModelAndView neo4j_deleteMovie(HttpServletRequest request) {
        Long movieId = Long.valueOf(request.getParameter("id"));
        Movie movie = movieService.findById(movieId);
        movieService.deleteMovie(movie);
        ModelAndView model = new ModelAndView();
        List<Movie> moviesList = (List<Movie>) movieService.findAllMovies();
        model.addObject("moviesList", moviesList);
        model.setViewName("neo4j_moviesList");
        return model;
    }

    @RequestMapping(value = "/neo4j_editMovie", method = RequestMethod.GET)
    public ModelAndView neo4j_editMovie(HttpServletRequest request) {
        Long movieId = Long.valueOf(request.getParameter("movieId"));
        Movie movie = movieService.findByMovieId(movieId);
        ModelAndView model = new ModelAndView("neo4j_addMovie");
        model.addObject("movie",movie);

        return model;
    }


    // RATING CONTROLLERS

    @RequestMapping(value="/neo4j_ratingsList" , method = RequestMethod.GET)
    public ModelAndView neo4j_ratingsList(ModelAndView model) throws IOException {
        List<Rating> ratingsList = (List<Rating>) ratingService.findAllRatings();
        model.addObject("ratingsList", ratingsList);
        model.setViewName("neo4j_ratingsList");
        return model;
    }

    @RequestMapping(value="/neo4j_addRating", method = RequestMethod.GET)
    public ModelAndView neo4j_addRating(){
        ModelAndView modelAndView = new ModelAndView();
        Rating rating = new Rating();
        modelAndView.addObject("rating",rating);
        modelAndView.setViewName("neo4j_addRating");
        return modelAndView;
    }

    @PostMapping("/neo4j_saveRating")
    public ModelAndView neo4j_saveRating(@ModelAttribute Rating rating) {
        ratingService.save(rating);
        ModelAndView modelAndView = new ModelAndView();
        List<Rating> ratingsList = (List<Rating>) ratingService.findAllRatings();
        modelAndView.addObject("ratingsList", ratingsList);
        modelAndView.setViewName("neo4j_ratingsList");
        modelAndView.addObject("rating",rating);
        return modelAndView;
    }

    @RequestMapping(value = "/neo4j_deleteRating", method = RequestMethod.GET)
    public ModelAndView neo4j_deleteRating(HttpServletRequest request) {
        Long id = Long.valueOf(request.getParameter("id"));
        Rating rating = ratingService.findById(id);
        ratingService.delete(rating);
        ModelAndView model = new ModelAndView();
        List<Rating> ratingsList = (List<Rating>) ratingService.findAllRatings();
        model.addObject("ratingsList", ratingsList);
        model.setViewName("neo4j_ratingsList");
        return model;
    }

    @RequestMapping(value = "/neo4j_editRating", method = RequestMethod.GET)
    public ModelAndView neo4j_editRating(HttpServletRequest request) {
        Long id = Long.valueOf(request.getParameter("id"));
        Rating rating = ratingService.findById(id);
        ModelAndView model = new ModelAndView("neo4j_addRating");
        model.addObject("rating",rating);

        return model;
    }



    // QUERIES

    @RequestMapping(value = "/neo4j_query1",method = RequestMethod.GET)
    public ModelAndView query1() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        long startTime = System.nanoTime();
        String query = "MATCH (n:Rating) WHERE n.rate > {0} RETURN n";
        List<Rating> ratingListWithRateMoreThan4 = ratingService.findByRate(4);
        long endTime = System.nanoTime();
        time = (endTime - startTime)/1000000;
        modelAndView.addObject("ratingListWithRateMoreThan4",ratingListWithRateMoreThan4);
        modelAndView.addObject("loadTime", time);
        modelAndView.addObject("query",query);
        modelAndView.setViewName("neo4j_queryResult");
        return modelAndView;
    }

    @RequestMapping(value = "/neo4j_query2",method = RequestMethod.GET)
    public ModelAndView query2() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        long startTime = System.nanoTime();
        String query = "MATCH (n:Rating) WHERE n.rate >= {0} RETURN n.movieId,COUNT(n) ORDER BY COUNT(n)";
        List<Rating> ratingCountList = ratingService.findByRateAndCount(5);
        long endTime = System.nanoTime();
        time = (endTime - startTime)/1000000;
        modelAndView.addObject("ratingCountList",ratingCountList);
        modelAndView.addObject("loadTime", time);
        modelAndView.addObject("query",query);
        modelAndView.setViewName("neo4j_queryResult");
        return modelAndView;
    }

    @RequestMapping(value = "/neo4j_query3",method = RequestMethod.GET)
    public ModelAndView query3() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        long startTime = System.nanoTime();
        String query = "MATCH (n:Rating) RETURN n.movieId,SUM(n.rate) ORDER BY SUM(n.rate)";
        List<Rating> ratingSumList = ratingService.sumRates();
        long endTime = System.nanoTime();
        time = (endTime - startTime)/1000000;
        modelAndView.addObject("ratingSumList",ratingSumList);
        modelAndView.addObject("loadTime", time);
        modelAndView.addObject("query",query);
        modelAndView.setViewName("neo4j_queryResult");
        return modelAndView;
    }




}
