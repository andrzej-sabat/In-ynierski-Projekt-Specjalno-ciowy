package ips_project.ips.resource;

import com.opencsv.CSVWriter;
import ips_project.ips.csv.CSVUtils;
import ips_project.ips.model.Movie;
import ips_project.ips.model.Rating;
import ips_project.ips.repository.MovieRepository;
import ips_project.ips.repository.RatingRepository;
import ips_project.ips.service.MovieService;
import ips_project.ips.service.RatingService;
import jnr.ffi.annotations.In;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;


import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
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
        model.addObject("wczytano","Pomyślnie wczytano dane w czasie: " + time + "ms");
        model.addObject("loadTime", time);
        model.setViewName("neo4j_main");
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
        long startTime = System.nanoTime();
        List<Rating> ratingsList = (List<Rating>) ratingService.findAllRatings();
        long endTime = System.nanoTime();
        time = (endTime - startTime)/1000000;
        model.addObject("time",time);
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
        List<Rating> ratingListWithRateMoreThan4 = ratingService.findByRate((float) 4);
        long endTime = System.nanoTime();
        time = (endTime - startTime)/1000000;
        modelAndView.addObject("ratingListWithRateMoreThan4",ratingListWithRateMoreThan4);
        modelAndView.addObject("succes","Zapytanie "+ query + " wykonano w czasie: " + time + "ms");
        modelAndView.addObject("query",query);
        modelAndView.setViewName("neo4j_queries");
        return modelAndView;
    }

    @RequestMapping(value = "/neo4j_query2",method = RequestMethod.GET)
    public ModelAndView query2() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        long startTime = System.nanoTime();
        String query = "MATCH (n:Rating) WHERE n.rate >= {0} RETURN n.movieId,COUNT(n) ORDER BY COUNT(n)";
        List<Rating> ratingCountList = ratingService.findByRateAndCount((float) 5);
        long endTime = System.nanoTime();
        time = (endTime - startTime)/1000000;
        modelAndView.addObject("ratingCountList",ratingCountList);
        modelAndView.addObject("succes","Zapytanie "+ query + " wykonano w czasie: " + time + "ms");
        modelAndView.addObject("query",query);
        modelAndView.setViewName("neo4j_queries");
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
        modelAndView.addObject("succes","Zapytanie "+ query + " wykonano w czasie: " + time + "ms");
        modelAndView.addObject("query",query);
        modelAndView.setViewName("neo4j_queries");
        return modelAndView;
    }



    @RequestMapping(value= "/ck_loadRatingsFromNeo4j",method = RequestMethod.GET)
    public ModelAndView ck_loadRatingsFromNeo4j() throws SQLException,IOException {
        ModelAndView model = new ModelAndView();
        String uri = "bolt://neo4j-ips2019:7687";
        List<Rating> ratings = new ArrayList<>();
        String ratingsCsv = "data/neo4j_ratings.csv";
        FileWriter writerRatingsCsv = new FileWriter(ratingsCsv);
        long startTime = System.nanoTime();
        Integer start = 0;
        Integer end = 50000;
        int i;

       try{

           Driver driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "admin"));
           Session session = driver.session();


            for (i = 0; i <= 10500000; i = i + 50000) {
                StatementResult result = session.run("MATCH (n:Rating) WHERE ID(n) >= {0} AND ID(n) <= {1} RETURN n.movieId as movieId,n.rate as rate,n.userId as userId,n.timestamp as timestamp;",Values.parameters("0",start,"1",end));
               /* System.out.println(*//*"USER ID: " + userId + " MOVIE ID: " + movieId + " RATING: " + rating + " TIMESTAMP: " + timestamp +*//* " \n\n\n" + "Wielkosc listy:   " + ratings.size() + "\n\n\n");
                System.out.println("Lista od: " + start + " do " + end);*/

                start = start + 50001;
                end = end + 50001;

                while (result.hasNext()) {
                    Record record = result.next();

                    int userId = record.get("userId").asInt();
                    int movieId = record.get("movieId").asInt();
                    float rating = record.get("rate").asFloat();
                    String timestamp = record.get("timestamp").asString();



                    ratings.add(new Rating(userId, movieId, rating, timestamp));

                }


            }

            for (Rating rating : ratings) {

                List<String> list = new ArrayList<>();
                list.add(String.valueOf(rating.getUserId()));
                list.add(String.valueOf(rating.getMovieId()));
                list.add(String.valueOf(rating.getRate()));
                list.add(String.valueOf(rating.getTimestamp()));



                CSVUtils.writeLine(writerRatingsCsv, list);
            }
            writerRatingsCsv.flush();
            writerRatingsCsv.close();

            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            ClickHouseStatement stm1 = connection.createStatement();

            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
            connection.createStatement().execute("DROP TABLE IF EXISTS test.ratings");
            connection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS test.ratings" +
                            "(userId Int32," +
                            " movieId Int32, " +
                            "rating Float32, " +
                            "timestamp String) " +
                            "ENGINE = Log");


            stm1
                    .write()
                    .table("test.ratings")
                    .option("format_csv_delimiter", ",")
                    .data(new File("/IPSpec/data/neo4j_ratings.csv"), ClickHouseFormat.CSV)
                    .send();

            long endTime = System.nanoTime();
            time = (endTime - startTime) / 1000000;
            connection.close();
            model.addObject("time", time);
            model.addObject("succes", "Skopiowano tabelę z Neo4j w czasie: ");
            model.setViewName("ck_main");
            return model;

        } catch (Exception e) {
            System.out.println(e);
            model.addObject("error", "Niepowodzenie");
            model.setViewName("ck_main");
            return model;

        }
    }

    @RequestMapping(value= "/ck_loadMoviesFromNeo4j",method = RequestMethod.GET)
    public ModelAndView ck_loadMoviesFromNeo4j() throws SQLException,IOException {
        ModelAndView model = new ModelAndView();
        String uri = "bolt://neo4j-ips2019:7687";
        List<Movie> movies = new ArrayList<>();
        String moviesCsv = "data/neo4j_movies.csv";
        FileWriter writerRatingsCsv = new FileWriter(moviesCsv);
        long startTime = System.nanoTime();
        Integer start = 0;
        Integer end = 50000;
        int i;

        try{

            Driver driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "admin"));
            Session session = driver.session();


            for (i = 0; i <= 110000; i = i + 50000) {
                StatementResult result = session.run("MATCH (n:Movie) WHERE ID(n) >= {0} AND ID(n) <= {1} RETURN n.movieId as movieId,n.title as title,n.genres as genres",Values.parameters("0",start,"1",end));
              /*  System.out.println(" \n\n\n" + "Wielkosc listy:   " + movies.size() + "\n\n\n");
                System.out.println("Lista od: " + start + " do " + end);*/

                start = start + 50001;
                end = end + 50001;

                while (result.hasNext()) {
                    Record record = result.next();


                    int movieId = record.get("movieId").asInt();
                    String title = record.get("title").asString();
                    String genres = record.get("genres").asString();


                    movies.add(new Movie(movieId, title, genres));

                }


            }

            for (Movie movie : movies) {

                List<String> list = new ArrayList<>();
                list.add(String.valueOf(movie.getMovieId()));
                list.add(String.valueOf(movie.getTitle()));
                list.add(String.valueOf(movie.getGenres()));


                CSVUtils.writeLine(writerRatingsCsv, list);
            }
            writerRatingsCsv.flush();
            writerRatingsCsv.close();

            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            ClickHouseStatement stm1 = connection.createStatement();

            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
            connection.createStatement().execute("DROP TABLE IF EXISTS test.movies");
            connection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS test.movies " +
                            "(movieId Int32, title String, genre String)" +
                            "ENGINE = Log"
            );

            stm1
                    .write()
                    .table("test.movies")
                    .option("format_csv_delimiter", ",")
                    .data(new File("/IPSpec/data/neo4j_movies.csv"), ClickHouseFormat.CSV)
                    .send();
            long endTime = System.nanoTime();
            time = (endTime - startTime) / 1000000;
            connection.close();
            model.addObject("time",time);
            model.addObject("succes", "Skopiowano tabelę z Neo4j w czasie: ");
            model.setViewName("ck_main");
            return model;
        } catch (Exception e) {
            model.addObject("error", "Niepowodzenie");
            model.setViewName("ck_main");
            return model;

        }
    }


    @RequestMapping(value = "/deleteAllFromNeo4j", method = RequestMethod.GET)
    public ModelAndView deleteAllFromNeo4j(ModelAndView model){
        long startTime = System.nanoTime();
        do {

            ratingRepository.deleteAll();

        }while(ratingRepository.numberOfNodes() != 0);
        long endTime = System.nanoTime();
        time = (endTime - startTime) / 1000000;
        model.addObject("usunieto","Pomyślnie usunięto wszystkie rekordy w czasie: " + time + "ms");
        model.setViewName("neo4j_main");
        return model;

    }




      /*@RequestMapping(value= "/ck_loadRatingsFromNeo4j",method = RequestMethod.GET)
    public ModelAndView ck_loadRatingsFromNeo4j() throws SQLException,IOException {
        ModelAndView model = new ModelAndView();
        try {
            Long time2;
            Long time3;
            long startTime = System.nanoTime();

            String ratingCsv = "data/neo4j_ratings.csv";
            FileWriter writerRatingCsv = new FileWriter(ratingCsv);

            System.out.println("Rozpoczęcie odczytania tabeli Rating do listy");
            long startTime1 = System.nanoTime();
            List<Rating> ratings = (List<Rating>) ratingService.findAllRatings();
            long endTime2 = System.nanoTime();
            time2 = (endTime2 - startTime1) / 1000000;
            System.out.println("Czas wykonania: " + time2);
            System.out.println("Rozpoczęcie zapisu do pliku");
            long startTime3 = System.nanoTime();
            for (Rating rate : ratings) {
                List<String> list = new ArrayList<>();
                list.add(String.valueOf(rate.getUserId()));
                list.add(String.valueOf(rate.getMovieId()));
                list.add(String.valueOf(rate.getRate()));
                list.add(String.valueOf(rate.getTimestamp()));

                CSVUtils.writeLine(writerRatingCsv, list);
            }

            writerRatingCsv.flush();
            writerRatingCsv.close();

            long endTime3 = System.nanoTime();
            time3 = (endTime3 - startTime3) / 1000000;
            System.out.println("Czas wykonania: " + time3);
            System.out.println("Rozpoczęcie zapisu do ClickHouse");
            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            ClickHouseStatement stm1 = connection.createStatement();

            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
            connection.createStatement().execute("DROP TABLE IF EXISTS test.ratings");
            connection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS test.ratings" +
                            "(userId Int32," +
                            " movieId Int32, " +
                            "rating Float32, " +
                            "timestamp String) " +
                            "ENGINE = Log");


            stm1
                    .write()
                    .table("test.ratings")
                    .option("format_csv_delimiter", ",")
                    .data(new File("/IPSpec/data/neo4j_ratings.csv"), ClickHouseFormat.CSV)
                    .send();

            long endTime = System.nanoTime();
            time = (endTime - startTime) / 1000000;
            connection.close();
            model.addObject("time",time);
            model.addObject("succes","Skopiowano tabelę z Neo4j w czasie: ");
            model.setViewName("ck_main");
            return model;
        }
        catch (Exception e){
            model.addObject("error","Niepowodzenie");
            model.setViewName("ck_main");
            return model;

        }
    }*/

    /*@RequestMapping(value= "/ck_loadMoviesFromNeo4j",method = RequestMethod.GET)
    public ModelAndView ck_loadMoviesFromNeo4j() throws SQLException,IOException {
        ModelAndView model = new ModelAndView();
        try {
            long startTime = System.nanoTime();

            String moviesCsv = "data/neo4j_movies.csv";
            FileWriter writerMoviesCsv = new FileWriter(moviesCsv);

            List<Movie> movies = (List<Movie>) movieService.findAllMovies();

            for (Movie movie : movies) {

                List<String> list = new ArrayList<>();
                list.add(String.valueOf(movie.getMovieId()));
                list.add(movie.getTitle());
                list.add(movie.getGenres());

                CSVUtils.writeLine(writerMoviesCsv, list);
            }

            writerMoviesCsv.flush();
            writerMoviesCsv.close();

            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            ClickHouseStatement stm1 = connection.createStatement();

            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
            connection.createStatement().execute("DROP TABLE IF EXISTS test.movies");
            connection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS test.movies " +
                            "(movieId Int32, title String, genre String)" +
                            "ENGINE = Log"
            );


            stm1
                    .write()
                    .table("test.movies")
                    .option("format_csv_delimiter", ",")
                    .data(new File("/IPSpec/data/neo4j_movies.csv"), ClickHouseFormat.CSV)
                    .send();
            long endTime = System.nanoTime();
            time = (endTime - startTime) / 1000000;
            connection.close();
            model.addObject("time",time);
            model.addObject("succes", "Skopiowano tabelę z Neo4j w czasie: ");
            model.setViewName("ck_main");
            return model;
        } catch (Exception e) {
            model.addObject("error", "Niepowodzenie");
            model.setViewName("ck_main");
            return model;

        }
    }*/


}
