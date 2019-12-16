package ips_project.ips.resource;

import ips_project.ips.csv.CSVUtils;
import ips_project.ips.model.ckMovie;
import ips_project.ips.model.ckRating;
import ips_project.ips.repository.MovieRepository;
import ips_project.ips.repository.RatingRepository;
import ips_project.ips.service.MovieService;
import ips_project.ips.service.RatingService;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.neo4j.transaction.Neo4jTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;


@RestController
public class CKResource {
    @Autowired
    RatingService ratingService;

    @Autowired
    MovieService moviesService;

    private long time;

    //Ładowanie danych do ck, wyswietlanie wyniku czasowego ladowania danych
    @RequestMapping(value = "/clickhouse_loadData",method = RequestMethod.GET)
    public ModelAndView loadData() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        ClickHouseStatement stm1 = connection.createStatement();
        ClickHouseStatement stm2 = connection.createStatement();


        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        connection.createStatement().execute("DROP TABLE IF EXISTS test.movies");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.movies " +
                        "(movieId Int32, title String, genre String)" +
                        "ENGINE = Log"
        );
        connection.createStatement().execute("DROP TABLE IF EXISTS test.ratings");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.ratings" +
                        "(userId Int32," +
                        " movieId Int32, " +
                        "rating Float32, " +
                        "timestamp String) " +
                        "ENGINE = Log");


        long startTime = System.nanoTime();
        stm1
                .write()
                .table("test.movies")
                .option("format_csv_delimiter",";")
                .data(new File("/IPSpec/data/movies.csv"), ClickHouseFormat.CSV)
                .send();
        stm2
                .write()
                .table("test.ratings")
                .option("format_csv_delimiter",":")
                .data(new File("/IPSpec/data/ratingss.csv"), ClickHouseFormat.CSV)
                .send();
        long endTime = System.nanoTime();

        time = (endTime - startTime)/1000000;
        connection.close();
        modelAndView.addObject("wczytano","Pomyślnie wczytano dane w czasie: " + time + "ms");
        modelAndView.addObject("loadTime", time);
        modelAndView.setViewName("/ck_main");
        return modelAndView;
    }
    //Buttony od oceny
    @RequestMapping(value = "/ck_addRating",method = RequestMethod.GET)
    public ModelAndView addRating() {
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/ck_addRating");
        return modelAndView;
    }


    @GetMapping("/ck_saveRating")
    public String showPage2(Model model){
        model.addAttribute("ck_rating", new ckRating());
        return "ck_saveRating";
    }

    @PostMapping(value = "/ck_saveRating")
    public ModelAndView saveRating(@ModelAttribute("ck_rating") ckRating rating) throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        int id;
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        //ResultSet rs = connection.createStatement().executeQuery("SELECT max(movieId) FROM test.movies");
        //System.out.println(rs);
        //rs.next();
        //id = rs.getInt(1) + 1;
        //System.out.println(id);

        String date = new SimpleDateFormat("ddmmyyyy").format(new Date());

        int uId = rating.getUserId();
        int mId = rating.getMovieId();
        float rati = rating.getRating();
        //String timestamp = rating.getTimestamp();


        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test.ratings (userId, movieId, rating, timestamp)" +
                        "VALUES (?,?,?,?)"
        );
        statement.setObject(1,uId);
        statement.setObject(2,mId);
        statement.setObject(3,rati);
        statement.setObject(4,date);
        statement.addBatch();
        statement.executeBatch();
        connection.close();
        modelAndView.setViewName("/ck_ratingList");
        return modelAndView;
    }




    @RequestMapping(value = "/ck_ratingList",method = RequestMethod.GET)
    public ModelAndView ratingList(Model model) {
        ModelAndView modelAndView = new ModelAndView();
        try {
            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            Statement stmt = connection.createStatement();
            String sql;
            sql = String.format("SELECT userId, movieId, rating, timestamp FROM test.ratings limit 1000");
            ResultSet rs = stmt.executeQuery(sql);

            List ratings = new ArrayList<>();
            while (rs.next()) {
                int userId = rs.getInt("userId");
                int movieId = rs.getInt("movieId");
                float rating = rs.getFloat("rating");
                String timestamp = rs.getString("timestamp");

                ratings.add(new ckRating(userId, movieId, rating, timestamp));
            }
            model.addAttribute("ratingList", ratings);
            modelAndView.setViewName("/ck_ratingList");
            return modelAndView;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return modelAndView;
    }


    //Buttony od filmu
    @RequestMapping(value = "/ck_addMovie",method = RequestMethod.GET)
    public ModelAndView addMovie(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/ck_addMovie");
        return modelAndView;
    }
    @RequestMapping(value = "/ck_movieList",method = RequestMethod.GET)
    public ModelAndView movieList(Model model)  {
        ModelAndView modelAndView = new ModelAndView();
        try {
            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            Statement stmt = connection.createStatement();
            String sql;
            sql = String.format("SELECT movieId, title, genre FROM test.movies limit 1000");
            ResultSet rs = stmt.executeQuery(sql);

            List movies = new ArrayList<>();
            while (rs.next()) {
                int movieId = rs.getInt("movieId");
                String title = rs.getString("title");
                String genre = rs.getString("genre");


                movies.add(new ckMovie(movieId, title, genre));
            }
            model.addAttribute("moviesList", movies);
            modelAndView.setViewName("/ck_movieList");
            return modelAndView;

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return modelAndView;

    }
    @RequestMapping(value = "/ck_queries",method = RequestMethod.GET)
    public ModelAndView queries(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/ck_queries");
        return modelAndView;
    }


    //Zapisywanie filmu
    @GetMapping("/ck_saveMovie")
    public String showPage(Model model){
        model.addAttribute("ck_movie", new ckMovie());
        return "ck_saveMovie";
    }

    @PostMapping(value = "/ck_saveMovie")
    public ModelAndView saveMovie(@ModelAttribute("ck_movie") ckMovie movie) throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        int id;
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        ResultSet rs = connection.createStatement().executeQuery("SELECT max(movieId) FROM test.movies");
        //System.out.println(rs);
        rs.next();
        id = rs.getInt(1) + 1;
        System.out.println(id);
        String title = movie.getTitle();
        String genres = movie.getGenre();

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test.movies (movieId, title, genre)" +
                        "VALUES (?,?,?)"
        );
        statement.setObject(1,id);
        statement.setObject(2,title);
        statement.setObject(3,genres);
        statement.addBatch();
        statement.executeBatch();
        connection.close();
        modelAndView.setViewName("/ck_movieList");
        return modelAndView;
    }


    @RequestMapping(value = "/deleteAllFromClickHouse", method = RequestMethod.GET)
    public ModelAndView deleteAll(HttpServletRequest request) throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();

        try {
            String sql = "DROP database test";
            long startTime = System.nanoTime();
            connection.createStatement().execute(sql);
            long endTime = System.nanoTime();

            time = (endTime - startTime) / 1000000;
            connection.close();

            ModelAndView model = new ModelAndView();
            model.addObject("usunieto", "Pomyślnie usunięto w czasie: " + time + "ms");
            model.setViewName("/ck_main");
            return model;
        } catch (Exception e) {
            ModelAndView model = new ModelAndView();
            model.addObject("error", "Baza danych jest pusta");
            model.setViewName("/ck_main");
            return model;
        }

    }


// zapytania


    @RequestMapping(value = "/ck_query1",method = RequestMethod.GET)
    public ModelAndView query1() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        ClickHouseStatement stm1 = connection.createStatement();
        String sql = "SELECT movieId FROM test.ratings WHERE rating > 4";
        long startTime = System.nanoTime();
        connection.createStatement().execute(sql);
        long endTime = System.nanoTime();

        time = (endTime - startTime)/1000000;
        connection.close();
        modelAndView.addObject("succes","Zapytanie "+ sql + " wykonano w czasie: " + time + "ms");
        modelAndView.addObject("error","Niepowodzenie");
        modelAndView.setViewName("/ck_queries");
        return modelAndView;
    }

    @RequestMapping(value = "/ck_query2",method = RequestMethod.GET)
    public ModelAndView query2() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        ClickHouseStatement stm1 = connection.createStatement();
        String sql = "SELECT movieId, count(*) FROM test.ratings WHERE rating >=5 GROUP BY movieId ORDER BY count(*)";
        long startTime = System.nanoTime();
        connection.createStatement().execute(sql);
        long endTime = System.nanoTime();

        time = (endTime - startTime)/1000000;
        connection.close();
        modelAndView.addObject("succes","Zapytanie "+ sql + " wykonano w czasie: " + time + "ms");
        modelAndView.addObject("error","Niepowodzenie");
        modelAndView.setViewName("/ck_queries");
        return modelAndView;
    }


    @RequestMapping(value = "/ck_query3",method = RequestMethod.GET)
    public ModelAndView query3() throws SQLException {
        ModelAndView modelAndView = new ModelAndView();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        ClickHouseStatement stm1 = connection.createStatement();
        String sql = "SELECT movieId, sum(rating) FROM test.ratings GROUP BY movieId ORDER BY sum(rating)";
        long startTime = System.nanoTime();
        connection.createStatement().execute(sql);
        long endTime = System.nanoTime();

        time = (endTime - startTime)/1000000;
        connection.close();
        modelAndView.addObject("succes","Zapytanie "+ sql + " wykonano w czasie: " + time + "ms");
        modelAndView.addObject("error","Niepowodzenie");
        modelAndView.addObject("query",sql);
        modelAndView.setViewName("/ck_queries");
        return modelAndView;
    }



   /* @RequestMapping(value = "/test",method = RequestMethod.GET)
    public ModelAndView neo4j_loadRatingFromCK(Model model) throws IOException, SQLException {
        ModelAndView modelAndView = new ModelAndView();
        String ratingsCsv = "neo4jdata/ck_ratings.csv";
        FileWriter writerRatingsCsv = new FileWriter(ratingsCsv);
        long startTime = System.nanoTime();
        long start = 0;
        long end = 500;
        int i;
        List<ckRating> ratings = new ArrayList<>();
        List<String> header = new ArrayList<>();
        header.add("UserId,MovieId,Rate,Timestamp");

        CSVUtils.writeLine(writerRatingsCsv,header);

        try {

            for (i = 0; i <= 71567; i = i + 500) {

                ClickHouseDataSource dataSource = new ClickHouseDataSource(
                        "jdbc:clickhouse://clickhouse-ips:8123");
                ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();



                Statement stmt = connection.createStatement();
                String sql;
                sql = String.format("SELECT userId, movieId, rating, timestamp FROM test.ratings WHERE userId BETWEEN " + start +" AND " + end + "");
                System.out.println("\n\nSQL STRING:  " + sql);
                ResultSet rs = stmt.executeQuery(sql);
                start = start + 500;
                end = end + 501;
                while (rs.next()) {

                    int userId = rs.getInt("userId");
                    int movieId = rs.getInt("movieId");
                    float rating = rs.getFloat("rating");
                    String timestamp = rs.getString("timestamp");

                    ratings.add(new ckRating(userId, movieId, rating, timestamp));
                }

                System.out.println("\n\nWielkosc listy: " + ratings.size());
                connection.close();
            }
            for (ckRating ckRating : ratings) {

                List<String> list = new ArrayList<>();
                list.add(String.valueOf(ckRating.getUserId()));
                list.add(String.valueOf(ckRating.getMovieId()));
                list.add(String.valueOf(ckRating.getRating()));
                list.add(String.valueOf(ckRating.getTimestamp()));


                CSVUtils.writeLine(writerRatingsCsv, list);
            }
            writerRatingsCsv.flush();
            writerRatingsCsv.close();
            System.out.println("\n\n ZAPISANIE DO PLIKU");

            long endTime = System.nanoTime();

            time = (endTime - startTime)/1000000;
            ratingService.loadRatingCsvFromClickHouse();

            modelAndView.addObject("time",time);
            modelAndView.addObject("succes","Skopiowano tabelę z ClickHouse w czasie: ");
            modelAndView.setViewName("neo4j_main");
            return modelAndView;

        } catch (Exception e) {
            System.out.println(e);
            modelAndView.addObject("error","Tabela nie istnieje");
            modelAndView.setViewName("neo4j_main");
            return modelAndView;
        }

    }*/

    @RequestMapping(value = "/neo4j_loadRatingFromCK",method = RequestMethod.GET)
    public ModelAndView neo4j_loadRatingFromCK(Model model) throws IOException {
        ModelAndView modelAndView = new ModelAndView();
        String ratingsCsv = "neo4jdata/ck_ratings.csv";
        FileWriter writerRatingsCsv = new FileWriter(ratingsCsv);
        long startTime = System.nanoTime();
        try {
            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            Statement stmt = connection.createStatement();
            String sql;
            sql = String.format("SELECT userId, movieId, rating, timestamp FROM test.ratings");
            ResultSet rs = stmt.executeQuery(sql);
            List<ckRating> ratings = new ArrayList<>();
            List<String> header = new ArrayList<>();
            header.add("UserId,MovieId,Rate,Timestamp");
            CSVUtils.writeLine(writerRatingsCsv,header);
            while (rs.next()) {

                int userId = rs.getInt("userId");
                int movieId = rs.getInt("movieId");
                float rating = rs.getFloat("rating");
                String timestamp = rs.getString("timestamp");

                ratings.add(new ckRating(userId, movieId, rating, timestamp));
            }

            for (ckRating ckRating : ratings) {

                List<String> list = new ArrayList<>();
                list.add(String.valueOf(ckRating.getMovieId()));
                list.add(String.valueOf(ckRating.getTimestamp()));
                list.add(String.valueOf(ckRating.getUserId()));
                list.add(String.valueOf(ckRating.getRating()));

                CSVUtils.writeLine(writerRatingsCsv, list);
            }
            writerRatingsCsv.flush();
            writerRatingsCsv.close();
            connection.close();
            long endTime = System.nanoTime();

            time = (endTime - startTime)/1000000;
            ratingService.loadRatingCsvFromClickHouse();
            modelAndView.addObject("time",time);
            modelAndView.addObject("succes","Skopiowano tabelę z ClickHouse w czasie: ");
            modelAndView.setViewName("neo4j_main");
            return modelAndView;

        } catch (Exception e) {
            modelAndView.addObject("error","Tabela nie istnieje");
            modelAndView.setViewName("neo4j_main");
            return modelAndView;
        }

    }

    @RequestMapping(value = "/neo4j_loadMoviesFromCK",method = RequestMethod.GET)
    public ModelAndView neo4j_loadMoviesFromCK(Model model) throws IOException {
        ModelAndView modelAndView = new ModelAndView();
        String moviesCsv = "neo4jdata/ck_movies.csv";
        FileWriter writerMoviesCsv = new FileWriter(moviesCsv);
        long startTime = System.nanoTime();
        try {
            ClickHouseDataSource dataSource = new ClickHouseDataSource(
                    "jdbc:clickhouse://clickhouse-ips:8123");
            ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
            Statement stmt = connection.createStatement();
            String sql;
            sql = String.format("SELECT movieId, title, genre FROM test.movies");
            ResultSet rs = stmt.executeQuery(sql);
            List<ckMovie> movies = new ArrayList<>();
            List<String> header = new ArrayList<>();
            header.add("MovieId,Title,Genres");
            CSVUtils.writeLine(writerMoviesCsv,header);
            while (rs.next()) {

                int movieId = rs.getInt("movieId");
                String title = rs.getString("title");
                String genre = rs.getString("genre");

                movies.add(new ckMovie(movieId, title, genre));
            }

            for (ckMovie ckMovie : movies) {

                List<String> list = new ArrayList<>();
                list.add(String.valueOf(ckMovie.getMovieId()));
                list.add(String.valueOf(ckMovie.getTitle()));
                list.add(String.valueOf(ckMovie.getGenre()));

                CSVUtils.writeLine(writerMoviesCsv, list);
            }
            writerMoviesCsv.flush();
            writerMoviesCsv.close();
            connection.close();


            long endTime = System.nanoTime();

            time = (endTime - startTime)/1000000;

            moviesService.loadMoviesCsvFromClickHouse();
            modelAndView.addObject("time",time);
            modelAndView.addObject("succes","Skopiowano tabelę z ClickHouse w czasie: ");
            modelAndView.setViewName("neo4j_main");
            return modelAndView;

        } catch (Exception e) {
            modelAndView.addObject("error","Tabela nie istnieje");
            modelAndView.setViewName("neo4j_main");
            return modelAndView;
        }

    }







}
