package ips_project.ips.resource;

import ips_project.ips.model.ckMovie;
import ips_project.ips.model.ckRating;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


@RestController
public class CKResource {

    private long time;
    ips_project.ips.service.ckMovieService ckMovieService;


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
                        "ENGINE = Join(ANY, LEFT, movieId)"
        );
        connection.createStatement().execute("DROP TABLE IF EXISTS test.ratings");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.ratings" +
                        "(userId Int32," +
                        " movieId Int32, " +
                        "rating Float32, " +
                        "timestamp String) " +
                        "ENGINE = Join(ANY, LEFT, userId)");


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
                .data(new File("/IPSpec/data/ratings.csv"), ClickHouseFormat.CSV)
                .send();
        long endTime = System.nanoTime();

        time = (endTime - startTime)/1000000;
        connection.close();
        modelAndView.addObject("loadTime", time);
        modelAndView.setViewName("/ck_loadDataResult");
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
            sql = String.format("SELECT userId, movieId, rating, timestamp FROM test.ratings");
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
        //List<ckMovie> moviesList = ckMovieService.getAllMovies();
        //model.addAttribute("moviesList", moviesList);



        modelAndView.setViewName("/ck_movieList");
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
                        "VALUES (?,?,?,?)"
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


}