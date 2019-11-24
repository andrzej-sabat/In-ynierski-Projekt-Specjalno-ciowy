package ips_project.ips.resource;

import ips_project.ips.model.ckMovie;
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


@RestController
public class CKResource {
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
                        "(movieId Int32, title String, tag String, genre String)" +
                        "ENGINE = Join(ANY, LEFT, movieId)"
        );
        connection.createStatement().execute("DROP TABLE IF EXISTS test.ratings");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.ratings" +
                        "(userId Int32," +
                        " movieId Int32, " +
                        "rating String, " +
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
//Mapowanie buttonów menu
    @RequestMapping(value = "/ck_addMovie",method = RequestMethod.GET)
    public ModelAndView addMovie(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/ck_addMovie");
        return modelAndView;
    }
    @RequestMapping(value = "/ck_movieList",method = RequestMethod.GET)
    public ModelAndView movieList(){
        ModelAndView modelAndView = new ModelAndView();
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
        ClickHouseStatement stm1 = connection.createStatement();
        ResultSet rs = connection.createStatement().executeQuery("SELECT max(movieId) FROM test.movies");
        System.out.println(rs);
        rs.next();
        id = rs.getInt(1) + 1;
        System.out.println(id);
        String title = movie.getTitle();
        String genres = movie.getTag();

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test.movies (movieId, title, tag, genre)" +
                        "VALUES (?,?,?,?)"
        );
        statement.setObject(1,id);
        statement.setObject(2,title);
        statement.setObject(3,null);
        statement.setObject(4,null);
        statement.addBatch();
        statement.executeBatch();
        connection.close();
        modelAndView.setViewName("/ck_movieList");
        return modelAndView;
    }


}
