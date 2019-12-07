package ips_project.ips.service;

import ips_project.ips.model.ckMovie;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
public class ckMovieService {


    JdbcTemplate template;

    public void setTemplate(JdbcTemplate template) {
        this.template = template;
    }

    public List<ckMovie> getAllMovies() throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://clickhouse-ips:8123");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        ResultSet rs = connection.createStatement().executeQuery("SELECT  FROM test.movies");



        String sql ="select * from test.movies";
        return template.query(sql, new RowMapper<ckMovie>() {
            @Override
            public ckMovie mapRow(ResultSet rs, int rowNum) throws SQLException {
                ckMovie movie = new ckMovie();
                movie.setMovieId(rs.getInt(1));
                movie.setTitle(rs.getString(2));
                movie.setTag(rs.getString(3));
                return movie;
            }
        });
    }
}
