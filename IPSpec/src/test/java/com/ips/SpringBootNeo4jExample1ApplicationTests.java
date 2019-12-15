package com.ips;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.*;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBootNeo4jExample1ApplicationTests {

    @Test
    public void contextLoads() {
    }

    @Test
    public void testSetCatalogAndPreparedStatementsClickhouse() throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://localhost:8123/default?option1=one%20two&option2=y");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        final String sql = "SELECT currentDatabase() FROM system.tables WHERE name = ? LIMIT 1";

        connection.setCatalog("system");
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, "tables");
        connection.setCatalog("default");
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        assertEquals(resultSet.getString(1), "system");

        statement = connection.prepareStatement(sql);
        statement.setString(1, "tables");
        resultSet = statement.executeQuery();
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
    }
    @Test
    public void testSetCatalogAndStatementsClickhouse() throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://localhost:8123/default?option1=one%20two&option2=y");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();
        final String sql = "SELECT currentDatabase()";

        connection.setCatalog("system");
        Statement statement = connection.createStatement();
        connection.setCatalog("default");
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals(resultSet.getString(1), "system");

        statement = connection.createStatement();
        resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
    }


    Connection writer;
    Connection reader;


    @Test public void commitShouldWorkFineNeo4j() throws SQLException {
        writer.setAutoCommit(false);

        // Creating a node with a transaction
        try (Statement stmt = writer.createStatement()) {
            stmt.executeQuery("CREATE (:CommitShouldWorkFine{result:\"ok\"})");

            Statement stmtRead = reader.createStatement();
            ResultSet rs = stmtRead.executeQuery("MATCH (n:CommitShouldWorkFine) RETURN n.result");
            assertFalse(rs.next());

            writer.commit();
            rs = stmtRead.executeQuery("MATCH (n:CommitShouldWorkFine) RETURN n.result");
            assertTrue(rs.next());
            assertEquals("ok", rs.getString("n.result"));
            assertFalse(rs.next());
        }

    }

    @Test public void setAutoCommitShouldCommitFromFalseToTrueNeo4j() throws SQLException {
        writer.setAutoCommit(false);

        // Creating a node with a transaction
        try (Statement stmt = writer.createStatement()) {
            stmt.executeQuery("CREATE (:SetAutoCommitSwitch{result:\"ok\"})");

            Statement stmtRead = reader.createStatement();
            ResultSet rs = stmtRead.executeQuery("MATCH (n:SetAutoCommitSwitch) RETURN n.result");
            assertFalse(rs.next());

            writer.setAutoCommit(true);
            rs = stmtRead.executeQuery("MATCH (n:SetAutoCommitSwitch) RETURN n.result");
            assertTrue(rs.next());
            assertEquals("ok", rs.getString("n.result"));
            assertFalse(rs.next());
        }

    }
}