package br.com.alura_kafka;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String nameDatabase) throws SQLException {
        String url = "jdbc:sqlite:target/" + nameDatabase + ".db";
        connection = DriverManager.getConnection(url);
    }

    public void createIfNotExist(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void update(String query, String... params) throws SQLException {
        preparedStatement(query, params).execute();
    }

    public ResultSet query(String query, String... params) throws SQLException {
        return preparedStatement(query, params).executeQuery();
    }

    private PreparedStatement preparedStatement(String query, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(query);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
