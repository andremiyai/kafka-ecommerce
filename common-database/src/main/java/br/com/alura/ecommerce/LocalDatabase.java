package br.com.alura.ecommerce;

import java.sql.*;
import java.util.UUID;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:target/" + name + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql){
        try {
            connection.createStatement().execute(sql);
        }catch(SQLException ex){
            // banco ja existe
            ex.printStackTrace();
        }
    }

    public void update(String statement, String ... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(statement, params);
        preparedStatement.execute();
    }

    public ResultSet query(String statement, String ... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(statement, params);
        return preparedStatement.executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for(int i = 0; i< params.length ; i++) {
            preparedStatement.setString(i+ 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
