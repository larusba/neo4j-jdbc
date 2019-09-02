package org.neo4j.jdbc.impl;

import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

/**
 * @description
 * @time 创建时间:2019/9/3 0:44
 * @param
 * @author xin.gong
 */
public class Neo4jConnectionImplTest {

    Neo4jConnectionImpl neo4jConnection = new Neo4jConnectionImpl(){
        @Override public Statement createStatement() throws SQLException {
            return null;
        }

        @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
            return null;
        }

        @Override public void setAutoCommit(boolean autoCommit) throws SQLException {

        }

        @Override public boolean getAutoCommit() throws SQLException {
            return false;
        }

        @Override public void commit() throws SQLException {

        }

        @Override public void rollback() throws SQLException {

        }

        @Override public void close() throws SQLException {

        }

        @Override public boolean isClosed() throws SQLException {
            return false;
        }

        @Override public DatabaseMetaData getMetaData() throws SQLException {
            return null;
        }

        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return null;
        }

        @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                throws SQLException {
            return null;
        }

        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                throws SQLException {
            return null;
        }

        @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                int resultSetHoldability) throws SQLException {
            return null;
        }

        @Override public boolean isValid(int timeout) throws SQLException {
            return false;
        }
    };
    @Test public void checkTransactionIsolationShouldReturnNothing() {
        Integer[] levels = {0, 1, 2, 4, 8};
        for (int level : levels) {
            try {
                neo4jConnection.checkTransactionIsolation(level);
            } catch (SQLException e) {
                fail();
            }
        }
    }

    @Test public void checkTransactionIsolationShouldThrowExceptionWhenInputErrorLevel() {
        Integer[] levels = {3, 5, 7, 9};
        for (int level : levels) {
            try {
                neo4jConnection.checkTransactionIsolation(level);
                fail();
            } catch (SQLException e) {
                // OK
            }
        }
    }
}
