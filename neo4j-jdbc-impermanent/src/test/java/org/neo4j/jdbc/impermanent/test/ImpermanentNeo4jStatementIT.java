package org.neo4j.jdbc.impermanent.test; /**
 * Copyright (c) 2016 LARUS Business Automation [http://www.larus-ba.it]
 * <p>
 * This file is part of the "LARUS Integration Framework for Neo4j".
 * <p>
 * The "LARUS Integration Framework for Neo4j" is licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Created on 05/12/17
 */

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jdbc.Neo4jResultSet;
import org.neo4j.jdbc.data.StatementData;
import org.neo4j.jdbc.impermanent.ImpermanentNeo4jConnection;

import java.sql.*;

import static org.junit.Assert.*;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class ImpermanentNeo4jStatementIT {

	public static final String JDBC_NEO4J_MEM = "jdbc:neo4j:mem";

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	/*------------------------------*/
	/*          executeQuery        */
	/*------------------------------*/
	@Test public void executeQueryShouldExecuteAndReturnCorrectData() throws SQLException {
		ImpermanentNeo4jConnection connection = (ImpermanentNeo4jConnection) DriverManager.getConnection(JDBC_NEO4J_MEM);
		GraphDatabaseService gds = connection.getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute(StatementData.STATEMENT_CREATE);
			tx.success();
		}
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(StatementData.STATEMENT_MATCH_ALL_STRING);

		assertTrue(rs.next());
		assertEquals("test", rs.getString(1));
		assertFalse(rs.next());
		connection.close();
		try (Transaction tx = gds.beginTx()) {
			gds.execute(StatementData.STATEMENT_CREATE_REV);
			tx.success();
		}
	}

	@Test public void executeQueryShouldExecuteAndReturnCorrectDataOnAutoCommitFalseStatement() throws SQLException {
		ImpermanentNeo4jConnection connection = (ImpermanentNeo4jConnection) DriverManager.getConnection(JDBC_NEO4J_MEM);
		GraphDatabaseService gds = connection.getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute(StatementData.STATEMENT_CREATE);
			tx.success();
		}
		Statement statement = connection.createStatement();
		connection.setAutoCommit(false);

		ResultSet rs = statement.executeQuery(StatementData.STATEMENT_MATCH_ALL_STRING);

		connection.commit();
		assertTrue(rs.next());
		assertEquals("test", rs.getString(1));
		assertFalse(rs.next());
		connection.close();
		try (Transaction tx = gds.beginTx()) {
			gds.execute(StatementData.STATEMENT_CREATE_REV);
			tx.success();
		}
	}

	@Test public void executeQueryShouldExecuteAndReturnCorrectDataOnAutoCommitFalseStatementAndCreatedWithParams() throws SQLException {
		ImpermanentNeo4jConnection connection = (ImpermanentNeo4jConnection) DriverManager.getConnection(JDBC_NEO4J_MEM);
		GraphDatabaseService gds = connection.getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute(StatementData.STATEMENT_CREATE);
			tx.success();
		}
		Statement statement = connection.createStatement(Neo4jResultSet.TYPE_FORWARD_ONLY, Neo4jResultSet.CONCUR_READ_ONLY);
		connection.setAutoCommit(false);

		ResultSet rs = statement.executeQuery(StatementData.STATEMENT_MATCH_ALL_STRING);

		connection.commit();
		assertTrue(rs.next());
		assertEquals("test", rs.getString(1));
		assertFalse(rs.next());
		connection.close();
		try (Transaction tx = gds.beginTx()) {
			gds.execute(StatementData.STATEMENT_CREATE_REV);
			tx.success();
		}
	}

	/*------------------------------*/
	/*         executeUpdate        */
	/*------------------------------*/
	@Test public void executeUpdateShouldExecuteAndReturnCorrectData() throws SQLException {
		Connection connection = DriverManager.getConnection(JDBC_NEO4J_MEM);
		Statement statement = connection.createStatement();
		int lines = statement.executeUpdate(StatementData.STATEMENT_CREATE);
		assertEquals(1, lines);

		lines = statement.executeUpdate(StatementData.STATEMENT_CREATE);
		assertEquals(1, lines);

		lines = statement.executeUpdate(StatementData.STATEMENT_CREATE_REV);
		assertEquals(2, lines);

		connection.close();
	}

	/*------------------------------*/
	/*            execute           */
	/*------------------------------*/
	@Test public void executeShouldExecuteAndReturnFalse() throws SQLException {
		Connection connection = DriverManager.getConnection(JDBC_NEO4J_MEM);
		Statement statement = connection.createStatement();
		boolean result = statement.execute(StatementData.STATEMENT_CREATE);
		assertFalse(result);

		result = statement.execute(StatementData.STATEMENT_CREATE_REV);
		assertFalse(result);

		connection.close();
	}

	@Test public void executeShouldExecuteAndReturnTrue() throws SQLException {
		Connection connection = DriverManager.getConnection(JDBC_NEO4J_MEM);
		Statement statement = connection.createStatement();
		boolean result = statement.execute(StatementData.STATEMENT_MATCH_ALL);
		assertTrue(result);

		connection.close();
	}

	@Test public void executeBadCypherQueryOnAutoCommitShouldReturnAnSQLException() throws SQLException {
		expectedEx.expect(SQLException.class);
		expectedEx.expectMessage("Invalid input");

		Connection connection = DriverManager.getConnection(JDBC_NEO4J_MEM);

		Statement statement = connection.createStatement();
		try {
			statement.execute("AZERTYUIOP");
		} finally {
			connection.close();
		}
	}

	@Test public void executeBadCypherQueryWithoutAutoCommitShouldReturnAnSQLException() throws SQLException {
		expectedEx.expect(SQLException.class);
		expectedEx.expectMessage("Invalid input");

		Connection connection = DriverManager.getConnection(JDBC_NEO4J_MEM);
		connection.setAutoCommit(false);

		Statement statement = connection.createStatement();
		try {
			statement.execute("AZERTYUIOP");
		} finally {
			connection.close();
		}
	}

}
