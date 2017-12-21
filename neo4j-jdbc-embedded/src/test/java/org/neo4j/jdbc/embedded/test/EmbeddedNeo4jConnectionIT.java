package org.neo4j.jdbc.embedded.test; /**
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

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.jdbc.data.StatementData;
import org.neo4j.jdbc.embedded.EmbeddedNeo4jConnection;
import org.neo4j.jdbc.utils.UncaughtExceptionLogger;

import java.io.File;
import java.sql.*;

import static org.junit.Assert.*;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
//TODO Check why in a single thread transaction isolation isn't working.
public class EmbeddedNeo4jConnectionIT {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private static String NEO4J_JDBC_IMPERMANENT_URL;
	private static GraphDatabaseService neo4j;
	private EmbeddedNeo4jConnection writer;
	private EmbeddedNeo4jConnection reader;


	@BeforeClass public static void setup() throws ClassNotFoundException, SQLException {
		NEO4J_JDBC_IMPERMANENT_URL = "jdbc:neo4j:file:target/test-db";
		EmbeddedNeo4jConnection conn = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		neo4j = conn.getGraphDatabaseService();
	}

	@Before public void setUpBefore() {
		clearDB();
	}

	@After public void clean() throws SQLException {
		clearDB();
	}

	private void clearDB() {
		try(Transaction tx = neo4j.beginTx()) {
			neo4j.execute(StatementData.STATEMENT_CLEAR_DB);
			tx.success();
		}
	}



	@Test public void differentConnectionShouldBeOnSameDatabase() throws SQLException {
		EmbeddedNeo4jConnection conn1 = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		EmbeddedNeo4jConnection conn2 = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);

		assertNotSame(conn1, conn2);
		assertSame(conn1.getGraphDatabaseService(), conn2.getGraphDatabaseService());
	}

	@Test public void commitShouldWorkFineMultiThread() throws SQLException, InterruptedException {

		Thread writerThread = new Thread() {
			public void run() {
				try {
					writer = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
					writer.setAutoCommit(false);
					Statement stmt = writer.createStatement();
					stmt.executeQuery("CREATE (:CommitShouldWorkFine{result:\"ok\"})");
					Thread.sleep(100);
					writer.commit();
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}

			}
		};

		Thread readerThread = new Thread() {
			public void run() {
				try {
					reader = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
					Statement stmt = reader.createStatement();
					ResultSet rs = stmt.executeQuery("MATCH (n:CommitShouldWorkFine) RETURN n.result");
					if (rs.next()) {
						throw new RuntimeException("rsnext1");
					}
					Thread.sleep(400);
					rs = stmt.executeQuery("MATCH (n:CommitShouldWorkFine) RETURN n.result");
					if (!rs.next()) {
						throw new RuntimeException("rsnext1");
					}
					if (!"ok".equals(rs.getString("n.result"))) {
						throw new RuntimeException("equals");
					}
					if (rs.next()) {
						throw new RuntimeException("rs.next2");
					}
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}

			}
		};

		UncaughtExceptionLogger exLog = new UncaughtExceptionLogger();
		readerThread.setUncaughtExceptionHandler(exLog);
		writerThread.setUncaughtExceptionHandler(exLog);

		writerThread.start();
		readerThread.start();
		writerThread.join();
		readerThread.join();

		assertEquals(0, exLog.getExceptions().size());

	}

	@Ignore @Test public void commitShouldWorkFineSameThread() throws SQLException {
		// Connect (autoCommit = false)
		writer = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		writer.setAutoCommit(false);

		reader = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);

		// Creating a node with a transaction
		Statement stmt = writer.createStatement();
		stmt.executeQuery("CREATE (:CommitShouldWorkFine{result:\"ok\"})");

		Statement stmtRead = reader.createStatement();
		ResultSet rs = stmtRead.executeQuery("MATCH (n:CommitShouldWorkFine) RETURN n.result");
		assertFalse(rs.next());
		writer.commit();
		rs = stmtRead.executeQuery("MATCH (n:CommitShouldWorkFine) RETURN n.result");
		assertTrue(rs.next());
		assertEquals("ok", rs.getString("n.result"));
		assertFalse(rs.next());

		writer.close();
		reader.close();
	}

	@Ignore @Test public void setAutoCommitShouldCommitFromFalseToTrueSameThread() throws SQLException {
		// Connect (autoCommit = false)
		writer = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		writer.setAutoCommit(false);
		reader = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);

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

		writer.close();
		reader.close();
	}

	@Ignore @Test public void setAutoCommitShouldWorkAfterMultipleChanges() throws SQLException {
		writer = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		reader = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);

		Statement writerStmt = writer.createStatement();
		writerStmt.executeQuery(StatementData.STATEMENT_CREATE);
		Statement readerStmt = reader.createStatement();
		ResultSet rs = readerStmt.executeQuery(StatementData.STATEMENT_COUNT_NODES);
		//Expect to read data
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));

		//Set autocommit to false
		writer.setAutoCommit(false);
		writerStmt.executeQuery(StatementData.STATEMENT_CREATE);
		rs = readerStmt.executeQuery(StatementData.STATEMENT_COUNT_NODES);
		//Expect not to find new node
		assertTrue(rs.next());
		assertEquals(1, rs.getInt(1));
		writer.commit();
		rs = readerStmt.executeQuery(StatementData.STATEMENT_COUNT_NODES);
		//Expect to find 2 nodes
		assertTrue(rs.next());
		assertEquals(2, rs.getInt(1));

		//Set autocommit to true again
		writer.setAutoCommit(true);
		writerStmt.executeQuery(StatementData.STATEMENT_CREATE);
		rs = readerStmt.executeQuery(StatementData.STATEMENT_COUNT_NODES);
		//Expect to find 3 nodes
		assertTrue(rs.next());
		assertEquals(3, rs.getInt(1));

		writer.close();
		reader.close();

		//TODO neo4j.getGraphDatabase().execute(StatementData.STATEMENT_CREATE_REV);
	}

	@Ignore @Test public void rollbackShouldWorkFineSameThread() throws SQLException {
		// Connect (autoCommit = false)
		writer = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		writer.setAutoCommit(false);
		reader = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		// Creating a node with a transaction
		Statement stmt = writer.createStatement();
		stmt.executeQuery("CREATE (:RollbackShouldWorkFine{result:\"ok\"})");

		Statement stmtRead = reader.createStatement();
		ResultSet rs = stmtRead.executeQuery("MATCH (n:RollbackShouldWorkFine) RETURN n.result");
		assertFalse(rs.next());

		writer.rollback();
		rs = stmtRead.executeQuery("MATCH (n:RollbackShouldWorkFine) RETURN n.result");
		assertFalse(rs.next());
		assertTrue(true);

		writer.close();
		reader.close();
	}

	@Test public void autoCommitShouldWorkFineSameThread() throws SQLException {
		// Connect (autoCommit = true, by default)
		writer = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		reader = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);

		// Creating a node
		Statement writeStatement = writer.createStatement();
		writeStatement.executeQuery("CREATE (:Person)");
		Statement readStatement = reader.createStatement();
		ResultSet rs = readStatement.executeQuery("MATCH (n) RETURN count(n)");
		assertTrue(rs.next());
		assertNotNull(rs.getObject(1));
		assertFalse(rs.next());
	}

	@Ignore @Test public void moreStatementsFromOneConnectionSameThread() throws SQLException {
		writer = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		writer.setAutoCommit(false);
		reader = (EmbeddedNeo4jConnection) DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);

		Statement statOne = writer.createStatement();
		Statement statTwo = writer.createStatement();

		//TODO use executeUpdate
		statOne.executeQuery("CREATE (:User {name:\"username\"})");
		statTwo.executeQuery("CREATE (:Company {name:\"companyname\"})");

		Statement statReader = reader.createStatement();
		ResultSet rs = statReader.executeQuery("MATCH (n) RETURN n.name");

		assertFalse(rs.next());

		writer.commit();
		rs = statReader.executeQuery("MATCH (n) RETURN n.name");

		assertTrue(rs.next());
		assertEquals("username", rs.getString(1));
		assertTrue(rs.next());
		assertEquals("companyname", rs.getString(1));
		assertFalse(rs.next());

		writer.close();
		reader.close();
	}

	@Test public void shouldRollbackAnEmptyTransaction() throws SQLException {
		// Connect (autoCommit = false)
		try (Connection connection = DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL)) {
			connection.setAutoCommit(false);

			connection.rollback();
		}
	}

	/*------------------------------*/
	/*         getMetaData          */
	/*------------------------------*/

	@Ignore @Test public void getMetaDataShouldWork() throws SQLException {
		Connection connection = DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL);
		DatabaseMetaData metaData = connection.getMetaData();
		assertNotNull(metaData);
		ResultSet resultSet = metaData.getColumns(null, null, null, null);
		while (resultSet.next()) {
			System.out.print(resultSet.getString(1) + " | ");
			System.out.print(resultSet.getString(2) + " | ");
			System.out.print(resultSet.getString(3) + " | ");
			System.out.print(resultSet.getString(4) + " | ");
			System.out.print(resultSet.getString(5) + " | ");
			System.out.println();
		}
		connection.close();
	}

	@Test public void killingQueryThreadExecutionShouldNotInvalidateTheConnection() throws SQLException {

		try (Connection connection = DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL)) {
			assertFalse(connection.isClosed());
			assertTrue(connection.isValid(0));

			Thread t = new Thread() {
				public void run() {
					try (Statement statement = connection.createStatement()) {
						statement.executeQuery(
								"WITH ['Michael','Stefan','Alberto','Marco','Gianmarco','Benoit','Frank'] AS names FOREACH (r IN range(0,10000000) | CREATE (:User {id:r, name:names[r % size(names)]+' '+r}));");
					} catch (SQLException sqle) {
					}
				}
			};

			t.start();
			t.interrupt();
			while (t.isAlive()) {
			}

			assertFalse(connection.isClosed());
			assertTrue(connection.isValid(0));

			try (Statement statement = connection.createStatement()) {
				try (ResultSet resultSet = statement.executeQuery("RETURN 1")) {
					assertTrue(resultSet.next());
					assertEquals(1, resultSet.getLong(1));
				}
			}
		}
	}

	@Test public void multipleRunShouldNotFail() throws SQLException {

		for (int i = 0; i < 1000; i++) {
			try (Connection connection = DriverManager.getConnection(NEO4J_JDBC_IMPERMANENT_URL)) {
				try (Statement statement = connection.createStatement()){
					ResultSet resultSet = statement.executeQuery("match (n) return count(n) as countOfNodes");
					if (resultSet.next()) {
						resultSet.getObject("countOfNodes");
					}
				}
			} catch (Exception e) {
				fail(e.getMessage());
			}
		}
	}
}
