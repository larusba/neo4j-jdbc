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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jdbc.data.StatementData;
import org.neo4j.jdbc.embedded.EmbeddedNeo4jConnection;
import org.neo4j.jdbc.embedded.EmbeddedNeo4jPreparedStatement;

import java.sql.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.jdbc.embedded.test.util.Mocker.*;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class EmbeddedNeo4JConnectionTest {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private EmbeddedNeo4jConnection openConnection;
	private EmbeddedNeo4jConnection closedConnection;
	private EmbeddedNeo4jConnection slowOpenConnection;
	private EmbeddedNeo4jConnection exceptionOpenConnection;

	@Before public void tearUp() throws SQLException {
		openConnection = new EmbeddedNeo4jConnection(mockAvailableGraphDB());
		closedConnection = mockConnectionClosed();
		slowOpenConnection = new EmbeddedNeo4jConnection(mockSlowGraphDB());
		exceptionOpenConnection = new EmbeddedNeo4jConnection(mockExceptionExecutionGraphDB());
	}

	/*------------------------------*/
	/*        setHoldability        */
	/*------------------------------*/

	@Test public void setHoldabilityShouldSetTheHoldability() throws SQLException {
		openConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
		assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, openConnection.getHoldability());
		openConnection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, openConnection.getHoldability());
	}

	@Test public void setHoldabilityShouldThrowExceptionIfHoldabilityIsNotSupported() throws SQLException {
		expectedEx.expect(SQLFeatureNotSupportedException.class);
		openConnection.setHoldability(999);
	}

	@Test public void setHoldabilityShouldThrowExceptionWhenCalledOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	/*------------------------------*/
	/*        getHoldability        */
	/*------------------------------*/

	@Test public void getHoldabilityShouldThrowExceptionWhenCalledOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.getHoldability();
	}

	/*------------------------------*/
	/*           isClosed           */
	/*------------------------------*/
	@Test public void isClosedShouldReturnFalse() throws SQLException {
		assertFalse(openConnection.isClosed());
	}

	@Test public void isClosedShouldReturnTrue() throws SQLException {
		assertTrue(closedConnection.isClosed());
	}

	/*------------------------------*/
	/*             close            */
	/*------------------------------*/
	@Test public void closeShouldCloseConnection() throws SQLException {
		GraphDatabaseService db = mockAvailableGraphDB();
		Connection connection = new EmbeddedNeo4jConnection(db);
		assertFalse(connection.isClosed());
		connection.close();
		assertTrue(connection.isClosed());
	}

	@Test public void closeShouldNotThrowExceptionWhenClosingAClosedConnection() throws SQLException {
		Connection connection = mockConnectionClosed();
		assertTrue(connection.isClosed());
		connection.close();
		assertTrue(connection.isClosed());
	}

	/*------------------------------*/
	/*          isReadOnly          */
	/*------------------------------*/
	@Test public void isReadOnlyShouldReturnFalse() throws SQLException {
		assertFalse(openConnection.isReadOnly());
	}

	@Test public void isReadOnlyShouldReturnTrue() throws SQLException {
		openConnection.setReadOnly(true);
		assertTrue(openConnection.isReadOnly());
	}

	@Test public void isReadOnlyShouldThrowExceptionWhenCalledOnAClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.isReadOnly();
	}

	/*------------------------------*/
	/*         setReadOnly          */
	/*------------------------------*/
	@Test public void setReadOnlyShouldSetReadOnlyTrue() throws SQLException {
		assertFalse(openConnection.isReadOnly());
		openConnection.setReadOnly(true);
		assertTrue(openConnection.isReadOnly());
	}

	@Test public void setReadOnlyShouldSetReadOnlyFalse() throws SQLException {
		assertFalse(openConnection.isReadOnly());
		openConnection.setReadOnly(false);
		assertFalse(openConnection.isReadOnly());
	}

	@Test public void setReadOnlyShouldSetReadOnlyFalseAfterSetItTrue() throws SQLException {
		assertFalse(openConnection.isReadOnly());
		openConnection.setReadOnly(true);
		assertTrue(openConnection.isReadOnly());
		openConnection.setReadOnly(false);
		assertFalse(openConnection.isReadOnly());
	}

	@Test public void setReadOnlyShouldThrowExceptionIfCalledOnAClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.setReadOnly(true);
	}

	/*------------------------------*/
	/*        createStatement       */
	/*------------------------------*/

	@Test public void createStatementNoParamsShouldReturnNewStatement() throws SQLException {
		Statement statement = openConnection.createStatement();

		//assertTrue(statement instanceof BoltNeo4jStatement);
		assertEquals(openConnection.getHoldability(), statement.getResultSetHoldability());
		assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
		assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
	}

	@Test public void createStatementNoParamsShouldThrowExceptionOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.createStatement();
	}

	@Test public void createStatementTwoParamsShouldReturnNewStatement() throws SQLException {
		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };

		for (int type : types) {
			Statement statement = openConnection.createStatement(type, concurrencies[0]);
			assertEquals(type, statement.getResultSetType());
		}

		for (int concurrency : concurrencies) {
			Statement statement = openConnection.createStatement(types[0], concurrency);
			assertEquals(concurrency, statement.getResultSetConcurrency());
		}
	}

	@Test public void createStatementTwoParamsShouldThrowExceptionOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	//Must be managed only the types specified in the createStatementTwoParamsShouldReturnNewStatement test
	//This test doesn't cover all integers different from supported ones
	@Test public void createStatementTwoParamsShouldThrowExceptionOnWrongParams() throws SQLException {

		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };

		for (int type : types) {
			try {
				openConnection.createStatement(concurrencies[0], type);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}

		for (int concurrency : concurrencies) {
			try {
				openConnection.createStatement(concurrency, types[0]);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}
	}

	@Test public void createStatementThreeParamsShouldReturnNewStatement() throws SQLException {
		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };
		int[] holdabilities = { ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT };

		for (int type : types) {
			Statement statement = openConnection.createStatement(type, concurrencies[0], holdabilities[0]);
			assertEquals(type, statement.getResultSetType());
		}

		for (int concurrency : concurrencies) {
			Statement statement = openConnection.createStatement(types[0], concurrency, holdabilities[0]);
			assertEquals(concurrency, statement.getResultSetConcurrency());
		}

		for (int holdability : holdabilities) {
			Statement statement = openConnection.createStatement(types[0], concurrencies[0], holdability);
			assertEquals(holdability, statement.getResultSetHoldability());
		}
	}

	@Test public void createStatementThreeParamsShouldThrowExceptionOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	//Must be managed only the types specified in the createStatementThreeParamsShouldReturnNewStatement test
	//This test doesn't cover all integers different from supported ones
	@Test public void createStatementThreeParamsShouldThrowExceptionOnWrongParams() throws SQLException {
		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };
		int[] holdabilities = { ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT };

		for (int type : types) {
			try {
				openConnection.createStatement(concurrencies[0], holdabilities[0], type);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}

		for (int concurrency : concurrencies) {
			try {
				openConnection.createStatement(concurrency, holdabilities[0], types[0]);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}

		for (int holdability : holdabilities) {
			try {
				openConnection.createStatement(concurrencies[0], holdability, types[0]);
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}
	}

	/*------------------------------*/
	/*        prepareStatement       */
	/*------------------------------*/

	@Test public void prepareStatementNoParamsShouldReturnNewStatement() throws SQLException {
		Statement statement = openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC);

		assertTrue(statement instanceof EmbeddedNeo4jPreparedStatement);
		assertEquals(openConnection.getHoldability(), statement.getResultSetHoldability());
		assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
		assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
	}

	@Test public void prepareStatementNoParamsShouldThrowExceptionOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC);
	}

	@Test public void prepareStatementTwoParamsShouldReturnNewStatement() throws SQLException {
		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };

		for (int type : types) {
			Statement statement = openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, type, concurrencies[0]);
			assertEquals(type, statement.getResultSetType());
		}

		for (int concurrency : concurrencies) {
			Statement statement = openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, types[0], concurrency);
			assertEquals(concurrency, statement.getResultSetConcurrency());
		}
	}

	@Test public void prepareStatementTwoParamsShouldThrowExceptionOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	//Must be managed only the types specified in the prepareStatementTwoParamsShouldReturnNewStatement test
	//This test doesn't cover all integers different from supported ones
	@Test public void prepareStatementTwoParamsShouldThrowExceptionOnWrongParams() throws SQLException {

		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };

		for (int type : types) {
			try {
				openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, concurrencies[0], type);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}

		for (int concurrency : concurrencies) {
			try {
				openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, concurrency, types[0]);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}
	}

	@Test public void prepareStatementThreeParamsShouldReturnNewStatement() throws SQLException {
		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };
		int[] holdabilities = { ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT };

		for (int type : types) {
			Statement statement = openConnection
					.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, type, concurrencies[0], holdabilities[0]);
			assertEquals(type, statement.getResultSetType());
		}

		for (int concurrency : concurrencies) {
			Statement statement = openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, types[0], concurrency, holdabilities[0]);
			assertEquals(concurrency, statement.getResultSetConcurrency());
		}

		for (int holdability : holdabilities) {
			Statement statement = openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, types[0], concurrencies[0], holdability);
			assertEquals(holdability, statement.getResultSetHoldability());
		}
	}

	@Test public void prepareStatementThreeParamsShouldThrowExceptionOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
				ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	//Must be managed only the types specified in the prepareStatementThreeParamsShouldReturnNewStatement test
	//This test doesn't cover all integers different from supported ones
	@Test public void prepareStatementThreeParamsShouldThrowExceptionOnWrongParams() throws SQLException {
		int[] types = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
		int[] concurrencies = { ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY };
		int[] holdabilities = { ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT };

		for (int type : types) {
			try {
				openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, concurrencies[0], holdabilities[0], type);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}

		for (int concurrency : concurrencies) {
			try {
				openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, concurrency, holdabilities[0], types[0]);
				fail();
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}

		for (int holdability : holdabilities) {
			try {
				openConnection.prepareStatement(StatementData.STATEMENT_MATCH_ALL_STRING_PARAMETRIC, concurrencies[0], holdability, types[0]);
			} catch (SQLFeatureNotSupportedException e) {
				//Expected Exception
			} catch (Exception e) {
				fail();
			}
		}
	}

	//TODO needs IT tests checking initialization succeeded

	/*------------------------------*/
	/*         setAutocommit        */
	/*------------------------------*/

	//Check what cases of exception can occur
	@Test public void setAutoCommitShouldThrowExceptionWhenDatabaseAccessErrorOccurred() throws SQLException {
		expectedEx.expect(SQLException.class);

		new EmbeddedNeo4jConnection(mockExceptionGraphDB()).setAutoCommit(false);
	}

	@Test public void setAutoCommitShouldThrowExceptionWhenCalledOnClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);

		closedConnection.setAutoCommit(true);
	}

	@Test public void setAutoCommitShouldSetWhatIsPassed() throws SQLException {
		openConnection.setAutoCommit(false);
		assertFalse(openConnection.getAutoCommit());
		openConnection.setAutoCommit(true);
		assertTrue(openConnection.getAutoCommit());
	}

	@Test public void setAutoCommitShouldCommit() throws SQLException {
		openConnection.setAutoCommit(true);
		verify(openConnection.getGraphDatabaseService(), times(0)).beginTx();

		openConnection.setAutoCommit(false);
		verify(openConnection.getGraphDatabaseService(), times(1)).beginTx();

		openConnection.setAutoCommit(false);
		verify(openConnection.getGraphDatabaseService(), times(1)).beginTx();

		openConnection.setAutoCommit(true);
		verify(openConnection.getGraphDatabaseService(), times(2)).beginTx();

		openConnection.setAutoCommit(true);
		verify(openConnection.getGraphDatabaseService(), times(2)).beginTx();

		openConnection.setAutoCommit(false);
		verify(openConnection.getGraphDatabaseService(), times(3)).beginTx();
	}

	/*------------------------------*/
	/*         getAutoCommit        */
	/*------------------------------*/

	@Test public void getAutoCommitShouldThrowExceptionIfConnectionIsClosed() throws SQLException {
		expectedEx.expect(SQLException.class);

		closedConnection.getAutoCommit();
	}

	@Test public void getAutoCommitShouldReturnTrueByDefault() throws SQLException {
		assertTrue(openConnection.getAutoCommit());
	}

	@Test public void getAutoCommitShouldReturnFalse() throws SQLException {
		openConnection.setAutoCommit(false);
		assertFalse(openConnection.getAutoCommit());
	}

	@Test public void getAutoCommitShouldReturnTrue() throws SQLException {
		openConnection.setAutoCommit(false);
		openConnection.setAutoCommit(true);
		assertTrue(openConnection.getAutoCommit());
	}

	/*------------------------------*/
	/*            commit            */
	/*------------------------------*/

	@Test public void commitShouldThrowExceptionIfConnectionIsClosed() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.commit();
	}

	@Test public void commitShouldThrowExceptionIfInAutoCommitIsTrue() throws SQLException {
		expectedEx.expect(SQLException.class);
		openConnection.setAutoCommit(true);
		openConnection.commit();
	}

	/*------------------------------*/
	/*           rollback           */
	/*------------------------------*/

	@Test public void rollbackShouldThrowExceptionIfConnectionIsClosed() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.rollback();
	}

	@Test public void rollbackShouldThrowExceptionIfInAutoCommitIsTrue() throws SQLException {
		expectedEx.expect(SQLException.class);
		openConnection.setAutoCommit(true);
		openConnection.rollback();
	}

	/*------------------------------*/
	/*   getTransactionIsolation    */
	/*------------------------------*/

	@Test public void getTransactionIsolationShouldThrowExceptionIfConnectionIsClosed() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.getTransactionIsolation();
	}

	@Test public void getTransactionIsolationShouldReturnTransactionReadCommitted() throws SQLException {
		assertEquals(Connection.TRANSACTION_READ_COMMITTED, openConnection.getTransactionIsolation());
	}

	/*------------------------------*/
	/*         setCatalog           */
	/*------------------------------*/

	@Test public void setCatalogShouldSilentlyIgnoreTheRequest() throws SQLException {
		openConnection.setCatalog("catalog");
		assertNull(openConnection.getCatalog());
	}

	@Test public void setCatalogShouldThrowExceptionWhenConnectionClosed() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.setCatalog("catalog");
	}

	/*------------------------------*/
	/*         getCatalog           */
	/*------------------------------*/

	@Test public void getCatalogShouldReturnNull() throws SQLException {
		assertNull(openConnection.getCatalog());
	}

	@Test public void getCatalogShouldThrowExceptionWhenConnectionClosed() throws SQLException {
		expectedEx.expect(SQLException.class);
		closedConnection.getCatalog();
	}

	/*-------------------------------*/
	/*            isValid            */
	/*-------------------------------*/
	@Test public void isValidShouldThrowExceptionWhenTimeoutSuppliedLessThanZero() throws SQLException {
		expectedEx.expect(SQLException.class);
		openConnection.isValid(-2);
	}

	@Test public void isValidShouldReturnFalseIfCalledOnClosedConnection() throws SQLException {
		assertFalse(closedConnection.isValid(0));
	}

	@Test(timeout = 1500) public void isValidShouldReturnFalseIfTimeout() throws SQLException {
		assertFalse(slowOpenConnection.isValid(1));
	}

	@Test(timeout = 5000) public void isValidShouldReturnTrueIfNotTimeout() throws SQLException {
		assertTrue(openConnection.isValid(4));
	}

	@Test public void isValidShouldReturnFalseIfSessionException() throws SQLException {
		assertFalse(exceptionOpenConnection.isValid(5));
		assertFalse(exceptionOpenConnection.isValid(0));
	}

	@Test public void isValidShouldReturnTrueWithoutATimeoutValue() throws SQLException {
		assertTrue(openConnection.isValid(0));
	}

}
