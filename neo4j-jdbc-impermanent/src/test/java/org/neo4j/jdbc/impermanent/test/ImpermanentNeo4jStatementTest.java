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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jdbc.Neo4jConnection;
import org.neo4j.jdbc.data.StatementData;
import org.neo4j.jdbc.impermanent.ImpermanentNeo4jConnection;
import org.neo4j.jdbc.impermanent.ImpermanentNeo4jResultSet;
import org.neo4j.jdbc.impermanent.ImpermanentNeo4jStatement;
import org.neo4j.jdbc.impermanent.util.ResultWrapper;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;
import static org.neo4j.jdbc.impermanent.test.util.Mocker.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ImpermanentNeo4jStatement.class, ImpermanentNeo4jResultSet.class })
public class ImpermanentNeo4jStatementTest {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private ImpermanentNeo4jResultSet mockedRS;

	@Before public void interceptBoltResultSetConstructor() throws Exception {
		mockedRS = mock(ImpermanentNeo4jResultSet.class);
		doNothing().when(mockedRS).close();
		whenNew(ImpermanentNeo4jResultSet.class).withArguments(anyObject(), anyObject()).thenReturn(mockedRS);
	}

	/*------------------------------*/
	/*             close            */
	/*------------------------------*/
	@Test public void closeShouldCloseExistingResultSet() throws Exception {

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpenWithTransactionThatReturns(null));
		statement.executeQuery(StatementData.STATEMENT_MATCH_ALL);
		statement.close();

		verify(mockedRS, times(1)).close();
	}

	@Test public void closeShouldNotCallCloseOnAnyResultSet() throws Exception {

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpenWithTransactionThatReturns(null));
		statement.close();

		verify(mockedRS, never()).close();
	}

	@Test public void closeMultipleTimesIsNOOP() throws Exception {

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpenWithTransactionThatReturns(null));
		statement.executeQuery(StatementData.STATEMENT_MATCH_ALL);
		statement.close();
		statement.close();
		statement.close();

		verify(mockedRS, times(1)).close();
	}

	@Test public void closeShouldNotTouchTheTransaction() throws Exception {

		Transaction mockTransaction = mock(Transaction.class);
		GraphDatabaseService mockGraphDatabaseService = mock(GraphDatabaseService.class);
		when(mockGraphDatabaseService.beginTx()).thenReturn(mockTransaction);

		ImpermanentNeo4jConnection mockConnection = mockConnectionOpen();
		when(mockConnection.getGraphDatabaseService()).thenReturn(mockGraphDatabaseService);

		Statement statement = new ImpermanentNeo4jStatement(mockConnection);

		statement.close();

		verify(mockTransaction, never()).failure();
		verify(mockTransaction, never()).success();
		verify(mockTransaction, never()).close();
	}

	/*------------------------------*/
	/*           isClosed           */
	/*------------------------------*/

	@Test public void isClosedShouldReturnFalseWhenCreated() throws SQLException {
		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpen());
		assertFalse(statement.isClosed());
	}

	/*------------------------------*/
	/*          executeQuery        */
	/*------------------------------*/

	@Test public void executeQueryShouldRun() throws SQLException {
		Result mockResult = mock(Result.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpenWithTransactionThatReturns(mockResult), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
		statement.executeQuery(StatementData.STATEMENT_CREATE);
	}

	@Test public void executeQueryShouldThrowExceptionWhenClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionClosed(), 0, 0, 0);
		statement.executeQuery(StatementData.STATEMENT_MATCH_ALL);
	}

	@Test public void executeQueryShouldReturnCorrectResultSetStructureConnectionNotAutocommit() throws Exception {
		ImpermanentNeo4jConnection mockConnection = mockConnectionOpenWithTransactionThatReturns(null);

		Statement statement = new ImpermanentNeo4jStatement(mockConnection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
				ResultSet.HOLD_CURSORS_OVER_COMMIT);
		statement.executeQuery(StatementData.STATEMENT_MATCH_ALL);

		verifyNew(ImpermanentNeo4jResultSet.class)
				.withArguments(eq(statement), any(ResultWrapper.class), eq(ResultSet.TYPE_FORWARD_ONLY), eq(ResultSet.CONCUR_READ_ONLY),
						eq(ResultSet.HOLD_CURSORS_OVER_COMMIT));
	}

	@Test public void executeQueryShouldThrowExceptionOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpen());
		statement.close();

		statement.executeQuery(StatementData.STATEMENT_MATCH_ALL);
	}

	@Test public void executeQueryShouldThrowExceptionOnPreparedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.prepareStatement("");
		statement.executeQuery(StatementData.STATEMENT_MATCH_ALL);
	}

	@Ignore @Test public void executeQueryShouldThrowExceptionOnCallableStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.prepareCall("");
		statement.executeQuery(StatementData.STATEMENT_MATCH_ALL);
	}

	/*------------------------------*/
	/*         executeUpdate        */
	/*------------------------------*/

	@Test public void executeUpdateShouldRun() throws SQLException {
		ResultWrapper mockResult = mock(ResultWrapper.class);
		QueryStatistics stats = mock(QueryStatistics.class);

		when(mockResult.getQueryStatistics()).thenReturn(stats);
		when(stats.getNodesCreated()).thenReturn(1);
		when(stats.getNodesDeleted()).thenReturn(0);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionWithExecuteThatReturns(mockResult), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
		statement.executeUpdate(StatementData.STATEMENT_CREATE);
	}

	@Test public void executeUpdateShouldThrowExceptionOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.createStatement();
		statement.close();
		statement.executeUpdate(StatementData.STATEMENT_CREATE);
	}

	@Test public void executeUpdateShouldThrowExceptionOnPreparedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.prepareStatement("");
		statement.executeUpdate(StatementData.STATEMENT_CREATE);
	}

	@Ignore @Test public void executeUpdateShouldThrowExceptionOnCallableStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.prepareCall(null);
		statement.executeUpdate(StatementData.STATEMENT_CREATE);
	}

	/*------------------------------*/
	/*    getResultSetConcurrency   */
	/*------------------------------*/
	@Test public void getResultSetConcurrencyShouldThrowExceptionWhenCalledOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionClosed());
		statement.close();
		statement.getResultSetConcurrency();
	}

	/*------------------------------*/
	/*    getResultSetHoldability   */
	/*------------------------------*/
	@Test public void getResultSetHoldabilityShouldThrowExceptionWhenCalledOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionClosed());
		statement.close();
		statement.getResultSetHoldability();
	}

	/*------------------------------*/
	/*       getResultSetType       */
	/*------------------------------*/
	@Test public void getResultSetTypeShouldThrowExceptionWhenCalledOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionClosed());
		statement.close();
		statement.getResultSetType();
	}

	/*------------------------------*/
	/*            execute           */
	/*------------------------------*/
	@Test public void executeShouldRunQuery() throws SQLException {
		Result mockResult = mock(Result.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpenWithTransactionThatReturns(mockResult), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
		statement.execute(StatementData.STATEMENT_MATCH_ALL);
	}

	@Test public void executeShouldRunUpdate() throws SQLException {
		ResultWrapper mockResult = mock(ResultWrapper.class);
		QueryStatistics stats = mock(QueryStatistics.class);

		when(mockResult.getQueryStatistics()).thenReturn(stats);
		when(stats.getNodesCreated()).thenReturn(1);
		when(stats.getNodesDeleted()).thenReturn(0);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionWithExecuteThatReturns(mockResult), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
		statement.execute(StatementData.STATEMENT_CREATE);
	}

	@Test public void executeShouldThrowExceptionOnQueryWhenClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionClosed(), 0, 0, 0);
		statement.execute(StatementData.STATEMENT_MATCH_ALL);
	}

	@Test public void executeShouldThrowExceptionOnUpdateWhenClosedConnection() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionClosed(), 0, 0, 0);
		statement.execute(StatementData.STATEMENT_CREATE);
	}

	@Test public void executeShouldThrowExceptionOnQueryOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpen());
		statement.close();

		statement.execute(StatementData.STATEMENT_MATCH_ALL);
	}

	@Test public void executeShouldThrowExceptionOnUpdateOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpen());
		statement.close();

		statement.execute(StatementData.STATEMENT_CREATE);
	}

	@Test public void executeShouldThrowExceptionOnQueryOnPreparedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.prepareStatement("");

		statement.execute(StatementData.STATEMENT_MATCH_ALL);
	}

	@Test public void executeShouldThrowExceptionOnUpdateOnPreparedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.prepareStatement("");
		when(statement.executeUpdate(anyString())).thenCallRealMethod();

		statement.execute(StatementData.STATEMENT_CREATE);
	}

	@Ignore @Test public void executeShouldThrowExceptionOnCallableStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jConnection connection = new ImpermanentNeo4jConnection(mockGraphDB());
		Statement statement = connection.prepareCall("");
		statement.execute(StatementData.STATEMENT_MATCH_ALL);
	}

	/*------------------------------*/
	/*        getUpdateCount        */
	/*------------------------------*/
	@Test public void getUpdateCountShouldReturnOne() throws SQLException {
		ImpermanentNeo4jStatement statement = mock(ImpermanentNeo4jStatement.class);
		when(statement.isClosed()).thenReturn(false);
		when(statement.getUpdateCount()).thenCallRealMethod();
		Whitebox.setInternalState(statement, "currentResultSet", null);
		Whitebox.setInternalState(statement, "currentUpdateCount", 1);

		assertEquals(1, statement.getUpdateCount());
	}

	@Test public void getUpdateCountShouldReturnMinusOne() throws SQLException {
		ImpermanentNeo4jStatement statement = mock(ImpermanentNeo4jStatement.class);
		when(statement.isClosed()).thenReturn(false);
		when(statement.getUpdateCount()).thenCallRealMethod();
		Whitebox.setInternalState(statement, "currentResultSet", mock(ImpermanentNeo4jResultSet.class));

		assertEquals(-1, statement.getUpdateCount());
	}

	@Test public void getUpdateCountShouldThrowExceptionOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpen());
		statement.close();

		statement.getUpdateCount();
	}

	/*------------------------------*/
	/*         getResultSet         */
	/*------------------------------*/
	@Test public void getResultSetShouldNotReturnNull() throws SQLException {
		ImpermanentNeo4jStatement statement = mock(ImpermanentNeo4jStatement.class);
		when(statement.isClosed()).thenReturn(false);
		when(statement.getResultSet()).thenCallRealMethod();
		Whitebox.setInternalState(statement, "currentResultSet", mock(ImpermanentNeo4jResultSet.class));
		Whitebox.setInternalState(statement, "currentUpdateCount", -1);

		assertTrue(statement.getResultSet() != null);
	}

	@Test public void getResultSetShouldReturnNull() throws SQLException {
		ImpermanentNeo4jStatement statement = mock(ImpermanentNeo4jStatement.class);
		when(statement.isClosed()).thenReturn(false);
		when(statement.getResultSet()).thenCallRealMethod();
		Whitebox.setInternalState(statement, "currentUpdateCount", 1);

		assertEquals(null, statement.getResultSet());
	}

	@Test public void getResultSetShouldThrowExceptionOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement statement = new ImpermanentNeo4jStatement(mockConnectionOpen());
		statement.close();

		statement.getResultSet();
	}


	/*------------------------------*/
	/*           addBatch           */
	/*------------------------------*/

	@Test public void addBatchShouldAddStringToStack() throws SQLException {
		Statement stmt = new ImpermanentNeo4jStatement(mockConnectionOpen());
		String str1 = "MATCH n WHERE id(n) = 1 SET n.property=1";
		String str2 = "MATCH n WHERE id(n) = 2 SET n.property=2";
		stmt.addBatch(str1);
		stmt.addBatch(str2);

		assertEquals(Arrays.asList(str1, str2), Whitebox.getInternalState(stmt, "batchStatements"));
	}

	@Test public void addBatchShouldThrowExceptionIfClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement stmt = new ImpermanentNeo4jStatement(mockConnectionOpen());
		stmt.close();
		stmt.addBatch("");
	}

	/*------------------------------*/
	/*          clearBatch          */
	/*------------------------------*/

	@Test public void clearBatchShouldWork() throws SQLException {
		Statement stmt = new ImpermanentNeo4jStatement(mockConnectionOpen());
		stmt.addBatch("MATCH n WHERE id(n) = 1 SET n.property=1");
		stmt.addBatch("MATCH n WHERE id(n) = 2 SET n.property=2");
		stmt.clearBatch();

		assertEquals(Collections.EMPTY_LIST, Whitebox.getInternalState(stmt, "batchStatements"));
	}

	@Test public void clearBatchShouldThrowExceptionIfClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement stmt = new ImpermanentNeo4jStatement(mockConnectionOpen());
		stmt.close();
		stmt.clearBatch();
	}

	/*------------------------------*/
	/*         getConnection        */
	/*------------------------------*/

	@Test public void getConnectionShouldWork() throws SQLException {
		Statement stmt = new ImpermanentNeo4jStatement(mockConnectionOpen());

		assertNotNull(stmt.getConnection());
		assertEquals(mockConnectionOpen().getClass(), stmt.getConnection().getClass());
	}

	@Test public void getConnectionShouldThrowExceptionOnClosedStatement() throws SQLException {
		expectedEx.expect(SQLException.class);

		Statement stmt = new ImpermanentNeo4jStatement(mockConnectionClosed());
		stmt.close();

		stmt.getConnection();
	}
}
