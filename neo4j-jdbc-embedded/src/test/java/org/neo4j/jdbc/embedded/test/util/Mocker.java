/**
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
 * Created on 11/12/17
 */
package org.neo4j.jdbc.embedded.test.util;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jdbc.embedded.EmbeddedNeo4jConnection;
import org.neo4j.jdbc.embedded.util.ResultWrapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class Mocker {
	/**
	 * Create a mocked embedded graph database that simulate an open connection.
	 *
	 * @return A mocked embedded database
	 */
	public static GraphDatabaseService mockAvailableGraphDB() {
		GraphDatabaseService db = mock(GraphDatabaseService.class);
		when(db.isAvailable(anyLong())).thenReturn(true);
		when(db.beginTx()).thenReturn(mock(Transaction.class));
		return db;
	}

	/**
	 * Create a mocked embedded graph database that simulate a closed connection.
	 *
	 * @return A mocked embedded database
	 */
	public static GraphDatabaseService mockUnavailableGraphDB() {
		GraphDatabaseService db = mock(GraphDatabaseService.class);
		when(db.isAvailable(anyLong())).thenReturn(false);
		return db;
	}

	/**
	 * Create a mocked embedded graph database that simulate a errors when calling execute method.
	 *
	 * @return A mocked embedded database
	 */
	public static GraphDatabaseService mockExceptionExecutionGraphDB() {
		GraphDatabaseService db = mock(GraphDatabaseService.class);
		when(db.isAvailable(anyLong())).thenReturn(true);
		when(db.execute(anyString())).thenThrow(new RuntimeException());
		doThrow(new RuntimeException()).when(db).shutdown();
		return db;
	}

	/**
	 * Create a mocked embedded graph database that simulate a errors when calling execute method.
	 *
	 * @return A mocked embedded database
	 */
	public static GraphDatabaseService mockExceptionGraphDB() {
		GraphDatabaseService db = mock(GraphDatabaseService.class, new Answer() {
			@Override public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				throw new RuntimeException();
			}
		});
		doReturn(true).when(db).isAvailable(anyLong());
		return db;
	}

	/**
	 * Create a mocked embedded graph database that simulate a slow connection.
	 *
	 * @return A mocked embedded database
	 */
	public static GraphDatabaseService mockSlowGraphDB() {
		GraphDatabaseService db = mock(GraphDatabaseService.class);
		when(db.isAvailable(anyLong())).thenReturn(true);
		Transaction transaction = mock(Transaction.class);
		when(db.beginTx()).thenReturn(transaction);
		when(db.execute(anyString())).thenAnswer(new Answer<ResultSet>() {
			@Override public ResultSet answer(InvocationOnMock invocation) {
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException e) {
				}
				return null;
			}
		});
		return db;
	}

	public static EmbeddedNeo4jConnection mockConnectionOpen() throws SQLException {
		EmbeddedNeo4jConnection mockConnection = mock(EmbeddedNeo4jConnection.class);
		when(mockConnection.isClosed()).thenReturn(false);
		return mockConnection;
	}

	public static EmbeddedNeo4jConnection mockConnectionClosed() throws SQLException {
		EmbeddedNeo4jConnection closedConnection = new EmbeddedNeo4jConnection(mockUnavailableGraphDB());
		EmbeddedNeo4jConnection mockConnection = spy(closedConnection);
		doReturn(true).when(mockConnection).isClosed();
		return mockConnection;
	}

	public static EmbeddedNeo4jConnection mockConnectionOpenWithTransactionThatReturns(Result cur) throws SQLException {
		GraphDatabaseService graphDatabaseService = mock(GraphDatabaseService.class);
		when(graphDatabaseService.execute(anyString())).thenReturn(cur);

		EmbeddedNeo4jConnection mockConnection = mockConnectionOpen();
		when(mockConnection.getGraphDatabaseService()).thenReturn(graphDatabaseService);
		return mockConnection;
	}

	public static EmbeddedNeo4jConnection mockConnectionWithExecuteThatReturns(ResultWrapper rw) throws SQLException {
		EmbeddedNeo4jConnection mockConnection = mockConnectionOpen();
		when(mockConnection.execute(anyString())).thenReturn(rw);
		return mockConnection;
	}

	public static GraphDatabaseService mockGraphDB() {
		return mock(GraphDatabaseService.class);
	}
}
