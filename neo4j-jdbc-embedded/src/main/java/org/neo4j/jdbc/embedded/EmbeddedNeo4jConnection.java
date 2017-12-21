package org.neo4j.jdbc.embedded; /**
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jdbc.Neo4jConnection;
import org.neo4j.jdbc.Neo4jResultSet;
import org.neo4j.jdbc.Neo4jStatement;
import org.neo4j.jdbc.embedded.util.ResultWrapper;
import org.neo4j.jdbc.utils.ExceptionBuilder;
import org.neo4j.jdbc.utils.TimeLimitedCodeBlock;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class EmbeddedNeo4jConnection extends Neo4jConnection {

	private GraphDatabaseService graphDatabaseService;
//	private Transaction          transaction;
	private boolean closed = false;
	private ThreadLocal<Transaction> txs = new ThreadLocal<>();

	/**
	 * Default constructor with properties.
	 *
	 * @param properties driver properties
	 * @param url        connection url
	 */

	public EmbeddedNeo4jConnection(GraphDatabaseService db, Properties properties, String url) {
		super(properties, url, Neo4jResultSet.DEFAULT_HOLDABILITY);
		this.graphDatabaseService = db;
	}

	/**
	 * Constructor with Session.
	 *
	 * @param db Impermanent database
	 */
	public EmbeddedNeo4jConnection(GraphDatabaseService db) {
		this(db, new Properties(), "");
	}

	@Override public Statement createStatement() throws SQLException {
		return createStatement(Neo4jResultSet.TYPE_FORWARD_ONLY, Neo4jResultSet.CONCUR_READ_ONLY, Neo4jResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override public Neo4jStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return createStatement(resultSetType, resultSetConcurrency, Neo4jResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override public Neo4jStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		checkClosed();
		//		if (this.transaction == null && !this.autoCommit) {
		//			this.transaction = this.graphDatabaseService.beginTx();
		//		}
		this.checkTypeParams(resultSetType);
		this.checkConcurrencyParams(resultSetConcurrency);
		this.checkHoldabilityParams(resultSetHoldability);
		return new EmbeddedNeo4jStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override public PreparedStatement prepareStatement(String sql) throws SQLException {
		return prepareStatement(sql, Neo4jResultSet.TYPE_FORWARD_ONLY, Neo4jResultSet.CONCUR_READ_ONLY, Neo4jResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return prepareStatement(sql, resultSetType, resultSetConcurrency, Neo4jResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		this.checkClosed();
		this.checkTypeParams(resultSetType);
		this.checkConcurrencyParams(resultSetConcurrency);
		this.checkHoldabilityParams(resultSetHoldability);
		return new EmbeddedNeo4jPreparedStatement(this, nativeSQL(sql), resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override public void setAutoCommit(boolean autoCommit) throws SQLException {
		this.checkClosed();
		try {
			if (this.autoCommit != autoCommit) {
				Transaction t = txs.get();
				if (/*this.transaction */ t != null && !this.autoCommit) {
					this.commit();
					/*this.transaction*/t.close();
				}

				if (this.autoCommit) {
					//this.transaction = this.graphDatabaseService.beginTx();
					this.txs.set(this.graphDatabaseService.beginTx());
				}

				this.autoCommit = autoCommit;
			}
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override public void commit() throws SQLException {
		this.checkClosed();
		this.checkAutoCommit();
		Transaction t = txs.get();
		if (/*this.transaction*/t == null) {
			throw new SQLException("The transaction is null");
		}
		/*this.transaction.success();
		this.transaction.close();
		this.transaction = this.graphDatabaseService.beginTx();*/
		t.success();
		t.close();
		txs.set(this.graphDatabaseService.beginTx());
	}

	@Override public void rollback() throws SQLException {
		this.checkClosed();
		this.checkAutoCommit();
		Transaction t = txs.get();
		if (/*this.transaction*/t == null) {
			throw new SQLException("The transaction is null");
		}
//		this.transaction.failure();
		t.failure();
		t.close();
		txs.set(this.graphDatabaseService.beginTx());
	}

	@Override public void close() throws SQLException {
		try {
			if (!this.isClosed()) {
				this.closed = true;
			}
		} catch (Exception e) {
			throw new SQLException("A database access error has occurred: " + e.getMessage());
		}
	}

	@Override public boolean isClosed() throws SQLException {
		return this.closed;
	}

	@Override public boolean isValid(int timeout) throws SQLException {
		if (timeout < 0) {
			throw new SQLException("Timeout can't be less than zero");
		}
		if (this.isClosed()) {
			return false;
		}

		Runnable r = new Runnable() {
			@Override public void run() {
				graphDatabaseService.execute(FASTEST_STATEMENT);
			}
		};

		try {
			TimeLimitedCodeBlock.runWithTimeout(r, timeout, TimeUnit.SECONDS);
		} catch (Exception e) { // also timeout
			return false;
		}

		return true;
	}

	@Override public DatabaseMetaData getMetaData() throws SQLException {
		throw ExceptionBuilder.buildUnsupportedOperationException();
	}

	public GraphDatabaseService getGraphDatabaseService() {
		return this.graphDatabaseService;
	}

	public ResultWrapper execute(String sql) throws SQLException {
		try {
			Transaction t = txs.get();
			if (/*this.transaction*/t == null) {
				//			this.transaction = this.graphDatabaseService.beginTx();
				txs.set(this.graphDatabaseService.beginTx());
				t = txs.get();
			}
			Result result = this.graphDatabaseService.execute(sql);
			ResultWrapper rw = new ResultWrapper(result);
			if (this.getAutoCommit()) {
				//			this.transaction.success();
				//			this.transaction.close();
				//			this.transaction = this.graphDatabaseService.beginTx();
				t.success();
				t.close();
				txs.set(this.graphDatabaseService.beginTx());
			}
			return rw;
		} catch(Exception e) {
			throw new SQLException(e);
		}
	}

}
