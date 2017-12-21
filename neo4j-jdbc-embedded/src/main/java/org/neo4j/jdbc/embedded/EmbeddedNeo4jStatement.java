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

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.jdbc.Neo4jStatement;
import org.neo4j.jdbc.embedded.util.ResultWrapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class EmbeddedNeo4jStatement extends Neo4jStatement {

	/**
	 * Default Constructor
	 *
	 * @param connection The connection used for sharing the transaction between statements
	 * @param rsParams   The params (type, concurrency and holdability) used to create a new ResultSet
	 */
	public EmbeddedNeo4jStatement(EmbeddedNeo4jConnection connection, int... rsParams) {
		super(connection, rsParams);
	}

	@Override public ResultSet executeQuery(String sql) throws SQLException {
		ResultWrapper result = executeInternal(sql);

		this.currentResultSet = new EmbeddedNeo4jResultSet(this, result, this.rsParams);
		this.currentUpdateCount = -1;
		return this.currentResultSet;
	}

	@Override public int executeUpdate(String sql) throws SQLException {
		ResultWrapper result = executeInternal(sql);

		this.currentUpdateCount = getQueryChangedElements(result);
		this.currentResultSet = null;
		return this.currentUpdateCount;
	}

	@Override public boolean execute(String sql) throws SQLException {
		ResultWrapper result = executeInternal(sql);

		//		boolean hasResultSet = false;
		//		if (result != null) {
		boolean hasResultSet = hasReturnClause(sql);
		if (hasResultSet) {
			this.currentResultSet = new EmbeddedNeo4jResultSet(this, result, this.rsParams);
			this.currentUpdateCount = -1;
		} else {
			this.currentResultSet = null;
			try {
				this.currentUpdateCount = getQueryChangedElements(result);
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
		//		}
		return hasResultSet;
	}

	private ResultWrapper executeInternal(String sql) throws SQLException {
		checkClosed();

		/*ResultWrapper rw;

		GraphDatabaseService graphDatabaseService = ((EmbeddedNeo4jConnection) this.getConnection()).getGraphDatabaseService();
		Result result;
		if (this.getConnection().getAutoCommit()) {

			try (Transaction t = graphDatabaseService.beginTx()) {
				result = graphDatabaseService.execute(sql);
				rw = new ResultWrapper(result);
				t.success();
			} catch (Exception e) {
				throw new SQLException(e);
			}
		} else {
			try {
				result = graphDatabaseService.execute(sql);
				rw = new ResultWrapper(result);
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
		return rw;*/
		return ((EmbeddedNeo4jConnection) this.getConnection()).execute(sql);
	}

	private int getQueryChangedElements(ResultWrapper result) {
		QueryStatistics queryStatistics = result.getQueryStatistics();
		return queryStatistics.getNodesCreated() + queryStatistics.getNodesDeleted() + queryStatistics.getRelationshipsCreated() + queryStatistics
				.getRelationshipsDeleted();
	}

}
