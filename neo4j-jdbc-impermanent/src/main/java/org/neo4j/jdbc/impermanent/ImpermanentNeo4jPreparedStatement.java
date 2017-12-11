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
package org.neo4j.jdbc.impermanent;

import org.neo4j.jdbc.Neo4jConnection;
import org.neo4j.jdbc.Neo4jPreparedStatement;

import java.sql.*;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class ImpermanentNeo4jPreparedStatement extends Neo4jPreparedStatement {

	public ImpermanentNeo4jPreparedStatement(Neo4jConnection connection, String rawStatement, int... rsParams) {
		super(connection, rawStatement, rsParams);
	}

	@Override public ResultSet executeQuery() throws SQLException {
		return null;
	}

	@Override public int executeUpdate() throws SQLException {
		return 0;
	}

	@Override public boolean execute() throws SQLException {
		return false;
	}

	@Override public ResultSetMetaData getMetaData() throws SQLException {
		return null;
	}

	@Override public ParameterMetaData getParameterMetaData() throws SQLException {
		return null;
	}
}
