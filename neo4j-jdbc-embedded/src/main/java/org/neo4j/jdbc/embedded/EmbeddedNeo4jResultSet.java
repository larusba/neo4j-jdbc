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

import org.neo4j.jdbc.Neo4jResultSet;
import org.neo4j.jdbc.Neo4jStatement;
import org.neo4j.jdbc.embedded.util.ResultWrapper;

import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class EmbeddedNeo4jResultSet extends Neo4jResultSet {

	private Iterator<Map<String, Object>> iterator;
	private Map<String, Object>           current;

	public EmbeddedNeo4jResultSet(Neo4jStatement statement, ResultWrapper rw, int... params) {
		super(statement, params);
		if (rw != null) {
			this.iterator = rw.getData().iterator();
			this.columnLabels = rw.getColumns();
		}
	}

	@Override public void close() throws SQLException {
		if (this.iterator == null) {
			throw new SQLException("ResultCursor not initialized");
		}
		this.isClosed = true;
	}

	@Override public String getString(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getString(columnLabel);
	}

	@Override public boolean getBoolean(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getBoolean(columnLabel);
	}

	@Override public short getShort(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getShort(columnLabel);
	}

	@Override public int getInt(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getInt(columnLabel);
	}

	@Override public long getLong(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getLong(columnLabel);
	}

	@Override public float getFloat(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getFloat(columnLabel);
	}

	@Override public double getDouble(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getDouble(columnLabel);
	}

	@Override public String getString(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		return (String) this.current.get(columnLabel);
	}

	@Override public boolean getBoolean(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		Object value = this.current.get(columnLabel);
		return value != null && (boolean) value;
	}

	@Override public short getShort(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		Object value = this.current.get(columnLabel);
		return value == null ? 0 : (short) value;
	}

	@Override public int getInt(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		Object value = this.current.get(columnLabel);
		return value == null ? 0 : (int) value;
	}

	@Override public long getLong(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		Object value = this.current.get(columnLabel);
		return value == null ? 0 : (long) value;
	}

	@Override public float getFloat(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		Object value = this.current.get(columnLabel);
		return value == null ? 0 : (float) value;
	}

	@Override public double getDouble(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		Object value = this.current.get(columnLabel);
		return value == null ? 0 : (double) value;
	}

	@Override public ResultSetMetaData getMetaData() throws SQLException {
		return null;
	}

	@Override public Object getObject(int columnIndex) throws SQLException {
		checkClosed();
		String columnLabel = checkValidColumnIndex(columnIndex);
		return getObject(columnLabel);
	}

	@Override public Object getObject(String columnLabel) throws SQLException {
		checkClosed();
		checkContainsColumnLabel(columnLabel);
		return this.current.get(columnLabel);
	}

	@Override public Array getArray(int columnIndex) throws SQLException {
		return null;
	}

	@Override public Array getArray(String columnLabel) throws SQLException {
		return null;
	}

	@Override protected boolean innerNext() throws SQLException {
		if (this.iterator == null) {
			throw new SQLException("ResultCursor not initialized");
		}
		if (this.iterator.hasNext()) {
			this.current = this.iterator.next();
		} else {
			this.current = null;
		}
		return this.current != null;
	}

	private void checkContainsColumnLabel(String columnLabel) throws SQLException {
		if (!this.current.containsKey(columnLabel)) {
			throw new SQLException("Column Label not valid");
		}
	}

	private String checkValidColumnIndex(int columnIndex) throws SQLException {
		if (columnIndex == 0 || columnIndex > this.columnLabels.size()) {
			throw new SQLException("Column Index not valid");
		} else {
			return this.columnLabels.get(columnIndex - 1);
		}
	}
}
