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

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jdbc.data.StatementData;
import org.neo4j.jdbc.impermanent.ImpermanentNeo4jConnection;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class ImpermanentNeo4jResultSetIT {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@BeforeClass public static void setUp() throws ClassNotFoundException {
		Class.forName("org.neo4j.jdbc.impermanent.ImpermanentNeo4jDriver");
	}

	/*------------------------------*/
	/*          flattening          */
	/*------------------------------*/

	@Ignore @Test public void flatteningNumberWorking() throws SQLException, ClassNotFoundException {
		Connection conn = DriverManager.getConnection("jdbc:neo4j:mem");

		GraphDatabaseService gds = ((ImpermanentNeo4jConnection) conn).getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute("CREATE (:User {name:\"name\"})");
			gds.execute("CREATE (:User {surname:\"surname\"})");
			tx.success();
		}

		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("MATCH (u:User) RETURN u;");
		assertEquals(4, rs.getMetaData().getColumnCount());
		assertTrue(rs.next());

		assertEquals(4, rs.findColumn("u.name"));
		assertEquals("name", rs.getString("u.name"));

		conn.close();
	}

	@Ignore @Test public void flatteningNumberWorkingMoreRows() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:neo4j:mem");

		GraphDatabaseService gds = ((ImpermanentNeo4jConnection) conn).getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute("CREATE (:User {name:\"name\"})");
			gds.execute("CREATE (:User {surname:\"surname\"})");
			tx.success();
		}

		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("MATCH (u:User) RETURN u;");
		assertEquals(5, rs.getMetaData().getColumnCount());
		assertTrue(rs.next());

		assertEquals(4, rs.findColumn("u.name"));
		assertEquals("name", rs.getString("u.name"));

		assertTrue(rs.next());
		assertEquals(5, rs.findColumn("u.surname"));
		assertEquals("surname", rs.getString("u.surname"));

		conn.close();
	}

	@Ignore @Test public void flatteningNumberWorkingAllRows() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:neo4j:mem");

		GraphDatabaseService gds = ((ImpermanentNeo4jConnection) conn).getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute("CREATE (:User {name:\"name\"})");
			gds.execute("CREATE (:User {surname:\"surname\"})");
			tx.success();
		}

		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("MATCH (u:User) RETURN u;");
		assertEquals(5, rs.getMetaData().getColumnCount());
		assertTrue(rs.next());

		assertEquals(4, rs.findColumn("u.name"));
		assertEquals("name", rs.getString("u.name"));

		assertTrue(rs.next());
		assertEquals(5, rs.findColumn("u.surname"));
		assertEquals("surname", rs.getString("u.surname"));

		conn.close();
	}

	@Ignore @Test public void findColumnShouldWorkWithFlattening() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:neo4j:mem");

		GraphDatabaseService gds = ((ImpermanentNeo4jConnection) conn).getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute(StatementData.STATEMENT_CREATE);
			tx.success();
		}
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(StatementData.STATEMENT_MATCH_NODES);

		assertEquals(4, rs.findColumn("n.name"));

		conn.close();
	}

	@Ignore @Test public void shouldGetRowReturnValidNumbers() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:neo4j:mem");

		GraphDatabaseService gds = ((ImpermanentNeo4jConnection) conn).getGraphDatabaseService();
		try (Transaction tx = gds.beginTx()) {
			gds.execute("unwind range(1,5) as x create (:User{number:x})");
			tx.success();
		}
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("match (u:User) return u.number as number order by number asc");

		while (rs.next()) {
			assertEquals(rs.getRow(), rs.getInt("number"));

		}
		conn.close();
	}
}
