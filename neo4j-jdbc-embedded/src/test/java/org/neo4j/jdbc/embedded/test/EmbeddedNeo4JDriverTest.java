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

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.jdbc.Neo4jDriver;
import org.neo4j.jdbc.embedded.EmbeddedNeo4jDriver;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
@RunWith(PowerMockRunner.class) @PrepareForTest({ GraphDatabaseFactory.class, EmbeddedNeo4jDriver.class})
public class EmbeddedNeo4JDriverTest {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private static final String COMPLETE_VALID_URL = "jdbc:neo4j:file:target/test-db";

	@BeforeClass public static void setUpClass() throws Exception {
		mockStatic(GraphDatabaseFactory.class);
		GraphDatabaseFactory gdf = mock(GraphDatabaseFactory.class);
		GraphDatabaseService gds = mock(GraphDatabaseService.class);
		when(gdf.newEmbeddedDatabase(any(File.class))).thenReturn(gds);
		whenNew(GraphDatabaseFactory.class).withNoArguments().thenReturn(gdf);
	}

	/*------------------------------*/
	/*           connect            */
	/*------------------------------*/

	@Test public void shouldConnectCreateConnection() throws Exception {
		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		Connection connection = driver.connect(COMPLETE_VALID_URL, null);
		assertNotNull(connection);
	}

	@Test public void shouldConnectCreateConnectionWithNoAuthTokenWithPropertiesObjectWithoutUserAndPassword() throws SQLException {
		Properties properties = new Properties();
		properties.put("test", "TEST_VALUE");

		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		Connection connection = driver.connect(COMPLETE_VALID_URL, properties);
		assertNotNull(connection);
	}

	@Test public void shouldCreateMultipleInMemoryConnectionToDifferentDatabases() throws SQLException {
		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		assertNotNull(driver.connect(COMPLETE_VALID_URL + ":one", null));
		assertNotNull(driver.connect(COMPLETE_VALID_URL + ":two", null));
		assertNotNull(driver.connect(COMPLETE_VALID_URL + ":three", null));
	}

	@Test public void shouldConnectReturnNullIfUrlNotValid() throws SQLException {
		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		assertNull(driver.connect("jdbc:neo4j:http://localhost:7474", null));
		assertNull(driver.connect("bolt://localhost:7474", null));
		assertNull(driver.connect("jdbcbolt://localhost:7474", null));
		assertNull(driver.connect("jdbc:mysql://localhost:3306/sakila", null));
		assertNull(driver.connect("jdbc:neo4j:bolt://localhost:3306", null));
	}

	@Test public void shouldConnectThrowExceptionOnNullURL() throws SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		driver.connect(null, null);
	}

	/*------------------------------*/
	/*          acceptsURL          */
	/*------------------------------*/
	@Test public void shouldAcceptURLOK() throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException, SQLException {
		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		assertTrue(driver.acceptsURL("jdbc:neo4j:file:/folder"));
		assertTrue(driver.acceptsURL("jdbc:neo4j:file:/otherFolder"));
	}

	@Test public void shouldAcceptURLKO() throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException, SQLException {
		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		assertFalse(driver.acceptsURL("jdbc:neo4j:file"));
		assertFalse(driver.acceptsURL("jdbc:neo4j:http://localhost:7474"));
		assertFalse(driver.acceptsURL("jdbc:file://192.168.0.1:7474"));
		assertFalse(driver.acceptsURL("bolt://localhost:7474"));
		assertFalse(driver.acceptsURL("jdbc:neo4j:bolt://localhost:7474"));
		assertFalse(driver.acceptsURL("jdbc:neo4j:bolt://192.168.0.1:7474"));
		assertFalse(driver.acceptsURL("jdbc:neo4j:bolt://localhost:8080"));
	}

	@Test public void shouldThrowException() throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException, SQLException {
		expectedEx.expect(SQLException.class);

		Neo4jDriver driver = new EmbeddedNeo4jDriver();
		assertFalse(driver.acceptsURL(null));
	}
}
