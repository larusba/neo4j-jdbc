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
 * Created on 05/12/17
 */
package org.neo4j.jdbc.impermanent;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jdbc.Neo4jDriver;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.WeakHashMap;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class ImpermanentNeo4jDriver extends Neo4jDriver {

	public static final  String                                    JDBC_IMPERMANENT_PREFIX = "mem";
	private static final WeakHashMap<String, GraphDatabaseService> databases               = new WeakHashMap<>();

	static {
		try {
			ImpermanentNeo4jDriver driver = new ImpermanentNeo4jDriver();
			DriverManager.registerDriver(driver);
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * Constructor for extended class.
	 *
	 * @throws SQLException sqlexception
	 */
	public ImpermanentNeo4jDriver() throws SQLException {
		super(JDBC_IMPERMANENT_PREFIX);
	}

	@Override public Connection connect(String url, Properties info) throws SQLException {
		if (url == null) {
			throw new SQLException("null is not a valid url");
		}
		Connection connection = null;
		if (acceptsURL(url)) {
			GraphDatabaseService graphDatabaseService = getDatabaseByName(getNameFromUrl(url));
			connection = new ImpermanentNeo4jConnection(graphDatabaseService, info, url);
		}
		return connection;
	}

	@Override public boolean acceptsURL(String url) throws SQLException {
		if (url == null) {
			throw new SQLException("null is not a valid url");
		}
		String[] pieces = url.split(":");
		if (pieces.length > 2 && url.startsWith(JDBC_PREFIX)) {
			if (driverPrefix != null) {
				if (pieces[2].matches(JDBC_IMPERMANENT_PREFIX)) {
					return true;
				}
			} else {
				return true;
			}
		}
		return false;
	}

	private GraphDatabaseService getDatabaseByName(String name) {
		GraphDatabaseService graphDatabaseService;
		synchronized (databases) {
			graphDatabaseService = databases.get(name);
			if (graphDatabaseService == null) {
				graphDatabaseService = withShutdownHook(defaultImpermanentDb());
				databases.put(name, graphDatabaseService);
			}
		}
		return graphDatabaseService;
	}

	private static GraphDatabaseService defaultImpermanentDb() {
		String uniqueFolder = UUID.randomUUID().toString().substring(0, 10);
		File file = new File("impermanent-db/" + uniqueFolder);
		return new TestGraphDatabaseFactory().newImpermanentDatabase(file);
	}

	private static GraphDatabaseService withShutdownHook(final GraphDatabaseService graphDatabaseService) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override public void run() {
				graphDatabaseService.shutdown();
			}
		});
		return graphDatabaseService;
	}

	private static String getNameFromUrl(String url) {
		String name = null;
		String[] urlElements = url.split(":");
		if (urlElements.length > 3) {
			name = urlElements[3];
		}
		return name;
	}
}
