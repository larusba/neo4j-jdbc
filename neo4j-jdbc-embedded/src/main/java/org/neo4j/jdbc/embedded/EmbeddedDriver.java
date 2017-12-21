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
package org.neo4j.jdbc.embedded;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.jdbc.Neo4jDriver;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class EmbeddedDriver extends Neo4jDriver {

	public static final  String                            JDBC_EMBEDDED_PREFIX = "file";
	private static final Map<String, GraphDatabaseService> databases            = new /*Weak*/HashMap<>();

	static {
		try {
			EmbeddedDriver driver = new EmbeddedDriver();
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
	public EmbeddedDriver() throws SQLException {
		super(JDBC_EMBEDDED_PREFIX);
	}

	@Override public Connection connect(String url, Properties info) throws SQLException {
		if (url == null) {
			throw new SQLException("null is not a valid url");
		}
		Connection connection = null;
		if (acceptsURL(url)) {
			GraphDatabaseService graphDatabaseService = getDatabaseByPath(getNameFromUrl(url));
			connection = new EmbeddedNeo4jConnection(graphDatabaseService, info, url);
		}
		return connection;
	}

	@Override public boolean acceptsURL(String url) throws SQLException {
		if (url == null) {
			throw new SQLException("null is not a valid url");
		}
		String[] pieces = url.split(":");
		if (pieces.length > 3 && url.startsWith(JDBC_PREFIX)) {
			if (driverPrefix != null) {
				if (pieces[2].matches(JDBC_EMBEDDED_PREFIX)) {
					return true;
				}
			} else {
				return true;
			}
		}
		return false;
	}

	private GraphDatabaseService getDatabaseByPath(String path) {
		GraphDatabaseService graphDatabaseService;
		synchronized (databases) {
			graphDatabaseService = databases.get(path);
			if (graphDatabaseService == null) {
				graphDatabaseService = withShutdownHook(createNewEmbeddedDb(path));
				databases.put(path, graphDatabaseService);
			}
		}
		return graphDatabaseService;
	}

	private static GraphDatabaseService createNewEmbeddedDb(String path) {
		File file = new File(path);
		return new GraphDatabaseFactory().newEmbeddedDatabase(file);
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
