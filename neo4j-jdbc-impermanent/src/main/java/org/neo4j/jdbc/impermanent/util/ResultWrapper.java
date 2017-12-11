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
 * Created on 14/12/17
 */
package org.neo4j.jdbc.impermanent.util;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class ResultWrapper {

	private List<Map<String, Object>> data;
	private List<String>              columns;
	private QueryStatistics           queryStatistics;

	public ResultWrapper(Result result) {
		data = new ArrayList<>();
		if (result != null) {
			columns = result.columns();
			queryStatistics = result.getQueryStatistics();
			while (result.hasNext()) {
				data.add(result.next());
			}
		}
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public List<String> getColumns() {
		return columns;
	}

	public QueryStatistics getQueryStatistics() {
		return queryStatistics;
	}
}
