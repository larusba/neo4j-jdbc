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
 * Created on 13/12/17
 */
package org.neo4j.jdbc.embedded.test.data;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.Result;
import org.neo4j.jdbc.data.ResultSetData;
import org.neo4j.jdbc.embedded.util.ResultWrapper;

import java.util.*;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class ImpermanentResultSetData extends ResultSetData {

	public static void initialize() {
		ResultSetData.initialize();
	}

	private static void setUpPaths() {

		//		Node node1 = new InternalNode(1, new LinkedList<String>() {
		//			{
		//				this.add("label1");
		//			}
		//		}, new HashMap<String, Value>() {
		//			{
		//				this.put("property", new StringValue("value"));
		//			}
		//		});
		//
		//		Node node2 = new InternalNode(2, new LinkedList<String>() {
		//			{
		//				this.add("label1");
		//			}
		//		}, new HashMap<String, Value>() {
		//			{
		//				this.put("property", new StringValue("value2"));
		//			}
		//		});
		//
		//		org.neo4j.driver.v1.types.Relationship rel1 = new InternalRelationship(3, 1, 2, "type", new HashMap<String, Value>() {
		//			{
		//				this.put("relProperty", new StringValue("value3"));
		//			}
		//		});
		//
		//		List<Entity> entities1 = new ArrayList<>();
		//		entities1.add(node1);
		//		entities1.add(rel1);
		//		entities1.add(node2);
		//
		//		path1 = new InternalPath(entities1);
		//
		//		Node node3 = new InternalNode(4, new LinkedList<String>() {
		//			{
		//				this.add("label1");
		//			}
		//		}, new HashMap<String, Value>() {
		//			{
		//				this.put("property", new StringValue("value"));
		//			}
		//		});
		//
		//		Node node4 = new InternalNode(5, new LinkedList<String>() {
		//			{
		//				this.add("label1");
		//			}
		//		}, new HashMap<String, Value>() {
		//			{
		//				this.put("property", new StringValue("value2"));
		//			}
		//		});
		//
		//		Node node5 = new InternalNode(6, new LinkedList<String>() {
		//			{
		//				this.add("label1");
		//			}
		//		}, new HashMap<String, Value>() {
		//			{
		//				this.put("property", new StringValue("value3"));
		//			}
		//		});
		//
		//		org.neo4j.driver.v1.types.Relationship rel2 = new InternalRelationship(7, 4, 5, "type", new HashMap<String, Value>() {
		//			{
		//				this.put("relProperty", new StringValue("value4"));
		//			}
		//		});
		//
		//		org.neo4j.driver.v1.types.Relationship rel3 = new InternalRelationship(8, 6, 5, "type", new HashMap<String, Value>() {
		//			{
		//				this.put("relProperty", new StringValue("value5"));
		//			}
		//		});
		//
		//		List<Entity> entities2 = new ArrayList<>();
		//		entities2.add(node3);
		//		entities2.add(rel2);
		//		entities2.add(node4);
		//		entities2.add(rel3);
		//		entities2.add(node5);
		//
		//		path2 = new InternalPath(entities2);
	}

	/**
	 * open up some package scope method for public usage
	 */
	private static void fixPublicForInternalResultCursor() {
		//		try {
		//			runResponseCollectorMethod = InternalStatementResult.class.getDeclaredMethod("runResponseCollector");
		//			runResponseCollectorMethod.setAccessible(true);
		//			pullAllResponseCollectorMethod = InternalStatementResult.class.getDeclaredMethod("pullAllResponseCollector");
		//			pullAllResponseCollectorMethod.setAccessible(true);
		//		} catch (NoSuchMethodException e) {
		//			throw new RuntimeException(e);
		//		}
	}

	/**
	 * hackish way to get a { InternalStatementResult}
	 *
	 * @param keys
	 * @param data
	 * @return
	 */
	public static Result buildResultCursor(final String[] keys, final List<Object[]> data) {

		try {
			Result cursor = mock(Result.class);
			final List<String> columns = asList(keys);
			when(cursor.columns()).thenReturn(columns);

			final Iterator<Object[]> it = data.iterator();
			when(cursor.hasNext()).thenReturn(it.hasNext());
			when(cursor.next()).thenAnswer(new Answer<Map<String, Object>>() {
				@Override public Map<String, Object> answer(InvocationOnMock invocationOnMock) throws Throwable {
					HashMap<String, Object> record = new HashMap<>();
					Object[] row = it.next();
					for (int i = 0; i < keys.length; i++) {
						record.put(keys[i], row[i]);
					}
					return record;
				}
			});

			return cursor;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static ResultWrapper buildResultWrapper(String[] keys, List<Object[]> data) {
		List<Map<String, Object>> mappedData = new ArrayList<>();
		for (Object[] row : data) {
			Map<String, Object> map = new HashMap<>();
			for (int i = 0; i < keys.length; i++) {
				if (i < row.length) {
					map.put(keys[i], row[i]);
				}
			}
			mappedData.add(map);
		}
		ResultWrapper rw = mock(ResultWrapper.class);
		when(rw.getColumns()).thenReturn(Arrays.asList(keys));
		when(rw.getData()).thenReturn(mappedData);

		return rw;
	}
}
