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
package org.neo4j.jdbc.data;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Gianmarco Laggia @ Larus B.A.
 * @since 3.2.0
 */
public class ResultSetData {

	protected ResultSetData() {}

	protected static final List<Object[]> RECORD_LIST_EMPTY = Collections.emptyList();
	protected static final List<Object[]> RECORD_LIST_ONE_ELEMENT = new LinkedList<>();
	protected static final List<Object[]> RECORD_LIST_ONE_NULL_ELEMENT = new LinkedList<>();
	protected static final List<Object[]> RECORD_LIST_MORE_ELEMENTS = new LinkedList<>();
	protected static final List<Object[]> RECORD_LIST_MORE_ELEMENTS_DIFF = new LinkedList<>();
	protected static final List<Object[]> RECORD_LIST_MORE_ELEMENTS_MIXED = new LinkedList<>();
	protected static final List<Object[]> RECORD_LIST_WITH_ARRAY = new LinkedList<>();
	private static final String[] KEYS_RECORD_LIST_EMPTY                   = new String[] {};
	private static final String[] KEYS_RECORD_LIST_ONE_ELEMENT             = new String[] { "columnA", "columnB" };
	private static final String[] KEYS_RECORD_LIST_ONE_NULL_ELEMENT        = KEYS_RECORD_LIST_ONE_ELEMENT;
	private static final String[] KEYS_RECORD_LIST_MORE_ELEMENTS           = KEYS_RECORD_LIST_ONE_ELEMENT;
	private static final String[] KEYS_RECORD_LIST_MORE_ELEMENTS_DIFF      = new String[] { "columnA", "columnB", "columnC" };
	private static final String[] KEYS_RECORD_LIST_MORE_ELEMENTS_MIXED     = new String[] { "columnInt", "columnString", "columnFloat", "columnShort",
			"columnDouble", "columnBoolean", "columnLong", "columnNull" };
	private static final String[] KEYS_RECORD_LIST_MORE_ELEMENTS_NODES     = new String[] { "node" };
	private static final String[] KEYS_RECORD_LIST_MORE_ELEMENTS_PATHS     = new String[] { "path" };
	private static final String[] KEYS_RECORD_LIST_MORE_ELEMENTS_RELATIONS = new String[] { "relation" };
	private static final String[] KEYS_RECORD_LIST_WITH_ARRAY              = new String[] { "array" };

	public static void initialize() {
		RECORD_LIST_ONE_ELEMENT.clear();
		RECORD_LIST_ONE_ELEMENT.add(new Object[] { "valueA1", "valueB1" });

		RECORD_LIST_ONE_NULL_ELEMENT.clear();
		RECORD_LIST_ONE_NULL_ELEMENT.add(new Object[] { null, null });

		RECORD_LIST_MORE_ELEMENTS.clear();
		RECORD_LIST_MORE_ELEMENTS.add(new Object[] { "valueA1", "valueB1" });
		RECORD_LIST_MORE_ELEMENTS.add(new Object[] { "valueA2", "valueB2" });
		RECORD_LIST_MORE_ELEMENTS.add(new Object[] { "valueA3", "valueB3" });

		RECORD_LIST_MORE_ELEMENTS_MIXED.clear();
		RECORD_LIST_MORE_ELEMENTS_MIXED.add(new Object[] { 1, "value1", 0.1f, (short) 1, 02.29D, true, 2L, null });
		RECORD_LIST_MORE_ELEMENTS_MIXED.add(new Object[] { 2, "value2", 0.2f, (short) 2, 20.16D, false, 6L, null });

		RECORD_LIST_MORE_ELEMENTS_DIFF.clear();
		RECORD_LIST_MORE_ELEMENTS_DIFF.add(new Object[] { "valueA", "valueB" });
		RECORD_LIST_MORE_ELEMENTS_DIFF.add(new Object[] { "valueA", "valueB", "valueC" });

		RECORD_LIST_WITH_ARRAY.clear();
		RECORD_LIST_WITH_ARRAY.add(new Object[] { new String[] { "a", "b", "c" } });
		RECORD_LIST_WITH_ARRAY.add(new Object[] { new Integer[] { 5, 10, 99 } });
		RECORD_LIST_WITH_ARRAY.add(new Object[] { new Boolean[] { true, false, false } });
		RECORD_LIST_WITH_ARRAY.add(new Object[] { new Double[] { 6.5, 4.3, 2.1 } });

	}

	public static List<Object[]> getRecordListEmpty() {
		return RECORD_LIST_EMPTY;
	}

	public static List<Object[]> getRecordListOneElement() {
		return RECORD_LIST_ONE_ELEMENT;
	}

	public static List<Object[]> getRecordListOneNullElement() {
		return RECORD_LIST_ONE_NULL_ELEMENT;
	}

	public static List<Object[]> getRecordListMoreElements() {
		return RECORD_LIST_MORE_ELEMENTS;
	}

	public static List<Object[]> getRecordListMoreElementsDiff() {
		return RECORD_LIST_MORE_ELEMENTS_DIFF;
	}

	public static List<Object[]> getRecordListMoreElementsMixed() {
		return RECORD_LIST_MORE_ELEMENTS_MIXED;
	}

	public static List<Object[]> getRecordListWithArray() {
		return RECORD_LIST_WITH_ARRAY;
	}

	public static String[] getKeysRecordListEmpty() {
		return KEYS_RECORD_LIST_EMPTY;
	}

	public static String[] getKeysRecordListOneElement() {
		return KEYS_RECORD_LIST_ONE_ELEMENT;
	}

	public static String[] getKeysRecordListOneNullElement() {
		return KEYS_RECORD_LIST_ONE_NULL_ELEMENT;
	}

	public static String[] getKeysRecordListMoreElements() {
		return KEYS_RECORD_LIST_MORE_ELEMENTS;
	}

	public static String[] getKeysRecordListMoreElementsDiff() {
		return KEYS_RECORD_LIST_MORE_ELEMENTS_DIFF;
	}

	public static String[] getKeysRecordListMoreElementsMixed() {
		return KEYS_RECORD_LIST_MORE_ELEMENTS_MIXED;
	}

	public static String[] getKeysRecordListMoreElementsNodes() {
		return KEYS_RECORD_LIST_MORE_ELEMENTS_NODES;
	}

	public static String[] getKeysRecordListMoreElementsPaths() {
		return KEYS_RECORD_LIST_MORE_ELEMENTS_PATHS;
	}

	public static String[] getKeysRecordListMoreElementsRelations() {
		return KEYS_RECORD_LIST_MORE_ELEMENTS_RELATIONS;
	}

	public static String[] getKeysRecordListWithArray() {
		return KEYS_RECORD_LIST_WITH_ARRAY;
	}
}
