/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, NoSuchFileException, Paths}

import scala.util.control.NonFatal

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * A test suite for LogicalPlan-to-SQL conversion.
 *
 * Each query has a golden generated SQL file in test/resources/sqlgen. The test suite also has
 * built-in functionality to automatically generate these golden files.
 *
 * To re-generate golden files, run:
 *    SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "hive/test-only *LogicalPlanToSQLSuite"
 */
class LogicalPlanToSQLSuite extends SQLBuilderTest with SQLTestUtils {
  import testImplicits._

  // Used for generating new query answer files by saving
  private val regenerateGoldenFiles: Boolean =
    Option(System.getenv("SPARK_GENERATE_GOLDEN_FILES")) == Some("1")
  private val goldenSQLPath = "src/test/resources/sqlgen/"

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql("DROP TABLE IF EXISTS parquet_t0")
    sql("DROP TABLE IF EXISTS parquet_t1")
    sql("DROP TABLE IF EXISTS parquet_t2")
    sql("DROP TABLE IF EXISTS t0")

    spark.range(10).write.saveAsTable("parquet_t0")
    sql("CREATE TABLE t0 AS SELECT * FROM parquet_t0")

    spark
      .range(10)
      .select('id as 'key, concat(lit("val_"), 'id) as 'value)
      .write
      .saveAsTable("parquet_t1")

    spark
      .range(10)
      .select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
      .write
      .saveAsTable("parquet_t2")

    def createArray(id: Column): Column = {
      when(id % 3 === 0, lit(null)).otherwise(array('id, 'id + 1))
    }

    spark
      .range(10)
      .select(
        createArray('id).as("arr"),
        array(array('id), createArray('id)).as("arr2"),
        lit("""{"f1": "1", "f2": "2", "f3": 3}""").as("json"),
        'id
      )
      .write
      .saveAsTable("parquet_t3")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS parquet_t0")
      sql("DROP TABLE IF EXISTS parquet_t1")
      sql("DROP TABLE IF EXISTS parquet_t2")
      sql("DROP TABLE IF EXISTS parquet_t3")
      sql("DROP TABLE IF EXISTS t0")
    } finally {
      super.afterAll()
    }
  }

  /**
   * Compare the generated SQL with the expected answer string.
   * Note that there exists a normalization for both arguments for the convenience.
   * - Remove the id from the generated attributes, e.g., `gen_attr_1` -> `gen_attr`.
   */
  private def checkSQLStructure(originalSQL: String, convertedSQL: String, answerFile: String) = {
    val normalizedGenSQL = convertedSQL.replaceAll("`gen_attr_\\d+`", "`gen_attr`")
    if (answerFile != null) {
      val separator = "-" * 80
      if (regenerateGoldenFiles) {
        val path = Paths.get(s"$goldenSQLPath/$answerFile.sql")
        val header = "-- This file is automatically generated by LogicalPlanToSQLSuite."
        val answerText = s"$header\n${originalSQL.trim()}\n${separator}\n$normalizedGenSQL\n"
        Files.write(path, answerText.getBytes(StandardCharsets.UTF_8))
      } else {
        val goldenFileName = s"sqlgen/$answerFile.sql"
        val resourceFile = getClass.getClassLoader.getResource(goldenFileName)
        if (resourceFile == null) {
          throw new NoSuchFileException(goldenFileName)
        }
        val path = resourceFile.getPath
        val answerText = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
        val sqls = answerText.split(separator)
        assert(sqls.length == 2, "Golden sql files should have a separator.")
        val normalizedExpectSQL = sqls(1).trim()
        assert(normalizedGenSQL == normalizedExpectSQL)
      }
    }
  }

  /**
   * 1. Checks if SQL parsing succeeds.
   * 2. Checks if SQL generation succeeds.
   * 3. Checks the generated SQL against golden files.
   * 4. Verifies the execution result stays the same.
   */
  private def checkSQL(sqlString: String, answerFile: String = null): Unit = {
    val df = sql(sqlString)

    val convertedSQL = try new SQLBuilder(df).toSQL catch {
      case NonFatal(e) =>
        fail(
          s"""Cannot convert the following SQL query plan back to SQL query string:
             |
             |# Original SQL query string:
             |$sqlString
             |
             |# Resolved query plan:
             |${df.queryExecution.analyzed.treeString}
           """.stripMargin, e)
    }

    checkSQLStructure(sqlString, convertedSQL, answerFile)

    try {
      checkAnswer(sql(convertedSQL), df)
    } catch { case cause: Throwable =>
      fail(
        s"""Failed to execute converted SQL string or got wrong answer:
           |
           |# Converted SQL query string:
           |$convertedSQL
           |
           |# Original SQL query string:
           |$sqlString
           |
           |# Resolved query plan:
           |${df.queryExecution.analyzed.treeString}
         """.stripMargin, cause)
    }
  }

  // When saving golden files, these tests should be ignored to prevent making files.
  if (!regenerateGoldenFiles) {
    test("Test should fail if the SQL query cannot be parsed") {
      val m = intercept[ParseException] {
        checkSQL("SELE", "NOT_A_FILE")
      }.getMessage
      assert(m.contains("mismatched input"))
    }

    test("Test should fail if the golden file cannot be found") {
      val m2 = intercept[NoSuchFileException] {
        checkSQL("SELECT 1", "NOT_A_FILE")
      }.getMessage
      assert(m2.contains("NOT_A_FILE"))
    }

    test("Test should fail if the SQL query cannot be regenerated") {
      spark.range(10).createOrReplaceTempView("not_sql_gen_supported_table_so_far")
      sql("select * from not_sql_gen_supported_table_so_far")
      val m3 = intercept[org.scalatest.exceptions.TestFailedException] {
        checkSQL("select * from not_sql_gen_supported_table_so_far", "in")
      }.getMessage
      assert(m3.contains("Cannot convert the following SQL query plan back to SQL query string"))
    }

    test("Test should fail if the SQL query did not equal to the golden SQL") {
      val m4 = intercept[org.scalatest.exceptions.TestFailedException] {
        checkSQL("SELECT 1", "in")
      }.getMessage
      assert(m4.contains("did not equal"))
    }
  }

  test("in") {
    checkSQL("SELECT id FROM parquet_t0 WHERE id IN (1, 2, 3)", "in")
  }

  test("not in") {
    checkSQL("SELECT id FROM t0 WHERE id NOT IN (1, 2, 3)", "not_in")
  }

  test("not like") {
    checkSQL("SELECT id FROM t0 WHERE id + 5 NOT LIKE '1%'", "not_like")
  }

  test("aggregate function in having clause") {
    checkSQL("SELECT COUNT(value) FROM parquet_t1 GROUP BY key HAVING MAX(key) > 0", "agg1")
  }

  test("aggregate function in order by clause") {
    checkSQL("SELECT COUNT(value) FROM parquet_t1 GROUP BY key ORDER BY MAX(key)", "agg2")
  }

  // When there are multiple aggregate functions in ORDER BY clause, all of them are extracted into
  // Aggregate operator and aliased to the same name "aggOrder".  This is OK for normal query
  // execution since these aliases have different expression ID.  But this introduces name collision
  // when converting resolved plans back to SQL query strings as expression IDs are stripped.
  test("aggregate function in order by clause with multiple order keys") {
    checkSQL("SELECT COUNT(value) FROM parquet_t1 GROUP BY key ORDER BY key, MAX(key)", "agg3")
  }

  test("type widening in union") {
    checkSQL("SELECT id FROM parquet_t0 UNION ALL SELECT CAST(id AS INT) AS id FROM parquet_t0",
      "type_widening")
  }

  test("union distinct") {
    checkSQL("SELECT * FROM t0 UNION SELECT * FROM t0", "union_distinct")
  }

  test("three-child union") {
    checkSQL(
      """
        |SELECT id FROM parquet_t0
        |UNION ALL SELECT id FROM parquet_t0
        |UNION ALL SELECT id FROM parquet_t0
      """.stripMargin,
      "three_child_union")
  }

  test("intersect") {
    checkSQL("SELECT * FROM t0 INTERSECT SELECT * FROM t0", "intersect")
  }

  test("except") {
    checkSQL("SELECT * FROM t0 EXCEPT SELECT * FROM t0", "except")
  }

  test("self join") {
    checkSQL("SELECT x.key FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key", "self_join")
  }

  test("self join with group by") {
    checkSQL(
      "SELECT x.key, COUNT(*) FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key group by x.key",
      "self_join_with_group_by")
  }

  test("case") {
    checkSQL("SELECT CASE WHEN id % 2 > 0 THEN 0 WHEN id % 2 = 0 THEN 1 END FROM parquet_t0",
      "case")
  }

  test("case with else") {
    checkSQL("SELECT CASE WHEN id % 2 > 0 THEN 0 ELSE 1 END FROM parquet_t0", "case_with_else")
  }

  test("case with key") {
    checkSQL("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' END FROM parquet_t0",
      "case_with_key")
  }

  test("case with key and else") {
    checkSQL("SELECT CASE id WHEN 0 THEN 'foo' WHEN 1 THEN 'bar' ELSE 'baz' END FROM parquet_t0",
      "case_with_key_and_else")
  }

  test("select distinct without aggregate functions") {
    checkSQL("SELECT DISTINCT id FROM parquet_t0", "select_distinct")
  }

  test("rollup/cube #1") {
    // Original logical plan:
    //   Aggregate [(key#17L % cast(5 as bigint))#47L,grouping__id#46],
    //             [(count(1),mode=Complete,isDistinct=false) AS cnt#43L,
    //              (key#17L % cast(5 as bigint))#47L AS _c1#45L,
    //              grouping__id#46 AS _c2#44]
    //   +- Expand [List(key#17L, value#18, (key#17L % cast(5 as bigint))#47L, 0),
    //              List(key#17L, value#18, null, 1)],
    //             [key#17L,value#18,(key#17L % cast(5 as bigint))#47L,grouping__id#46]
    //      +- Project [key#17L,
    //                  value#18,
    //                  (key#17L % cast(5 as bigint)) AS (key#17L % cast(5 as bigint))#47L]
    //         +- Subquery t1
    //            +- Relation[key#17L,value#18] ParquetRelation
    // Converted SQL:
    //   SELECT count( 1) AS `cnt`,
    //          (`t1`.`key` % CAST(5 AS BIGINT)),
    //          grouping_id() AS `_c2`
    //   FROM `default`.`t1`
    //   GROUP BY (`t1`.`key` % CAST(5 AS BIGINT))
    //   GROUPING SETS (((`t1`.`key` % CAST(5 AS BIGINT))), ())
    checkSQL(
      "SELECT count(*) as cnt, key%5, grouping_id() FROM parquet_t1 GROUP BY key % 5 WITH ROLLUP",
      "rollup_cube_1_1")

    checkSQL(
      "SELECT count(*) as cnt, key%5, grouping_id() FROM parquet_t1 GROUP BY key % 5 WITH CUBE",
      "rollup_cube_1_2")
  }

  test("rollup/cube #2") {
    checkSQL("SELECT key, value, count(value) FROM parquet_t1 GROUP BY key, value WITH ROLLUP",
      "rollup_cube_2_1")

    checkSQL("SELECT key, value, count(value) FROM parquet_t1 GROUP BY key, value WITH CUBE",
      "rollup_cube_2_2")
  }

  test("rollup/cube #3") {
    checkSQL(
      "SELECT key, count(value), grouping_id() FROM parquet_t1 GROUP BY key, value WITH ROLLUP",
      "rollup_cube_3_1")

    checkSQL(
      "SELECT key, count(value), grouping_id() FROM parquet_t1 GROUP BY key, value WITH CUBE",
      "rollup_cube_3_2")
  }

  test("rollup/cube #4") {
    checkSQL(
      s"""
        |SELECT count(*) as cnt, key % 5 as k1, key - 5 as k2, grouping_id() FROM parquet_t1
        |GROUP BY key % 5, key - 5 WITH ROLLUP
      """.stripMargin,
      "rollup_cube_4_1")

    checkSQL(
      s"""
        |SELECT count(*) as cnt, key % 5 as k1, key - 5 as k2, grouping_id() FROM parquet_t1
        |GROUP BY key % 5, key - 5 WITH CUBE
      """.stripMargin,
      "rollup_cube_4_2")
  }

  test("rollup/cube #5") {
    checkSQL(
      s"""
        |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id(key % 5, key - 5) AS k3
        |FROM (SELECT key, key%2, key - 5 FROM parquet_t1) t GROUP BY key%5, key-5
        |WITH ROLLUP
      """.stripMargin,
      "rollup_cube_5_1")

    checkSQL(
      s"""
        |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id(key % 5, key - 5) AS k3
        |FROM (SELECT key, key % 2, key - 5 FROM parquet_t1) t GROUP BY key % 5, key - 5
        |WITH CUBE
      """.stripMargin,
      "rollup_cube_5_2")
  }

  test("rollup/cube #6") {
    checkSQL("SELECT a, b, sum(c) FROM parquet_t2 GROUP BY ROLLUP(a, b) ORDER BY a, b",
      "rollup_cube_6_1")

    checkSQL("SELECT a, b, sum(c) FROM parquet_t2 GROUP BY CUBE(a, b) ORDER BY a, b",
      "rollup_cube_6_2")

    checkSQL("SELECT a, b, sum(a) FROM parquet_t2 GROUP BY ROLLUP(a, b) ORDER BY a, b",
      "rollup_cube_6_3")

    checkSQL("SELECT a, b, sum(a) FROM parquet_t2 GROUP BY CUBE(a, b) ORDER BY a, b",
      "rollup_cube_6_4")

    checkSQL("SELECT a + b, b, sum(a - b) FROM parquet_t2 GROUP BY a + b, b WITH ROLLUP",
      "rollup_cube_6_5")

    checkSQL("SELECT a + b, b, sum(a - b) FROM parquet_t2 GROUP BY a + b, b WITH CUBE",
      "rollup_cube_6_6")
  }

  test("rollup/cube #7") {
    checkSQL("SELECT a, b, grouping_id(a, b) FROM parquet_t2 GROUP BY cube(a, b)",
      "rollup_cube_7_1")

    checkSQL("SELECT a, b, grouping(b) FROM parquet_t2 GROUP BY cube(a, b)",
      "rollup_cube_7_2")

    checkSQL("SELECT a, b, grouping(a) FROM parquet_t2 GROUP BY cube(a, b)",
      "rollup_cube_7_3")
  }

  test("rollup/cube #8") {
    // grouping_id() is part of another expression
    checkSQL(
      s"""
         |SELECT hkey AS k1, value - 5 AS k2, hash(grouping_id()) AS hgid
         |FROM (SELECT hash(key) as hkey, key as value FROM parquet_t1) t GROUP BY hkey, value-5
         |WITH ROLLUP
      """.stripMargin,
      "rollup_cube_8_1")

    checkSQL(
      s"""
         |SELECT hkey AS k1, value - 5 AS k2, hash(grouping_id()) AS hgid
         |FROM (SELECT hash(key) as hkey, key as value FROM parquet_t1) t GROUP BY hkey, value-5
         |WITH CUBE
      """.stripMargin,
      "rollup_cube_8_2")
  }

  test("rollup/cube #9") {
    // self join is used as the child node of ROLLUP/CUBE with replaced quantifiers
    checkSQL(
      s"""
         |SELECT t.key - 5, cnt, SUM(cnt)
         |FROM (SELECT x.key, COUNT(*) as cnt
         |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key GROUP BY x.key) t
         |GROUP BY cnt, t.key - 5
         |WITH ROLLUP
      """.stripMargin,
      "rollup_cube_9_1")

    checkSQL(
      s"""
         |SELECT t.key - 5, cnt, SUM(cnt)
         |FROM (SELECT x.key, COUNT(*) as cnt
         |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key GROUP BY x.key) t
         |GROUP BY cnt, t.key - 5
         |WITH CUBE
      """.stripMargin,
      "rollup_cube_9_2")
  }

  test("grouping sets #1") {
    checkSQL(
      s"""
         |SELECT count(*) AS cnt, key % 5 AS k1, key - 5 AS k2, grouping_id() AS k3
         |FROM (SELECT key, key % 2, key - 5 FROM parquet_t1) t GROUP BY key % 5, key - 5
         |GROUPING SETS (key % 5, key - 5)
      """.stripMargin,
      "grouping_sets_1")
  }

  test("grouping sets #2") {
    checkSQL(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (a, b) ORDER BY a, b",
      "grouping_sets_2_1")

    checkSQL(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (a) ORDER BY a, b",
      "grouping_sets_2_2")

    checkSQL(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (b) ORDER BY a, b",
      "grouping_sets_2_3")

    checkSQL(
      "SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (()) ORDER BY a, b",
      "grouping_sets_2_4")

    checkSQL(
      s"""
         |SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b
         |GROUPING SETS ((), (a), (a, b)) ORDER BY a, b
      """.stripMargin,
      "grouping_sets_2_5")
  }

  test("cluster by") {
    checkSQL("SELECT id FROM parquet_t0 CLUSTER BY id", "cluster_by")
  }

  test("distribute by") {
    checkSQL("SELECT id FROM parquet_t0 DISTRIBUTE BY id", "distribute_by")
  }

  test("distribute by with sort by") {
    checkSQL("SELECT id FROM parquet_t0 DISTRIBUTE BY id SORT BY id",
      "distribute_by_with_sort_by")
  }

  test("SPARK-13720: sort by after having") {
    checkSQL("SELECT COUNT(value) FROM parquet_t1 GROUP BY key HAVING MAX(key) > 0 SORT BY key",
      "sort_by_after_having")
  }

  test("distinct aggregation") {
    checkSQL("SELECT COUNT(DISTINCT id) FROM parquet_t0", "distinct_aggregation")
  }

  test("TABLESAMPLE") {
    // Project [id#2L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- Subquery s
    //       +- Subquery parquet_t0
    //          +- Relation[id#2L] ParquetRelation
    checkSQL("SELECT s.id FROM parquet_t0 TABLESAMPLE(100 PERCENT) s", "tablesample_1")

    // Project [id#2L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- Subquery parquet_t0
    //       +- Relation[id#2L] ParquetRelation
    checkSQL("SELECT * FROM parquet_t0 TABLESAMPLE(100 PERCENT)", "tablesample_2")

    // Project [id#21L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- MetastoreRelation default, t0, Some(s)
    checkSQL("SELECT s.id FROM t0 TABLESAMPLE(100 PERCENT) s", "tablesample_3")

    // Project [id#24L]
    // +- Sample 0.0, 1.0, false, ...
    //    +- MetastoreRelation default, t0, None
    checkSQL("SELECT * FROM t0 TABLESAMPLE(100 PERCENT)", "tablesample_4")

    // When a sampling fraction is not 100%, the returned results are random.
    // Thus, added an always-false filter here to check if the generated plan can be successfully
    // executed.
    checkSQL("SELECT s.id FROM parquet_t0 TABLESAMPLE(0.1 PERCENT) s WHERE 1=0", "tablesample_5")
    checkSQL("SELECT * FROM parquet_t0 TABLESAMPLE(0.1 PERCENT) WHERE 1=0", "tablesample_6")
  }

  test("multi-distinct columns") {
    checkSQL("SELECT a, COUNT(DISTINCT b), COUNT(DISTINCT c), SUM(d) FROM parquet_t2 GROUP BY a",
      "multi_distinct")
  }

  test("persisted data source relations") {
    Seq("orc", "json", "parquet").foreach { format =>
      val tableName = s"${format}_parquet_t0"
      withTable(tableName) {
        spark.range(10).write.format(format).saveAsTable(tableName)
        checkSQL(s"SELECT id FROM $tableName", s"data_source_$tableName")
      }
    }
  }

  test("script transformation - schemaless") {
    checkSQL("SELECT TRANSFORM (a, b, c, d) USING 'cat' FROM parquet_t2",
      "script_transformation_1")
    checkSQL("SELECT TRANSFORM (*) USING 'cat' FROM parquet_t2",
      "script_transformation_2")
  }

  test("script transformation - alias list") {
    checkSQL("SELECT TRANSFORM (a, b, c, d) USING 'cat' AS (d1, d2, d3, d4) FROM parquet_t2",
      "script_transformation_alias_list")
  }

  test("script transformation - alias list with type") {
    checkSQL(
      """FROM
        |(FROM parquet_t1 SELECT TRANSFORM(key, value) USING 'cat' AS (thing1 int, thing2 string)) t
        |SELECT thing1 + 1
      """.stripMargin,
      "script_transformation_alias_list_with_type")
  }

  test("script transformation - row format delimited clause with only one format property") {
    checkSQL(
      """SELECT TRANSFORM (key) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |USING 'cat' AS (tKey) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        |FROM parquet_t1
      """.stripMargin,
      "script_transformation_row_format_one")
  }

  test("script transformation - row format delimited clause with multiple format properties") {
    checkSQL(
      """SELECT TRANSFORM (key)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |USING 'cat' AS (tKey)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\t'
        |FROM parquet_t1
      """.stripMargin,
      "script_transformation_row_format_multiple")
  }

  test("script transformation - row format serde clauses with SERDEPROPERTIES") {
    checkSQL(
      """SELECT TRANSFORM (key, value)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |WITH SERDEPROPERTIES('field.delim' = '|')
        |USING 'cat' AS (tKey, tValue)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |WITH SERDEPROPERTIES('field.delim' = '|')
        |FROM parquet_t1
      """.stripMargin,
      "script_transformation_row_format_serde")
  }

  test("script transformation - row format serde clauses without SERDEPROPERTIES") {
    checkSQL(
      """SELECT TRANSFORM (key, value)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |USING 'cat' AS (tKey, tValue)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |FROM parquet_t1
      """.stripMargin,
      "script_transformation_row_format_without_serde")
  }

  test("plans with non-SQL expressions") {
    spark.udf.register("foo", (_: Int) * 2)
    intercept[UnsupportedOperationException](new SQLBuilder(sql("SELECT foo(id) FROM t0")).toSQL)
  }

  test("named expression in column names shouldn't be quoted") {
    def checkColumnNames(query: String, expectedColNames: String*): Unit = {
      checkSQL(query)
      assert(sql(query).columns === expectedColNames)
    }

    // Attributes
    checkColumnNames(
      """SELECT * FROM (
        |  SELECT 1 AS a, 2 AS b, 3 AS `we``ird`
        |) s
      """.stripMargin,
      "a", "b", "we`ird"
    )

    checkColumnNames(
      """SELECT x.a, y.a, x.b, y.b
        |FROM (SELECT 1 AS a, 2 AS b) x
        |INNER JOIN (SELECT 1 AS a, 2 AS b) y
        |ON x.a = y.a
      """.stripMargin,
      "a", "a", "b", "b"
    )

    // String literal
    checkColumnNames(
      "SELECT 'foo', '\"bar\\''",
      "foo", "\"bar\'"
    )

    // Numeric literals (should have CAST or suffixes in column names)
    checkColumnNames(
      "SELECT 1Y, 2S, 3, 4L, 5.1, 6.1D",
      "1", "2", "3", "4", "5.1", "6.1"
    )

    // Aliases
    checkColumnNames(
      "SELECT 1 AS a",
      "a"
    )

    // Complex type extractors
    checkColumnNames(
      """SELECT
        |  a.f1, b[0].f1, b.f1, c["foo"], d[0]
        |FROM (
        |  SELECT
        |    NAMED_STRUCT("f1", 1, "f2", "foo") AS a,
        |    ARRAY(NAMED_STRUCT("f1", 1, "f2", "foo")) AS b,
        |    MAP("foo", 1) AS c,
        |    ARRAY(1) AS d
        |) s
      """.stripMargin,
      "f1", "b[0].f1", "f1", "c[foo]", "d[0]"
    )
  }

  test("window basic") {
    checkSQL("SELECT MAX(value) OVER (PARTITION BY key % 3) FROM parquet_t1", "window_basic_1")

    checkSQL(
      """
         |SELECT key, value, ROUND(AVG(key) OVER (), 2)
         |FROM parquet_t1 ORDER BY key
      """.stripMargin,
      "window_basic_2")

    checkSQL(
      """
         |SELECT value, MAX(key + 1) OVER (PARTITION BY key % 5 ORDER BY key % 7) AS max
         |FROM parquet_t1
      """.stripMargin,
      "window_basic_3")
  }

  test("multiple window functions in one expression") {
    checkSQL(
      """
        |SELECT
        |  MAX(key) OVER (ORDER BY key DESC, value) / MIN(key) OVER (PARTITION BY key % 3)
        |FROM parquet_t1
      """.stripMargin)
  }

  test("regular expressions and window functions in one expression") {
    checkSQL("SELECT MAX(key) OVER (PARTITION BY key % 3) + key FROM parquet_t1",
      "regular_expressions_and_window")
  }

  test("aggregate functions and window functions in one expression") {
    checkSQL("SELECT MAX(c) + COUNT(a) OVER () FROM parquet_t2 GROUP BY a, b",
      "aggregate_functions_and_window")
  }

  test("window with different window specification") {
    checkSQL(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (ORDER BY key, value) AS dr,
         |MAX(value) OVER (PARTITION BY key ORDER BY key ASC) AS max
         |FROM parquet_t1
      """.stripMargin)
  }

  test("window with the same window specification with aggregate + having") {
    checkSQL(
      """
         |SELECT key, value,
         |MAX(value) OVER (PARTITION BY key % 5 ORDER BY key DESC) AS max
         |FROM parquet_t1 GROUP BY key, value HAVING key > 5
      """.stripMargin,
      "window_with_the_same_window_with_agg_having")
  }

  test("window with the same window specification with aggregate functions") {
    checkSQL(
      """
         |SELECT key, value,
         |MAX(value) OVER (PARTITION BY key % 5 ORDER BY key) AS max
         |FROM parquet_t1 GROUP BY key, value
      """.stripMargin,
      "window_with_the_same_window_with_agg_functions")
  }

  test("window with the same window specification with aggregate") {
    checkSQL(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (DISTRIBUTE BY key SORT BY key, value) AS dr,
         |COUNT(key)
         |FROM parquet_t1 GROUP BY key, value
      """.stripMargin,
      "window_with_the_same_window_with_agg")
  }

  test("window with the same window specification without aggregate and filter") {
    checkSQL(
      """
         |SELECT key, value,
         |DENSE_RANK() OVER (DISTRIBUTE BY key SORT BY key, value) AS dr,
         |COUNT(key) OVER(DISTRIBUTE BY key SORT BY key, value) AS ca
         |FROM parquet_t1
      """.stripMargin,
      "window_with_the_same_window_with_agg_filter")
  }

  test("window clause") {
    checkSQL(
      """
         |SELECT key, MAX(value) OVER w1 AS MAX, MIN(value) OVER w2 AS min
         |FROM parquet_t1
         |WINDOW w1 AS (PARTITION BY key % 5 ORDER BY key), w2 AS (PARTITION BY key % 6)
      """.stripMargin)
  }

  test("special window functions") {
    checkSQL(
      """
        |SELECT
        |  RANK() OVER w,
        |  PERCENT_RANK() OVER w,
        |  DENSE_RANK() OVER w,
        |  ROW_NUMBER() OVER w,
        |  NTILE(10) OVER w,
        |  CUME_DIST() OVER w,
        |  LAG(key, 2) OVER w,
        |  LEAD(key, 2) OVER w
        |FROM parquet_t1
        |WINDOW w AS (PARTITION BY key % 5 ORDER BY key)
      """.stripMargin)
  }

  test("window with join") {
    checkSQL(
      """
        |SELECT x.key, MAX(y.key) OVER (PARTITION BY x.key % 5 ORDER BY x.key)
        |FROM parquet_t1 x JOIN parquet_t1 y ON x.key = y.key
      """.stripMargin,
      "window_with_join")
  }

  test("join 2 tables and aggregate function in having clause") {
    checkSQL(
      """
        |SELECT COUNT(a.value), b.KEY, a.KEY
        |FROM parquet_t1 a, parquet_t1 b
        |GROUP BY a.KEY, b.KEY
        |HAVING MAX(a.KEY) > 0
      """.stripMargin,
      "join_2_tables")
  }

  test("generator in project list without FROM clause") {
    checkSQL("SELECT EXPLODE(ARRAY(1,2,3))", "generator_without_from_1")
    checkSQL("SELECT EXPLODE(ARRAY(1,2,3)) AS val", "generator_without_from_2")
  }

  test("generator in project list with non-referenced table") {
    checkSQL("SELECT EXPLODE(ARRAY(1,2,3)) FROM t0", "generator_non_referenced_table_1")
    checkSQL("SELECT EXPLODE(ARRAY(1,2,3)) AS val FROM t0", "generator_non_referenced_table_2")
  }

  test("generator in project list with referenced table") {
    checkSQL("SELECT EXPLODE(arr) FROM parquet_t3", "generator_referenced_table_1")
    checkSQL("SELECT EXPLODE(arr) AS val FROM parquet_t3", "generator_referenced_table_2")
  }

  test("generator in project list with non-UDTF expressions") {
    checkSQL("SELECT EXPLODE(arr), id FROM parquet_t3", "generator_non_udtf_1")
    checkSQL("SELECT EXPLODE(arr) AS val, id as a FROM parquet_t3", "generator_non_udtf_2")
  }

  test("generator in lateral view") {
    checkSQL("SELECT val, id FROM parquet_t3 LATERAL VIEW EXPLODE(arr) exp AS val",
      "generator_in_lateral_view_1")
    checkSQL("SELECT val, id FROM parquet_t3 LATERAL VIEW OUTER EXPLODE(arr) exp AS val",
      "generator_in_lateral_view_2")
  }

  test("generator in lateral view with ambiguous names") {
    checkSQL(
      """
        |SELECT exp.id, parquet_t3.id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr) exp AS id
      """.stripMargin,
      "generator_with_ambiguous_names_1")

    checkSQL(
      """
        |SELECT exp.id, parquet_t3.id
        |FROM parquet_t3
        |LATERAL VIEW OUTER EXPLODE(arr) exp AS id
      """.stripMargin,
      "generator_with_ambiguous_names_2")
  }

  test("use JSON_TUPLE as generator") {
    checkSQL(
      """
        |SELECT c0, c1, c2
        |FROM parquet_t3
        |LATERAL VIEW JSON_TUPLE(json, 'f1', 'f2', 'f3') jt
      """.stripMargin,
      "json_tuple_generator_1")

    checkSQL(
      """
        |SELECT a, b, c
        |FROM parquet_t3
        |LATERAL VIEW JSON_TUPLE(json, 'f1', 'f2', 'f3') jt AS a, b, c
      """.stripMargin,
      "json_tuple_generator_2")
  }

  test("nested generator in lateral view") {
    checkSQL(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW EXPLODE(nested_array) exp1 AS val
      """.stripMargin,
      "nested_generator_in_lateral_view_1")

    checkSQL(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW OUTER EXPLODE(nested_array) exp1 AS val
      """.stripMargin,
      "nested_generator_in_lateral_view_2")
  }

  test("generate with other operators") {
    checkSQL(
      """
        |SELECT EXPLODE(arr) AS val, id
        |FROM parquet_t3
        |WHERE id > 2
        |ORDER BY val, id
        |LIMIT 5
      """.stripMargin,
      "generate_with_other_1")

    checkSQL(
      """
        |SELECT val, id
        |FROM parquet_t3
        |LATERAL VIEW EXPLODE(arr2) exp1 AS nested_array
        |LATERAL VIEW EXPLODE(nested_array) exp1 AS val
        |WHERE val > 2
        |ORDER BY val, id
        |LIMIT 5
      """.stripMargin,
      "generate_with_other_2")
  }

  test("filter after subquery") {
    checkSQL("SELECT a FROM (SELECT key + 1 AS a FROM parquet_t1) t WHERE a > 5",
      "filter_after_subquery")
  }

  test("SPARK-14933 - select parquet table") {
    withTable("parquet_t") {
      sql("create table parquet_t stored as parquet as select 1 as c1, 'abc' as c2")
      checkSQL("select * from parquet_t", "select_parquet_table")
    }
  }

  test("predicate subquery") {
    withTable("t1") {
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        sql("CREATE TABLE t1(a int)")
        checkSQL("select * from t1 b where exists (select * from t1 a)", "predicate_subquery")
      }
    }
  }

  test("SPARK-14933 - select orc table") {
    withTable("orc_t") {
      sql("create table orc_t stored as orc as select 1 as c1, 'abc' as c2")
      checkSQL("select * from orc_t", "select_orc_table")
    }
  }
}
