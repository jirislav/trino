/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import static io.trino.SystemSessionProperties.ASOF_JOIN_STRATEGY;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.OptimizerConfig.AsofJoinStrategy.SORT_MERGE;
import static io.trino.sql.planner.OptimizerConfig.AsofJoinStrategy.WINDOWING;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestAsofJoinQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = new io.trino.testing.StandaloneQueryRunner(testSessionBuilder()
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.toString())
                .build());

        queryRunner.installPlugin(new io.trino.plugin.tpch.TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", com.google.common.collect.ImmutableMap.of("tpch.splits-per-node", "1"));

        return queryRunner;
    }

    @org.junit.jupiter.api.Test
    public void testRightLteLeftWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts, v) AS (
                    VALUES
                      (1, DATE '1992-01-01', 10),
                      (1, DATE '1992-01-03', 20),
                      (2, DATE '1992-01-02', 30),
                      (3, DATE '1992-01-01', 40)
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND r.ts <= l.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-01', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', 300),
                  (3, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testRightLteLeftWithSortMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts, v) AS (
                    VALUES
                      (1, DATE '1992-01-01', 10),
                      (1, DATE '1992-01-03', 20),
                      (2, DATE '1992-01-02', 30),
                      (3, DATE '1992-01-01', 40)
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND r.ts <= l.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-01', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', 300),
                  (3, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testCompositeEquiKeysWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k1, k2, ts, v) AS (
                    VALUES
                      (1, 'a', DATE '1992-01-01', 10),
                      (1, 'a', DATE '1992-01-03', 20),
                      (1, 'b', DATE '1992-01-02', 30),
                      (2, 'a', DATE '1992-01-02', 40)
                  ),
                  right_t(k1, k2, ts, x) AS (
                    VALUES
                      (1, 'a', DATE '1992-01-01', 100),
                      (1, 'a', DATE '1992-01-02', 200),
                      (1, 'b', DATE '1992-01-01', 300),
                      (2, 'a', DATE '1992-01-01', 400)
                  )
                SELECT l.k1, l.k2, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k1 = r.k1 AND l.k2 = r.k2 AND r.ts <= l.ts
                ORDER BY l.k1, l.k2, l.ts""",
                """
                VALUES
                  (1, 'a', DATE '1992-01-01', 100),
                  (1, 'a', DATE '1992-01-03', 200),
                  (1, 'b', DATE '1992-01-02', 300),
                  (2, 'a', DATE '1992-01-02', 400)
                """);
    }

    @org.junit.jupiter.api.Test
    public void testCompositeEquiKeysWithSortMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k1, k2, ts, v) AS (
                    VALUES
                      (1, 'a', DATE '1992-01-01', 10),
                      (1, 'a', DATE '1992-01-03', 20),
                      (1, 'b', DATE '1992-01-02', 30),
                      (2, 'a', DATE '1992-01-02', 40)
                  ),
                  right_t(k1, k2, ts, x) AS (
                    VALUES
                      (1, 'a', DATE '1992-01-01', 100),
                      (1, 'a', DATE '1992-01-02', 200),
                      (1, 'b', DATE '1992-01-01', 300),
                      (2, 'a', DATE '1992-01-01', 400)
                  )
                SELECT l.k1, l.k2, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k1 = r.k1 AND l.k2 = r.k2 AND r.ts <= l.ts
                ORDER BY l.k1, l.k2, l.ts""",
                """
                VALUES
                  (1, 'a', DATE '1992-01-01', 100),
                  (1, 'a', DATE '1992-01-03', 200),
                  (1, 'b', DATE '1992-01-02', 300),
                  (2, 'a', DATE '1992-01-02', 400)
                """);
    }

    @org.junit.jupiter.api.Test
    public void testNoEquiConditionWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(ts, v) AS (
                    VALUES
                      (DATE '1992-01-02', 10),
                      (DATE '1992-01-03', 20)
                  ),
                  right_t(ts, x) AS (
                    VALUES
                      (DATE '1992-01-01', 100),
                      (DATE '1992-01-02', 200)
                  )
                SELECT l.ts, v, x
                FROM left_t l ASOF JOIN right_t r ON r.ts <= l.ts
                ORDER BY l.ts""",
                """
                VALUES
                  (DATE '1992-01-02', 10, 200),
                  (DATE '1992-01-03', 20, 200)
                """);
    }

    @org.junit.jupiter.api.Test
    public void testNoEquiConditionWithSortMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(ts, v) AS (
                    VALUES
                      (DATE '1992-01-02', 10),
                      (DATE '1992-01-03', 20)
                  ),
                  right_t(ts, x) AS (
                    VALUES
                      (DATE '1992-01-01', 100),
                      (DATE '1992-01-02', 200)
                  )
                SELECT l.ts, v, x
                FROM left_t l ASOF JOIN right_t r ON r.ts <= l.ts
                ORDER BY l.ts""",
                """
                VALUES
                  (DATE '1992-01-02', 10, 200),
                  (DATE '1992-01-03', 20, 200)
                """);
    }

    // Tests for R < L (strict less than)
    @org.junit.jupiter.api.Test
    public void testRightLtLeftWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-02')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-02', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND r.ts < l.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testRightLtLeftWithSortMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-02')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-02', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND r.ts < l.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', CAST(NULL AS INTEGER))
                """);
    }

    // Tests for L >= R (reversed, equivalent to R <= L)
    @org.junit.jupiter.api.Test
    public void testLeftGteRightWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-02')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-03', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND l.ts >= r.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 200),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testLeftGteRightWithSortMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-02')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-03', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND l.ts >= r.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 200),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', CAST(NULL AS INTEGER))
                """);
    }

    // Tests for L > R (reversed strict, equivalent to R < L)
    @org.junit.jupiter.api.Test
    public void testLeftGtRightWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-01')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND l.ts > r.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testLeftGtRightWithSortMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-01')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT l.k, l.ts, r.x
                FROM left_t l ASOF JOIN right_t r
                  ON l.k = r.k AND l.ts > r.ts
                ORDER BY l.k, l.ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 100),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-01', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testUsingNoEquiConditionWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(ts, v) AS (
                    VALUES
                      (DATE '1992-01-02', 10),
                      (DATE '1992-01-03', 20)
                  ),
                  right_t(ts, x) AS (
                    VALUES
                      (DATE '1992-01-01', 100),
                      (DATE '1992-01-02', 200)
                  )
                SELECT ts, v, x
                FROM left_t ASOF JOIN right_t USING (ts)
                ORDER BY ts""",
                """
                VALUES
                  (DATE '1992-01-02', 10, 200),
                  (DATE '1992-01-03', 20, 200)
                """);
    }

    @org.junit.jupiter.api.Test
    public void testUsingNoEquiConditionWithSortMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(ts, v) AS (
                    VALUES
                      (DATE '1992-01-02', 10),
                      (DATE '1992-01-03', 20)
                  ),
                  right_t(ts, x) AS (
                    VALUES
                      (DATE '1992-01-01', 100),
                      (DATE '1992-01-02', 200)
                  )
                SELECT ts, v, x
                FROM left_t ASOF JOIN right_t USING (ts)
                ORDER BY ts""",
                """
                VALUES
                  (DATE '1992-01-02', 10, 200),
                  (DATE '1992-01-03', 20, 200)
                """);
    }

    @org.junit.jupiter.api.Test
    public void testUsingSingleEquiConditionWithWindowingStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, WINDOWING.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-01')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT k, ts, v, x
                FROM left_t ASOF JOIN right_t USING (k, ts)
                ORDER BY ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 200),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', CAST(NULL AS INTEGER))
                """);
    }

    @org.junit.jupiter.api.Test
    public void testUsingSingleEquiConditionWithSOrtMergeStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ASOF_JOIN_STRATEGY, SORT_MERGE.name())
                .build();

        assertQuery(
                session,
                """
                WITH
                  left_t(k, ts) AS (
                    VALUES
                      (1, DATE '1992-01-02'),
                      (1, DATE '1992-01-03'),
                      (2, DATE '1992-01-01')
                  ),
                  right_t(k, ts, x) AS (
                    VALUES
                      (1, DATE '1992-01-01', 100),
                      (1, DATE '1992-01-02', 200),
                      (2, DATE '1992-01-01', 300)
                  )
                SELECT k, ts, v, x
                FROM left_t ASOF JOIN right_t USING (k, ts)
                ORDER BY ts""",
                """
                VALUES
                  (1, DATE '1992-01-02', 200),
                  (1, DATE '1992-01-03', 200),
                  (2, DATE '1992-01-02', CAST(NULL AS INTEGER))
                """);
    }

}
