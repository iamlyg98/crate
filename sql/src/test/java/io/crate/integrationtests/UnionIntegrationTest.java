/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import io.crate.action.sql.SQLActionException;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2, maxNumDataNodes = 5)
@UseJdbc
public class UnionIntegrationTest extends SQLTransportIntegrationTest {

    private boolean setupDone;

    @Before
    public void beforeTest() {
        if (!setupDone) {
            execute("create table t1 (id integer, text string)");
            execute("create table t2 (id integer, text string)");
            execute("create table t3 (id integer, text string)");

            execute("insert into t1 (id, text) values (?, ?)", new Object[][]{
                new Object[]{1, "text"},
                new Object[]{1000, "text1"},
                new Object[]{42, "magic number"}
            });
            execute("insert into t2 (id, text) values (?, ?)", new Object[][]{
                new Object[]{11, "text"},
                new Object[]{1000, "text2"},
                new Object[]{43, "magic number"}
            });
            execute("insert into t3 (id, text) values (?, ?)", new Object[][]{
                new Object[]{111, "text"},
                new Object[]{1000, "text3"},
                new Object[]{44, "magic number"}
            });
            refresh();
            setupDone = true;
        }
    }

    @Test
    public void testUnionDistinctNotSupported() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UNION [DISTINCT] is not supported");
        execute("select 1 " +
                "union all " +
                "select 2 " +
                "union " +
                "select 3");
    }

    @Test
    public void testUnionAllSimpleSelect() {
        execute("select 1 " +
                "union all " +
                "select 2");
        assertThat(response.rows(), arrayContainingInAnyOrder(
            new Object[] {1L},
            new Object[] {2L}));
    }

    @Test
    public void testUnionAllSelf() {
        execute("select id from t1 " +
                "union all " +
                "select id from t1");
        assertThat(response.rows(),  arrayContainingInAnyOrder(
            new Object[] {1},
            new Object[] {42},
            new Object[] {1000},
            // same results twice
            new Object[] {1},
            new Object[] {42},
            new Object[] {1000}));
    }

    @Test
    public void testUnionAll2Tables() {
        execute("select id from t1 " +
                "union all " +
                "select id from t2 ");
        assertThat(response.rows(), arrayContainingInAnyOrder(
            new Object[] {1},
            new Object[] {11},
            new Object[] {42},
            new Object[] {43},
            new Object[] {1000},
            new Object[] {1000}));
    }

    @Test
    public void testUnionAll3Tables() {
        execute("select id from t1 " +
                "union all " +
                "select id from t2 " +
                "union all " +
                "select id from t3");
        assertThat(response.rows(), arrayContainingInAnyOrder(
            new Object[] {1},
            new Object[] {11},
            new Object[] {42},
            new Object[] {43},
            new Object[] {44},
            new Object[] {111},
            new Object[] {1000},
            new Object[] {1000},
            new Object[] {1000}));
    }

    @Test
    public void testUnion2TablesWithOrderBy() {
        execute("select id from t1 " +
                "union all " +
                "select id from t2 " +
                "order by id");
        assertThat(response.rows(), arrayContaining(
            new Object[] {1},
            new Object[] {11},
            new Object[] {42},
            new Object[] {43},
            new Object[] {1000},
            new Object[] {1000}));
    }

    @Test
    public void testUnionAll3TablesWithOrderBy() {
        execute("select id from t1 " +
                "union all " +
                "select id from t2 " +
                "union all " +
                "select id from t3 " +
                "order by id");
        assertThat(response.rows(), arrayContaining(
            new Object[] {1},
            new Object[] {11},
            new Object[] {42},
            new Object[] {43},
            new Object[] {44},
            new Object[] {111},
            new Object[] {1000},
            new Object[] {1000},
            new Object[] {1000}));
    }

    @Test
    public void testUnionAllWith1SubSelect() {
        execute("select * from (select text from t1 order by text limit 2) a " +
                "union all " +
                "select text from t2 ");
        assertThat(response.rows(), arrayContainingInAnyOrder(
            new Object[] {"magic number"},
            new Object[] {"magic number"},
            new Object[] {"text"},
            new Object[] {"text"},
            new Object[] {"text2"}));
    }

    @Test
    public void testUnionAllWith1SubSelectOrderBy() {
        execute("select * from (select text from t1 order by text limit 2) a " +
                "union all " +
                "select text from t2 " +
                "order by text");
        assertThat(response.rows(), arrayContaining(
            new Object[] {"magic number"},
            new Object[] {"magic number"},
            new Object[] {"text"},
            new Object[] {"text"},
            new Object[] {"text2"}));
    }

    @Test
    public void testUnionAllWith2SubSelect() {
        execute("select * from (select text from t1 order by text limit 2) a " +
                "union all " +
                "select * from (select text from t2 order by text limit 1) b " +
                "order by text " );
        assertThat(response.rows(), arrayContaining(
            new Object[] {"magic number"},
            new Object[] {"magic number"},
            new Object[] {"text"}
        ));
    }

    /**
     * The left and right side of the Union could utilize a fetch operation
     * (no ORDER BY specified). Fetch operations are currently not supported
     * in Union.
     */
    @Test
    public void testUnionAllNoFetching() {
        execute("select * from (select text from t1 limit 1) a " +
                "union all " +
                "select * from (select text from t2 limit 1) b " +
                "order by text " );
        assertThat(response.rows().length, is(2));
    }

    @Test
    public void testUnionAllWithSystemTable() {
        execute("select name from sys.nodes " +
                "union all " +
                "select text from t2");
        int numResults = clusterService().state().nodes().getSize() + 3;
        assertThat(response.rows().length, is(numResults));
    }


    @Test
    public void testUnionAllSubselectJoins() {
        execute("select * from (select t1.id from t1 join t2 on t1.id = t2.id) a " +
                "union all " +
                "select * from (select t2.id from t1 join t2 on t1.text = t2.text) b " +
                "order by id");
        assertThat(response.rows(), arrayContaining(
            new Object[]{11},
            new Object[]{43},
            new Object[]{1000}
        ));
    }
}
