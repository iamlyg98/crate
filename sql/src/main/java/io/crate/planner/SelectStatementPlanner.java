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

package io.crate.planner;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.exceptions.VersionInvalidException;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.operators.Get;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.MultiPhase;
import io.crate.planner.operators.RootRelationBoundary;

public class SelectStatementPlanner {

    private final Visitor visitor;

    public SelectStatementPlanner(LogicalPlanner logicalPlanner) {
        visitor = new Visitor(logicalPlanner);
    }

    public LogicalPlan plan(QueriedRelation relation, PlannerContext context, SubqueryPlanner subqueryPlanner) {
        return visitor.process(relation, new Context(context, subqueryPlanner));
    }

    private static class Context {
        private final PlannerContext plannerContext;
        private final SubqueryPlanner subqueryPlanner;

        public Context(PlannerContext plannerContext, SubqueryPlanner subqueryPlanner) {
            this.plannerContext = plannerContext;
            this.subqueryPlanner = subqueryPlanner;
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Context, LogicalPlan> {

        private final LogicalPlanner logicalPlanner;

        public Visitor(LogicalPlanner logicalPlanner) {
            this.logicalPlanner = logicalPlanner;
        }

        private LogicalPlan invokeLogicalPlanner(QueriedRelation relation, Context context) {
            LogicalPlan logicalPlan = logicalPlanner.plan(relation, context.plannerContext, context.subqueryPlanner, FetchMode.MAYBE_CLEAR);
            if (logicalPlan == null) {
                throw new UnsupportedOperationException("Cannot create plan for: " + relation);
            }
            return new RootRelationBoundary(logicalPlan);
        }

        @Override
        protected LogicalPlan visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            throw new UnsupportedOperationException("Cannot create plan for: " + relation);
        }

        @Override
        public LogicalPlan visitQueriedRelation(QueriedRelation relation, Context context) {
            return invokeLogicalPlanner(relation, context);
        }

        @Override
        public LogicalPlan visitQueriedTable(QueriedTable table, Context context) {
            context.plannerContext.applySoftLimit(table.querySpec());
            return super.visitQueriedTable(table, context);
        }

        @Override
        public LogicalPlan visitQueriedDocTable(QueriedDocTable table, Context context) {
            QuerySpec querySpec = table.querySpec();
            PlannerContext plannerContext = context.plannerContext;
            plannerContext.applySoftLimit(querySpec);
            if (querySpec.hasAggregates() || (!querySpec.groupBy().isEmpty())) {
                return invokeLogicalPlanner(table, context);
            }
            if (querySpec.where().docKeys().isPresent() && !table.tableRelation().tableInfo().isAlias()) {
                return MultiPhase.createIfNeeded(Get.create(table), table, context.subqueryPlanner);
            }
            if (querySpec.where().hasVersions()) {
                throw new VersionInvalidException();
            }
            return invokeLogicalPlanner(table, context);
        }

        @Override
        public LogicalPlan visitMultiSourceSelect(MultiSourceSelect mss, Context context) {
            QuerySpec querySpec = mss.querySpec();
            context.plannerContext.applySoftLimit(querySpec);
            return invokeLogicalPlanner(mss, context);
        }
    }
}
