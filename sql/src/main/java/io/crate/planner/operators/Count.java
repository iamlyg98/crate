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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlannerContext;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.CountPhase;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An optimized version for "select count(*) from t where ..."
 */
public class Count implements LogicalPlan {

    private static final String COUNT_PHASE_NAME = "count-merge";

    final AbstractTableRelation<TableInfo> tableRelation;
    final WhereClause where;

    private final List<Symbol> outputs;
    private final List<AbstractTableRelation> baseTables;

    Count(Function countFunction, AbstractTableRelation<TableInfo> tableRelation, WhereClause where) {
        this.outputs = Collections.singletonList(countFunction);
        this.tableRelation = tableRelation;
        this.baseTables = Collections.singletonList(tableRelation);
        this.where = where;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint) {

        Routing routing = plannerContext.allocateRouting(
            tableRelation.tableInfo(),
            where,
            RoutingProvider.ShardSelection.ANY,
            plannerContext.transactionContext().sessionContext());
        CountPhase countPhase = new CountPhase(
            plannerContext.nextExecutionPhaseId(),
            routing,
            where,
            DistributionInfo.DEFAULT_BROADCAST
        );
        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            COUNT_PHASE_NAME,
            countPhase.nodeIds().size(),
            Collections.singletonList(plannerContext.handlerNode()),
            Collections.singletonList(DataTypes.LONG),
            Collections.singletonList(MergeCountProjection.INSTANCE),
            DistributionInfo.DEFAULT_SAME_NODE,
            null
        );
        return new CountPlan(countPhase, mergePhase);
    }

    @Override
    public LogicalPlan tryCollapse() {
        return this;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return Collections.emptyMap();
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return baseTables;
    }

    @Override
    public long numExpectedRows() {
        return 1L;
    }
}
