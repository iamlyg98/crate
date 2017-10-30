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

package io.crate.analyze.relations;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.HavingClause;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Aggregations;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FunctionCopyVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.Limits;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class SubselectRewriter {

    private static final Visitor INSTANCE = new Visitor();

    private SubselectRewriter() {
    }

    public static AnalyzedRelation rewrite(AnalyzedRelation relation) {
        return relation;
        //return INSTANCE.process(relation, null);
    }

    private static final class Visitor extends AnalyzedRelationVisitor<QueriedSelectRelation, AnalyzedRelation> {

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, QueriedSelectRelation parent) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, QueriedSelectRelation parent) {
            boolean mergedWithParent = false;
            QuerySpec currentQS = relation.querySpec();
            if (parent != null) {
                FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs(), parent);
                QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
                if (canBeMerged(currentQS, parentQS)) {
                    QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                    relation = new QueriedSelectRelation(
                        relation.subRelation(),
                        namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.fieldsByQsOutputSymbols),
                        currentWithParentMerged
                    );
                    mergedWithParent = true;
                }
            }
            AnalyzedRelation origSubRelation = relation.subRelation();
            QueriedRelation subRelation = (QueriedRelation) process(origSubRelation, relation);

            if (origSubRelation == subRelation) {
                return relation;
            }
            if (!mergedWithParent && parent != null) {
                parent.subRelation(subRelation);
                FieldReplacer fieldReplacer = new FieldReplacer(subRelation.fields(), parent);
                parent.querySpec().replace(fieldReplacer);
            }
            if (relation.subRelation() != origSubRelation) {
                return relation;
            }

            return subRelation;
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, QueriedSelectRelation parent) {
            if (parent == null) {
                return table;
            }
            QuerySpec currentQS = table.querySpec();
            FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs(), parent);
            QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
            if (canBeMerged(currentQS, parentQS)) {
                QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                return new QueriedTable(
                    table.tableRelation(),
                    namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.fieldsByQsOutputSymbols),
                    currentWithParentMerged
                );
            }
            return table;
        }

        @Override
        public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, QueriedSelectRelation parent) {
            if (parent == null) {
                return table;
            }
            QuerySpec currentQS = table.querySpec();
            FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs(), parent);
            QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
            if (canBeMerged(currentQS, parentQS)) {
                QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                return new QueriedDocTable(
                    table.tableRelation(),
                    namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.fieldsByQsOutputSymbols),
                    currentWithParentMerged
                );
            }
            return table;
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, QueriedSelectRelation parent) {
            if (parent == null) {
                return multiSourceSelect;
            }
            QuerySpec currentQS = multiSourceSelect.querySpec();
            FieldReplacer fieldReplacer = new FieldReplacer(currentQS.outputs(), parent);
            QuerySpec parentQS = parent.querySpec().copyAndReplace(fieldReplacer);
            if (canBeMerged(currentQS, parentQS)) {
                QuerySpec currentWithParentMerged = mergeQuerySpec(currentQS, parentQS);
                return new MultiSourceSelect(
                    multiSourceSelect.sources(),
                    namesFromOutputs(currentWithParentMerged.outputs(), fieldReplacer.fieldsByQsOutputSymbols),
                    currentWithParentMerged,
                    multiSourceSelect.joinPairs()
                );
            }
            return multiSourceSelect;
        }
    }

    /**
     * @return new output names of a relation which has been merged with it's parent.
     *         It tries to preserve the alias/name of the parent if possible
     */
    private static Collection<Path> namesFromOutputs(Collection<? extends Symbol> outputs,
                                                     Map<Tuple<Symbol, Integer>, Field> replacedFieldsByNewOutput) {
        List<Path> outputNames = new ArrayList<>(outputs.size());
        int idx = 0;
        for (Symbol output : outputs) {
            Field field = replacedFieldsByNewOutput.get(new Tuple<>(output, idx));
            if (field == null) {
                if (output instanceof Path) {
                    outputNames.add((Path) output);
                } else {
                    outputNames.add(new OutputName(SymbolPrinter.INSTANCE.printSimple(output)));
                }
            } else {
                outputNames.add(field);
            }
            idx++;
        }
        return outputNames;
    }

    private static QuerySpec mergeQuerySpec(QuerySpec childQSpec, QuerySpec parentQSpec) {
        // merge everything: validation that merge is possible has already been done.
        OrderBy newOrderBy;
        if (parentQSpec.hasAggregates() || !parentQSpec.groupBy().isEmpty()) {
            // select avg(x) from (select x from t order by x)
            // -> can't keep order, but it doesn't matter for aggregations anyway so
            //    only keep the one from the parent Qspec
            newOrderBy = parentQSpec.orderBy();
        } else {
            newOrderBy = tryReplace(childQSpec.orderBy(), parentQSpec.orderBy());
        }
        return new QuerySpec()
            .outputs(parentQSpec.outputs())
            .where(mergeWhere(childQSpec.where(), parentQSpec.where()))
            .orderBy(newOrderBy)
            .offset(Limits.mergeAdd(childQSpec.offset(), parentQSpec.offset()))
            .limit(Limits.mergeMin(childQSpec.limit(), parentQSpec.limit()))
            .groupBy(pushGroupBy(childQSpec.groupBy(), parentQSpec.groupBy()))
            .having(pushHaving(childQSpec.having(), parentQSpec.having()))
            .hasAggregates(childQSpec.hasAggregates() || parentQSpec.hasAggregates());
    }


    private static WhereClause mergeWhere(WhereClause where1, WhereClause where2) {
        if (!where1.hasQuery() || where1 == WhereClause.MATCH_ALL) {
            return where2;
        } else if (!where2.hasQuery() || where2 == WhereClause.MATCH_ALL) {
            return where1;
        }

        return new WhereClause(AndOperator.join(ImmutableList.of(where2.query(), where1.query())));
    }

    /**
     * "Merge" OrderBy of child & parent relations.
     * <p/>
     * examples:
     * <pre>
     *      childOrderBy: col1, col2
     *      parentOrderBy: col2, col3, col4
     *
     *      merged OrderBy returned: col2, col3, col4
     * </pre>
     * <p/>
     * <pre>
     *      childOrderBy: col1, col2
     *      parentOrderBy:
     *
     *      merged OrderBy returned: col1, col2
     * </pre>
     *
     * @param childOrderBy  The OrderBy of the relation being processed
     * @param parentOrderBy The OrderBy of the parent relation (outer select,  union, etc.)
     * @return The merged orderBy
     */
    @Nullable
    private static OrderBy tryReplace(@Nullable OrderBy childOrderBy, @Nullable OrderBy parentOrderBy) {
        if (parentOrderBy != null) {
            return parentOrderBy;
        }
        return childOrderBy;
    }

    private static List<Symbol> pushGroupBy(List<Symbol> childGroupBy, List<Symbol> parentGroupBy) {
        assert !(!childGroupBy.isEmpty() && !parentGroupBy.isEmpty()) :
            "Cannot merge 'group by' if exists in both parent and child relations";
        if (childGroupBy.isEmpty()) {
            return parentGroupBy;
        }
        return childGroupBy;
    }

    @Nullable
    private static HavingClause pushHaving(@Nullable HavingClause childHaving, @Nullable HavingClause parentHaving) {
        assert !(childHaving != null && parentHaving != null) :
            "Cannot merge 'having' if exists in both parent and child relations";
        if (childHaving == null) {
            return parentHaving;
        }
        return childHaving;
    }

    private static boolean canBeMerged(QuerySpec childQuerySpec, QuerySpec parentQuerySpec) {
        WhereClause parentWhere = parentQuerySpec.where();
        boolean parentHasWhere = !parentWhere.equals(WhereClause.MATCH_ALL);
        boolean childHasLimit = childQuerySpec.limit() != null;
        if (parentHasWhere && childHasLimit) {
            return false;
        }

        boolean parentHasAggregations = parentQuerySpec.hasAggregates() || !parentQuerySpec.groupBy().isEmpty();
        boolean childHasAggregations = childQuerySpec.hasAggregates() || !childQuerySpec.groupBy().isEmpty();
        if (parentHasAggregations && (childHasLimit || childHasAggregations)) {
            return false;
        }

        OrderBy childOrderBy = childQuerySpec.orderBy();
        OrderBy parentOrderBy = parentQuerySpec.orderBy();
        if (childHasLimit && childOrderBy != null && parentOrderBy != null &&
            !childOrderBy.equals(parentOrderBy)) {
            return false;
        }
        if (parentHasWhere && parentWhere.hasQuery() && Aggregations.containsAggregation(parentWhere.query())) {
            return false;
        }
        return true;
    }

    private static final class FieldReplacer extends FunctionCopyVisitor<Void> implements Function<Symbol, Symbol> {

        private final List<? extends Symbol> outputs;
        private final Map<Tuple<Symbol, Integer>, Field> fieldsByQsOutputSymbols = new HashMap<>();
        private int idx = 0;

        FieldReplacer(List<? extends Symbol> outputs, QueriedRelation relation) {
            super();
            // Store the fields of a relation mapped to its output symbol so they will not get lost (e.g. alias preserve)
            this.outputs = outputs;

            /* Create a "backrefs" map to be able to preserve field names/aliases if we merge relations
             *
             * currentQS.outputs,  parent-relation
             *
             * select aliased_sub.x / aliased_sub.i from (select x, i from t1) as aliased_sub
             *
             * parent.fields(): Field[divide..., rel=root]
             * parent.querySpec.outputs:  divide(Field[x, rel=aliased_sub], Field[i, rel=aliased_sub]
             * currentQS.outputs: Field[x, rel=t1], Field[i, rel=t1]
             *
             * -> fieldByQSOutputSymbol
             *
             * divide(Field[x, rel=t1], Field[i, rel=t2]  -> divide(Field[x, rel=aliased_sub], Field[i, rel=aliased_sub])
             */
            for (; idx < relation.fields().size(); idx++) {
                Field field = relation.fields().get(idx);
                addReferencedField(apply(relation.querySpec().outputs().get(field.index())), field);
            }
        }

        @Override
        public Symbol visitField(Field field, Void context) {
            Symbol newOutput = outputs.get(field.index());
            addReferencedField(newOutput, field);
            return newOutput;
        }

        @Override
        public Symbol apply(Symbol symbol) {
            return process(symbol, null);
        }

        private void addReferencedField(Symbol symbol, Field field) {
            fieldsByQsOutputSymbols.put(new Tuple<>(symbol, idx), field);
        }
    }
}
