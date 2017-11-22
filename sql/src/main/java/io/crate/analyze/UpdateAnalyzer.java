/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.Update;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import java.util.HashMap;
import java.util.List;
import java.util.RandomAccess;
import java.util.function.Predicate;

/**
 * Used to analyze statements like: `UPDATE t1 SET col1 = ? WHERE id = ?`
 */
public class UpdateAnalyzer {

    public static final String VERSION_SEARCH_EX_MSG =
        "_version is not allowed in update queries without specifying a primary key";

    private static final UnsupportedFeatureException VERSION_SEARCH_EX = new UnsupportedFeatureException(
        VERSION_SEARCH_EX_MSG);
    private static final Predicate<Reference> IS_OBJECT_ARRAY =
        input -> input != null
        && input.valueType().id() == ArrayType.ID
        && ((ArrayType) input.valueType()).innerType().equals(DataTypes.OBJECT);

    private final Functions functions;
    private final RelationAnalyzer relationAnalyzer;


    UpdateAnalyzer(Functions functions, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedUpdateStatement analyze(Update update, ParamTypeHints typeHints, TransactionContext txnCtx) {
        /* UPDATE t1 SET col1 = ?, col2 = ? WHERE id = ?`
         *               ^^^^^^^^^^^^^^^^^^       ^^^^^^
         *               assignments               whereClause
         *
         *               col1 = ?
         *               |      |
         *               |     source
         *             columnName/target
         */
        StatementAnalysisContext stmtCtx = new StatementAnalysisContext(typeHints, Operation.UPDATE, txnCtx);
        final RelationAnalysisContext relCtx = stmtCtx.startRelation();
        AnalyzedRelation relation = relationAnalyzer.analyze(update.relation(), stmtCtx);
        stmtCtx.endRelation();
        if (!(relation instanceof AbstractTableRelation)) {
            throw new UnsupportedOperationException("UPDATE is only supported on base-tables");
        }
        AbstractTableRelation table = (AbstractTableRelation) relation;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, table);
        ExpressionAnalyzer sourceExprAnalyzer = new ExpressionAnalyzer(
            functions,
            txnCtx,
            typeHints,
            new FullQualifiedNameFieldProvider(
                relCtx.sources(),
                relCtx.parentSources(),
                txnCtx.sessionContext().defaultSchema()
            ),
            null
        );
        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext();

        HashMap<Reference, Symbol> assignmentByTargetCol = getAssignments(
            update.assignements(), typeHints, txnCtx, table, normalizer, sourceExprAnalyzer, exprCtx);
        return new AnalyzedUpdateStatement(
            table,
            assignmentByTargetCol,
            normalizer.normalize(sourceExprAnalyzer.generateQuerySymbol(update.whereClause(), exprCtx), txnCtx)
        );
    }

    private HashMap<Reference, Symbol> getAssignments(List<Assignment> assignments,
                                                      ParamTypeHints typeHints,
                                                      TransactionContext txnCtx,
                                                      AbstractTableRelation table,
                                                      EvaluatingNormalizer normalizer,
                                                      ExpressionAnalyzer sourceExprAnalyzer,
                                                      ExpressionAnalysisContext exprCtx) {
        HashMap<Reference, Symbol> assignmentByTargetCol = new HashMap<>();
        ExpressionAnalyzer targetExprAnalyzer = new ExpressionAnalyzer(
            functions,
            txnCtx,
            typeHints,
            new NameFieldProvider(table),
            null
        );
        targetExprAnalyzer.setResolveFieldsOperation(Operation.UPDATE);
        assert assignments instanceof RandomAccess
            : "assignments should implement RandomAccess for indexed loop to avoid iterator allocations";
        for (int i = 0; i < assignments.size(); i++) {
            Assignment assignment = assignments.get(i);
            AssignmentNameValidator.ensureNoArrayElementUpdate(assignment.columnName());

            Symbol target = normalizer.normalize(targetExprAnalyzer.convert(assignment.columnName(), exprCtx), txnCtx);
            assert target instanceof Reference : "AstBuilder restricts left side of assignments to Columns/Subscripts";
            Reference targetCol = (Reference) target;

            Symbol source = normalizer.normalize(sourceExprAnalyzer.convert(assignment.expression(), exprCtx), txnCtx);
            try {
                source = ValueNormalizer.normalizeInputForReference(source, targetCol, table.tableInfo());
            } catch (IllegalArgumentException | UnsupportedOperationException e) {
                throw new ColumnValidationException(targetCol.ident().columnIdent().sqlFqn(), table.tableInfo().ident(), e);
            }

            if (assignmentByTargetCol.put(targetCol, source) != null) {
                throw new IllegalArgumentException("Target expression repeated: " + targetCol.ident().columnIdent().sqlFqn());
            }
        }
        return assignmentByTargetCol;
    }

    private static class AssignmentNameValidator extends AstVisitor<Void, Boolean> {

        private static final AssignmentNameValidator INSTANCE = new AssignmentNameValidator();

        static void ensureNoArrayElementUpdate(Expression expression) {
            INSTANCE.process(expression, false);
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, Boolean childOfSubscript) {
            process(node.index(), true);
            return super.visitSubscriptExpression(node, childOfSubscript);
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, Boolean childOfSubscript) {
            if (childOfSubscript) {
                throw new IllegalArgumentException("Updating a single element of an array is not supported");
            }
            return null;
        }
    }
}
