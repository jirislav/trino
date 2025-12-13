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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.SortMergeAsofJoinNode;
import io.trino.sql.planner.plan.SortNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.plan.JoinType.ASOF;
import static io.trino.sql.planner.plan.Patterns.Join.type;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites ASOF join to use sort-merge algorithm:
 * - Sorts both inputs by equi-keys + timestamp
 * - Creates SortMergeAsofJoinNode that performs merge-scan
 * - Complexity: O(n log n + m log m) instead of O(n × m)
 */
public class RewriteAsofJoinToSortMergeAsofJoin
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().with(type().equalTo(ASOF));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        // Extract and validate non-equi inequality predicate
        Comparison inequality = extractSupportedInequality(node)
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ASOF join requires a single supported inequality predicate"));

        // Determine ordering columns
        OrderingInfo orderingInfo = resolveOrderingInfo(node, inequality);
        if (!orderingInfo.leftOrderingSymbol.type().isOrderable() || !orderingInfo.rightOrderingSymbol.type().isOrderable()) {
            throw new TrinoException(NOT_SUPPORTED, "ASOF join inequality requires orderable types");
        }

        // Extract equi-join symbols for sorting
        List<Symbol> leftEquiSymbols = node.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getLeft)
                .toList();
        List<Symbol> rightEquiSymbols = node.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getRight)
                .toList();

        // Create sort order for left: equi-keys (ASC) + left timestamp (ASC/DESC based on inequality)
        List<Symbol> leftSortSymbols = Stream.concat(
                        leftEquiSymbols.stream(),
                        Stream.of(orderingInfo.leftOrderingSymbol))
                .collect(Collectors.toList());
        ImmutableMap.Builder<Symbol, SortOrder> leftSortOrderBuilder = ImmutableMap.builder();
        for (Symbol symbol : leftEquiSymbols) {
            leftSortOrderBuilder.put(symbol, SortOrder.ASC_NULLS_LAST);
        }
        leftSortOrderBuilder.put(orderingInfo.leftOrderingSymbol, orderingInfo.leftSortOrder);
        OrderingScheme leftOrderingScheme = new OrderingScheme(leftSortSymbols, leftSortOrderBuilder.buildOrThrow());

        // Create sort order for right: equi-keys (ASC) + right timestamp (ASC/DESC based on inequality)
        List<Symbol> rightSortSymbols = Stream.concat(
                        rightEquiSymbols.stream(),
                        Stream.of(orderingInfo.rightOrderingSymbol))
                .collect(Collectors.toList());
        ImmutableMap.Builder<Symbol, SortOrder> rightSortOrderBuilder = ImmutableMap.builder();
        for (Symbol symbol : rightEquiSymbols) {
            rightSortOrderBuilder.put(symbol, SortOrder.ASC_NULLS_LAST);
        }
        rightSortOrderBuilder.put(orderingInfo.rightOrderingSymbol, orderingInfo.rightSortOrder);
        OrderingScheme rightOrderingScheme = new OrderingScheme(rightSortSymbols, rightSortOrderBuilder.buildOrThrow());

        // Sort left input
        SortNode sortedLeft = new SortNode(
                context.getIdAllocator().getNextId(),
                node.getLeft(),
                leftOrderingScheme,
                false);

        // Sort right input
        SortNode sortedRight = new SortNode(
                context.getIdAllocator().getNextId(),
                node.getRight(),
                rightOrderingScheme,
                false);

        // Create SortMergeAsofJoinNode
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(node.getLeftOutputSymbols())
                .addAll(node.getRightOutputSymbols())
                .build();

        SortMergeAsofJoinNode asofJoinNode = new SortMergeAsofJoinNode(
                node.getId(),
                sortedLeft,
                sortedRight,
                outputSymbols,
                node.getCriteria(),
                orderingInfo.leftOrderingSymbol,
                orderingInfo.rightOrderingSymbol,
                orderingInfo.inequalityOperator);

        return Result.ofPlanNode(asofJoinNode);
    }

    private Optional<Comparison> extractSupportedInequality(JoinNode node)
    {
        Optional<Expression> filter = node.getFilter();
        if (filter.isEmpty()) {
            return Optional.empty();
        }
        List<Expression> conjuncts = IrUtils.extractConjuncts(filter.get());
        List<Comparison> comparisons = conjuncts.stream()
                .filter(Comparison.class::isInstance)
                .map(Comparison.class::cast)
                .filter(c -> c.operator() == LESS_THAN || c.operator() == LESS_THAN_OR_EQUAL ||
                        c.operator() == GREATER_THAN || c.operator() == GREATER_THAN_OR_EQUAL)
                .toList();
        if (comparisons.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(comparisons.getFirst());
    }

    private OrderingInfo resolveOrderingInfo(JoinNode node, Comparison inequality)
    {
        Set<Symbol> leftSymbols = ImmutableSet.copyOf(node.getLeft().getOutputSymbols());
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(node.getRight().getOutputSymbols());

        Expression leftExpr = inequality.left();
        Expression rightExpr = inequality.right();

        boolean leftExprFromLeft = referencesOnly(leftExpr, leftSymbols);
        boolean rightExprFromRight = referencesOnly(rightExpr, rightSymbols);

        Comparison.Operator op = inequality.operator();

        // Pattern: l.ts >= r.ts  (left >= right)
        // Semantically: find max r.ts where r.ts <= l.ts (backward looking)
        // Normalize to: LESS_THAN_OR_EQUAL (build <= probe)
        if (leftExprFromLeft && rightExprFromRight && (op == GREATER_THAN_OR_EQUAL || op == GREATER_THAN)) {
            Symbol leftTs = symbolFromReference(leftExpr);
            Symbol rightTs = symbolFromReference(rightExpr);
            Comparison.Operator normalizedOp = (op == GREATER_THAN) ? LESS_THAN : LESS_THAN_OR_EQUAL;
            return new OrderingInfo(leftTs, rightTs, normalizedOp, SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST);
        }

        // Pattern: r.ts <= l.ts  (right <= left)
        // Semantically: find max r.ts where r.ts <= l.ts (backward looking)
        // Keep as: LESS_THAN_OR_EQUAL (build <= probe)
        boolean leftExprFromRight = referencesOnly(leftExpr, rightSymbols);
        boolean rightExprFromLeft = referencesOnly(rightExpr, leftSymbols);
        if (leftExprFromRight && rightExprFromLeft && (op == LESS_THAN_OR_EQUAL || op == LESS_THAN)) {
            Symbol rightTs = symbolFromReference(leftExpr);
            Symbol leftTs = symbolFromReference(rightExpr);
            // Already normalized: op is LESS_THAN or LESS_THAN_OR_EQUAL
            return new OrderingInfo(leftTs, rightTs, op, SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST);
        }

        // Pattern: l.ts <= r.ts  (left <= right)
        // Semantically: find min r.ts where r.ts >= l.ts (forward looking)
        // Normalize to: GREATER_THAN_OR_EQUAL (build >= probe)
        if (leftExprFromLeft && rightExprFromRight && (op == LESS_THAN_OR_EQUAL || op == LESS_THAN)) {
            Symbol leftTs = symbolFromReference(leftExpr);
            Symbol rightTs = symbolFromReference(rightExpr);
            Comparison.Operator normalizedOp = (op == LESS_THAN) ? GREATER_THAN : GREATER_THAN_OR_EQUAL;
            return new OrderingInfo(leftTs, rightTs, normalizedOp, SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST);
        }

        // Pattern: r.ts >= l.ts  (right >= left)
        // Semantically: find min r.ts where r.ts >= l.ts (forward looking)
        // Keep as: GREATER_THAN_OR_EQUAL (build >= probe)
        if (leftExprFromRight && rightExprFromLeft && (op == GREATER_THAN_OR_EQUAL || op == GREATER_THAN)) {
            Symbol rightTs = symbolFromReference(leftExpr);
            Symbol leftTs = symbolFromReference(rightExpr);
            // Already normalized: op is GREATER_THAN or GREATER_THAN_OR_EQUAL
            return new OrderingInfo(leftTs, rightTs, op, SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST);
        }

        throw new TrinoException(NOT_SUPPORTED, "ASOF join inequality must compare a left-side and right-side symbol");
    }

    private boolean referencesOnly(Expression expression, Set<Symbol> allowed)
    {
        return allowed.containsAll(io.trino.sql.planner.SymbolsExtractor.extractUnique(expression));
    }

    private Symbol symbolFromReference(Expression expression)
    {
        checkArgument(expression instanceof io.trino.sql.ir.Reference, "Expected Reference but got: %s", expression);
        return Symbol.from((io.trino.sql.ir.Reference) expression);
    }

    private record OrderingInfo(
            Symbol leftOrderingSymbol,
            Symbol rightOrderingSymbol,
            Comparison.Operator inequalityOperator,
            SortOrder leftSortOrder,
            SortOrder rightSortOrder)
    {
        private OrderingInfo
        {
            requireNonNull(leftOrderingSymbol, "leftOrderingSymbol is null");
            requireNonNull(rightOrderingSymbol, "rightOrderingSymbol is null");
            requireNonNull(inequalityOperator, "inequalityOperator is null");
            requireNonNull(leftSortOrder, "leftSortOrder is null");
            requireNonNull(rightSortOrder, "rightSortOrder is null");
        }
    }
}
