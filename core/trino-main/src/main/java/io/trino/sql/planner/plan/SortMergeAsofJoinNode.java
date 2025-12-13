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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.ir.Comparison;
import io.trino.sql.planner.Symbol;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Represents an ASOF join implemented using sort-merge algorithm.
 * For each left row, finds the right row with matching equi-keys (if any) and
 * the closest timestamp satisfying the inequality condition.
 */
@Immutable
public class SortMergeAsofJoinNode
        extends PlanNode
{
    private final PlanNode left;
    private final PlanNode right;
    private final List<Symbol> outputSymbols;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final Symbol leftOrderingSymbol;
    private final Symbol rightOrderingSymbol;
    private final Comparison.Operator inequalityOperator;

    @JsonCreator
    public SortMergeAsofJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("criteria") List<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("leftOrderingSymbol") Symbol leftOrderingSymbol,
            @JsonProperty("rightOrderingSymbol") Symbol rightOrderingSymbol,
            @JsonProperty("inequalityOperator") Comparison.Operator inequalityOperator)
    {
        super(id);

        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
        this.criteria = ImmutableList.copyOf(requireNonNull(criteria, "criteria is null"));
        this.leftOrderingSymbol = requireNonNull(leftOrderingSymbol, "leftOrderingSymbol is null");
        this.rightOrderingSymbol = requireNonNull(rightOrderingSymbol, "rightOrderingSymbol is null");
        this.inequalityOperator = requireNonNull(inequalityOperator, "inequalityOperator is null");

        checkArgument(
                inequalityOperator == Comparison.Operator.LESS_THAN ||
                        inequalityOperator == Comparison.Operator.LESS_THAN_OR_EQUAL ||
                        inequalityOperator == Comparison.Operator.GREATER_THAN ||
                        inequalityOperator == Comparison.Operator.GREATER_THAN_OR_EQUAL,
                "inequalityOperator must be a comparison operator: %s", inequalityOperator);
        checkArgument(left.getOutputSymbols().contains(leftOrderingSymbol),
                "left does not contain leftOrderingSymbol: %s", leftOrderingSymbol);
        checkArgument(right.getOutputSymbols().contains(rightOrderingSymbol),
                "right does not contain rightOrderingSymbol: %s", rightOrderingSymbol);
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("right")
    public PlanNode getRight()
    {
        return right;
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("criteria")
    public List<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("leftOrderingSymbol")
    public Symbol getLeftOrderingSymbol()
    {
        return leftOrderingSymbol;
    }

    @JsonProperty("rightOrderingSymbol")
    public Symbol getRightOrderingSymbol()
    {
        return rightOrderingSymbol;
    }

    @JsonProperty("inequalityOperator")
    public Comparison.Operator getInequalityOperator()
    {
        return inequalityOperator;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAsofJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new SortMergeAsofJoinNode(
                getId(),
                newChildren.get(0),
                newChildren.get(1),
                outputSymbols,
                criteria,
                leftOrderingSymbol,
                rightOrderingSymbol,
                inequalityOperator);
    }
}
