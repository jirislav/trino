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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.operator.OperatorContext;
import io.trino.spi.Page;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Builder for accumulating build side (right side) pages for sort-merge ASOF join.
 * Pages are expected to be pre-sorted.
 */
public class SortMergeAsofJoinPagesBuilder
{
    private final OperatorContext operatorContext;
    private List<Page> pages;
    private boolean finished;
    private long estimatedSize;

    SortMergeAsofJoinPagesBuilder(OperatorContext operatorContext)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pages = new ArrayList<>();
    }

    public void addPage(Page page)
    {
        checkNotFinished();

        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }

        pages.add(page);
        estimatedSize += page.getRetainedSizeInBytes();
    }

    public DataSize getEstimatedSize()
    {
        return DataSize.ofBytes(estimatedSize);
    }

    public void compact()
    {
        checkNotFinished();

        long estimatedSize = 0L;
        for (Page page : pages) {
            page.compact();
            estimatedSize += page.getRetainedSizeInBytes();
        }
        this.estimatedSize = estimatedSize;
    }

    public SortMergeAsofJoinPages build()
    {
        checkNotFinished();

        finished = true;
        pages = ImmutableList.copyOf(pages);
        return new SortMergeAsofJoinPages(pages, getEstimatedSize(), operatorContext);
    }

    private void checkNotFinished()
    {
        checkState(!finished, "SortMergeAsofJoinPagesBuilder is already finished");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("estimatedSize", estimatedSize)
                .add("pageCount", pages.size())
                .toString();
    }
}
