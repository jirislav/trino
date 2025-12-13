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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of SortMergeAsofJoinBridge that uses SettableFuture
 * to coordinate between build and probe operators.
 */
public final class SortMergeAsofJoinPagesSupplier
        implements SortMergeAsofJoinBridge
{
    private final SettableFuture<SortMergeAsofJoinPages> pagesFuture = SettableFuture.create();
    private final SettableFuture<Void> pagesNoLongerNeeded = SettableFuture.create();

    @Override
    public ListenableFuture<SortMergeAsofJoinPages> getPagesFuture()
    {
        return transformAsync(pagesFuture, Futures::immediateFuture, directExecutor());
    }

    @Override
    public ListenableFuture<Void> setPages(SortMergeAsofJoinPages pages)
    {
        requireNonNull(pages, "pages is null");
        // For single-node execution, only one build operator should call setPages.
        // The guard in SortMergeAsofJoinBuildOperator.finish() ensures each operator
        // only calls this once. If called multiple times, subsequent calls are ignored.
        pagesFuture.set(pages);
        return pagesNoLongerNeeded;
    }

    @Override
    public void destroy()
    {
        // Let the build operator know that pages are no longer needed
        pagesNoLongerNeeded.set(null);
    }
}
