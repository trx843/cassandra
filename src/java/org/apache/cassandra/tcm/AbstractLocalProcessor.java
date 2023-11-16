/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tcm;

import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.Replication;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.exceptions.ExceptionCode.SERVER_ERROR;

public abstract class AbstractLocalProcessor implements Processor
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosBackedProcessor.class);

    protected final LocalLog log;

    public AbstractLocalProcessor(LocalLog log)
    {
        this.log = log;
    }

    @Override
    public final Commit.Result commit(Entry.Id entryId, Transformation transform, final Epoch lastKnown)
    {
        Transformation.Result result;

        try
        {
            result = commitLocally(entryId, transform);
        }
        catch (Throwable e)
        {
            logger.error("Caught error while trying to perform a local commit", e);
            JVMStabilityInspector.inspectThrowable(e);
            return new Commit.Result.Failure(SERVER_ERROR, e.getMessage(), false);
        }

        if (result.isSuccess())
        {
            Replication replication;
            if (lastKnown == null || lastKnown.isDirectlyBefore(result.success().metadata.epoch))
            {
                replication = Replication.of(new Entry(entryId, result.success().metadata.epoch, transform));
            }
            else
            {
                replication = log.getCommittedEntries(lastKnown);
            }

            assert !replication.isEmpty();
            return new Commit.Result.Success(result.success().metadata.epoch,
                                             replication);
        }
        else
        {
            return new Commit.Result.Failure(result.rejected().code, result.rejected().reason, true);
        }
    }

    /**
     * Epoch returned by processor in the Result is _not_ guaranteed to be visible by the Follower by
     * the time when this method returns.
     */
    private Transformation.Result commitLocally(Entry.Id entryId, Transformation transform) throws InterruptedException, TimeoutException
    {
        // TODO: we need to add deadlines to CMS-local tasks, so that their retries would not exacerbate remote CMS ones
        Retry.Jitter jitter = new Retry.Jitter(TCMMetrics.instance.commitRetries);
        while (true)
        {
            ClusterMetadata previous = log.waitForHighestConsecutive();
            if (!previous.fullCMSMembers().contains(FBUtilities.getBroadcastAddressAndPort()))
                throw new IllegalStateException("Node is not a member of CMS anymore");
            Transformation.Result result = transform.execute(previous);
            // if we're rejected, just try to catch up to the latest distributed state
            if (result.isRejected())
            {
                Epoch replayed = fetchLogAndWait(jitter).epoch;

                // Retry if replay has changed the epoch, return rejection otherwise.
                if (!replayed.isAfter(previous.epoch))
                    return result.rejected();
                else
                    continue;
            }

            Epoch nextEpoch = result.success().metadata.epoch;
            // If metadata applies, try committing it to the log
            boolean applied = tryCommitOne(entryId, transform,
                                           previous.epoch, nextEpoch,
                                           previous.period, previous.nextPeriod(),
                                           result.success().metadata.lastInPeriod);
            if (applied)
            {
                logger.info("Committed {}. New epoch is {}", transform, nextEpoch);
                log.append(new Entry(entryId, nextEpoch, new Transformation.Executed(transform, result)));
                log.awaitAtLeast(nextEpoch);
                return result;
            }
            else if (jitter.reachedMax())
            {
                throw new IllegalStateException(String.format("Escaping infinite loop after %s tries. Current epoch: %s. Next epoch: %s.", jitter.currentTries(),
                                                              previous.epoch, nextEpoch));

            }
            else
            {
                // It may happen that we have raced with a different processor, in which case we need to catch up and retry.
                fetchLogAndWait(jitter);
                jitter.maybeSleep();
            }
        }
    }

    @Override
    public final ClusterMetadata fetchLogAndWait()
    {
        return fetchLogAndWait(new Retry.Jitter(TCMMetrics.instance.fetchLogRetries));
    }

    protected final ClusterMetadata fetchLogAndWait(Retry.Jitter retry)
    {
        while (!retry.reachedMax())
        {
            try
            {
                return tryReplayAndWait();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
            }
        }

        throw new IllegalStateException(String.format("Could not succeed with replay after %s tries.", retry.currentTries()));
    }

    protected abstract ClusterMetadata tryReplayAndWait();
    protected abstract boolean tryCommitOne(Entry.Id entryId, Transformation transform,
                                            Epoch previousEpoch, Epoch nextEpoch,
                                            long previousPeriod, long nextPeriod, boolean sealPeriod);
}