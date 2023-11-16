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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.tcm.membership.NodeState.LEAVING;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public class UnbootstrapAndLeave extends InProgressSequence<UnbootstrapAndLeave>
{
    private static final Logger logger = LoggerFactory.getLogger(UnbootstrapAndLeave.class);
    public static final Serializer serializer = new Serializer();

    public final Epoch latestModification;
    public final LockedRanges.Key lockKey;
    public final Transformation.Kind next;

    public final PrepareLeave.StartLeave startLeave;
    public final PrepareLeave.MidLeave midLeave;
    public final PrepareLeave.FinishLeave finishLeave;
    private final LeaveStreams streams;

    public UnbootstrapAndLeave(Epoch latestModification,
                               LockedRanges.Key lockKey,
                               Transformation.Kind next,
                               PrepareLeave.StartLeave startLeave,
                               PrepareLeave.MidLeave midLeave,
                               PrepareLeave.FinishLeave finishLeave,
                               LeaveStreams streams)
    {
        this.latestModification = latestModification;
        this.lockKey = lockKey;
        this.next = next;
        this.startLeave = startLeave;
        this.midLeave = midLeave;
        this.finishLeave = finishLeave;
        this.streams = streams;
    }

    /**
     * Entrypoint to begin node decommission process.
     *
     * @param shutdownNetworking if set to true, will also shut down networking on completion
     * @param force if set to true, will decommission the node even if this would mean there will be not enough nodes
     *              to satisfy replication factor
     */
    public static void decommission(boolean shutdownNetworking, boolean force)
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't decommission until that is done.");

        ClusterMetadata metadata = ClusterMetadata.current();

        StorageService.Mode mode = StorageService.instance.operationMode();
        if (!EnumSet.of(StorageService.Mode.LEAVING, StorageService.Mode.NORMAL).contains(mode))
            throw new UnsupportedOperationException(String.format("Node in %s state; wait for status to become normal", mode));
        logger.debug("DECOMMISSIONING");

        NodeId self = metadata.myNodeId();

        ReconfigureCMS.maybeReconfigureCMS(metadata, getBroadcastAddressAndPort());
        InProgressSequence<?> inProgress = metadata.inProgressSequences.get(self);

        if (inProgress == null)
        {
            logger.info("starting decom with {} {}", metadata.epoch, self);
            ClusterMetadataService.instance().commit(new PrepareLeave(self,
                                                                      force,
                                                                      ClusterMetadataService.instance().placementProvider(),
                                                                      LeaveStreams.Kind.UNBOOTSTRAP),
                                                     (metadata_) -> null,
                                                     (metadata_, code, reason) -> {
                                                         InProgressSequence<?> sequence = metadata_.inProgressSequences.get(self);
                                                         // We might have discovered a sequence we ourselves committed but got no response for
                                                         if (sequence == null || sequence.kind() != InProgressSequences.Kind.LEAVE)
                                                         {
                                                             throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting leave sequence.",
                                                                                                           reason));
                                                         }
                                                         return null;
                                                     });
        }
        else if (!InProgressSequences.isLeave(inProgress))
        {
            throw new IllegalArgumentException("Can not decommission a node that has an in-progress sequence");
        }

        InProgressSequences.finishInProgressSequences(self);
        if (shutdownNetworking)
            StorageService.instance.shutdownNetworking();
    }

    /**
     * Entrypoint to begin node removal process
     *
     * @param toRemove id of the node to remove
     * @param force if set to true, will remove the node even if this would mean there will be not enough nodes
     *              to satisfy replication factor
     */
    public static void removeNode(NodeId toRemove, boolean force)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (toRemove.equals(metadata.myNodeId()))
            throw new UnsupportedOperationException("Cannot remove self");
        InetAddressAndPort endpoint = metadata.directory.endpoint(toRemove);
        if (endpoint == null)
            throw new UnsupportedOperationException("Host ID not found.");
        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");

        NodeState removeState = metadata.directory.peerState(toRemove);
        if (removeState == null)
            throw new UnsupportedOperationException("Node to be removed is not a member of the token ring");
        if (removeState == LEAVING)
            logger.warn("Node {} is already leaving or being removed, continuing removal anyway", endpoint);

        if (metadata.inProgressSequences.contains(toRemove))
            throw new UnsupportedOperationException("Can not remove a node that has an in-progress sequence");

        ReconfigureCMS.maybeReconfigureCMS(metadata, endpoint);

        logger.info("starting removenode with {} {}", metadata.epoch, toRemove);

        ClusterMetadataService.instance().commit(new PrepareLeave(toRemove,
                                                                  force,
                                                                  ClusterMetadataService.instance().placementProvider(),
                                                                  LeaveStreams.Kind.REMOVENODE),
                                                 (metadata_) -> null,
                                                 (metadata_, code, reason) -> {
                                                     InProgressSequence<?> sequence = metadata_.inProgressSequences.get(toRemove);
                                                     // We might have discovered a startup sequence we ourselves committed but got no response for
                                                     if (sequence == null || sequence.kind() != InProgressSequences.Kind.REMOVE)
                                                     {
                                                         throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting removenode sequence.",
                                                                                                       reason));
                                                     }
                                                     return null;
                                                 });
        InProgressSequences.finishInProgressSequences(toRemove);
    }

    @Override
    public UnbootstrapAndLeave advance(Epoch waitFor)
    {
        return new UnbootstrapAndLeave(waitFor, lockKey, stepFollowing(next),
                                       startLeave, midLeave, finishLeave, streams);
    }

    @Override
    public Transformation.Kind nextStep()
    {
        return next;
    }

    @Override
    public InProgressSequences.Kind kind()
    {
        switch (streams.kind())
        {
            case UNBOOTSTRAP:
                return InProgressSequences.Kind.LEAVE;
            case REMOVENODE:
                return InProgressSequences.Kind.REMOVE;
            default:
                throw new IllegalStateException("Invalid stream kind: "+streams.kind());
        }
    }

    @Override
    public ProgressBarrier barrier()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        LockedRanges.AffectedRanges affectedRanges = metadata.lockedRanges.locked.get(lockKey);
        Location location = metadata.directory.location(startLeave.nodeId());
        if (kind() == InProgressSequences.Kind.REMOVE)
            return new ProgressBarrier(latestModification, location, affectedRanges, (e) -> !e.equals(metadata.directory.endpoint(startLeave.nodeId())));
        else
            return new ProgressBarrier(latestModification, location, affectedRanges);
    }

    @Override
    public SequenceState executeNext()
    {
        switch (next)
        {
            case START_LEAVE:
                try
                {
                    DatabaseDescriptor.getSeverityDuringDecommission().ifPresent(DynamicEndpointSnitch::addSeverity);
                    commit(startLeave);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                break;
            case MID_LEAVE:
                try
                {
                    streams.execute(startLeave.nodeId(),
                                    startLeave.delta(),
                                    midLeave.delta(),
                                    finishLeave.delta());
                    commit(midLeave);
                }
                catch (ExecutionException e)
                {
                    StorageService.instance.markDecommissionFailed();
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.error("Error while decommissioning node: {}", e.getCause().getMessage());
                    throw new RuntimeException("Error while decommissioning node: " + e.getCause().getMessage());
                }
                catch (Throwable t)
                {
                    logger.warn("Error committing midLeave", t);
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                break;
            case FINISH_LEAVE:
                try
                {
                    commit(finishLeave);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                break;
            default:
                return error(new IllegalStateException("Can't proceed with leave from " + next));
        }

        return continuable();
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;
        switch (next)
        {
            // need to undo MID_LEAVE and START_LEAVE, but PrepareLeave doesn't affect placement
            case FINISH_LEAVE:
                placements = midLeave.inverseDelta().apply(metadata.nextEpoch(), placements);
            case MID_LEAVE:
            case START_LEAVE:
                placements = startLeave.inverseDelta().apply(metadata.nextEpoch(), placements);
                break;
            default:
                throw new IllegalStateException("Can't revert leave from " + next);
        }
        LockedRanges newLockedRanges = metadata.lockedRanges.unlock(lockKey);
        return metadata.transformer()
                       .with(placements)
                       .with(newLockedRanges)
                       .withNodeState(startLeave.nodeId(), NodeState.JOINED);
    }

    @Override
    protected Transformation.Kind stepFollowing(Transformation.Kind kind)
    {
        if (kind == null)
            return null;

        switch (kind)
        {
            case START_LEAVE:
                return Transformation.Kind.MID_LEAVE;
            case MID_LEAVE:
                return Transformation.Kind.FINISH_LEAVE;
            case FINISH_LEAVE:
                return null;
            default:
                throw new IllegalStateException(String.format("Step %s is not a part of %s sequence", kind, kind()));
        }
    }

    @Override
    protected InProgressSequences.SequenceKey sequenceKey()
    {
        return startLeave.nodeId();
    }

    public String status()
    {
        return String.format("step: %s, streams: %s", next, streams.status());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnbootstrapAndLeave that = (UnbootstrapAndLeave) o;
        return Objects.equals(startLeave, that.startLeave) &&
               Objects.equals(midLeave, that.midLeave) &&
               Objects.equals(finishLeave, that.finishLeave) &&
               Objects.equals(latestModification, that.latestModification) &&
               Objects.equals(lockKey, that.lockKey) &&
               next == that.next;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(startLeave, midLeave, finishLeave, latestModification, lockKey, next);
    }

    @Override
    public String toString()
    {
        return "UnbootstrapAndLeavePlan{" +
               "lastModified=" + latestModification +
               ", lockKey=" + lockKey +
               ", startLeave=" + startLeave +
               ", midLeave=" + midLeave +
               ", finishLeave=" + finishLeave +
               ", next=" + next +
               '}';
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, UnbootstrapAndLeave>
    {
        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
        {
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) t;

            Epoch.serializer.serialize(plan.latestModification, out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);
            VIntCoding.writeUnsignedVInt32(plan.streams.kind().ordinal(), out);

            PrepareLeave.StartLeave.serializer.serialize(plan.startLeave, out, version);
            PrepareLeave.MidLeave.serializer.serialize(plan.midLeave, out, version);
            PrepareLeave.FinishLeave.serializer.serialize(plan.finishLeave, out, version);
        }

        public UnbootstrapAndLeave deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch barrier = Epoch.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);

            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            LeaveStreams.Kind streamKind = LeaveStreams.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            PrepareLeave.StartLeave startLeave = PrepareLeave.StartLeave.serializer.deserialize(in, version);
            PrepareLeave.MidLeave midLeave = PrepareLeave.MidLeave.serializer.deserialize(in, version);
            PrepareLeave.FinishLeave finishLeave = PrepareLeave.FinishLeave.serializer.deserialize(in, version);

            return new UnbootstrapAndLeave(barrier, lockKey, next,
                                           startLeave, midLeave, finishLeave,
                                           streamKind.supplier.get());
        }

        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) t;
            long size = Epoch.serializer.serializedSize(plan.latestModification, version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());
            size += VIntCoding.computeVIntSize(plan.streams.kind().ordinal());
            size += PrepareLeave.StartLeave.serializer.serializedSize(plan.startLeave, version);
            size += PrepareLeave.StartLeave.serializer.serializedSize(plan.midLeave, version);
            size += PrepareLeave.StartLeave.serializer.serializedSize(plan.finishLeave, version);
            return size;
        }
    }
}
