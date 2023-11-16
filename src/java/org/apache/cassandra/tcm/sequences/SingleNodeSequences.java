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

import java.util.Collections;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;

import static org.apache.cassandra.tcm.membership.NodeState.LEAVING;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.LEAVE;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.MOVE;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

/**
 * This exists simply to group the static entrypoints to sequences that modify a single node
 * e.g. decommission, remove, move
 */
public interface SingleNodeSequences
{
    Logger logger = LoggerFactory.getLogger(SingleNodeSequences.class);

    /**
     * Entrypoint to begin node decommission process.
     *
     * @param shutdownNetworking if set to true, will also shut down networking on completion
     * @param force if set to true, will decommission the node even if this would mean there will be not enough nodes
     *              to satisfy replication factor
     */
    static void decommission(boolean shutdownNetworking, boolean force)
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
        MultiStepOperation<?> inProgress = metadata.inProgressSequences.get(self);

        if (inProgress == null)
        {
            logger.info("starting decom with {} {}", metadata.epoch, self);
            ClusterMetadataService.instance().commit(new PrepareLeave(self,
                                                                      force,
                                                                      ClusterMetadataService.instance().placementProvider(),
                                                                      LeaveStreams.Kind.UNBOOTSTRAP),
                                                     (metadata_) -> null,
                                                     (metadata_, code, reason) -> {
                                                         MultiStepOperation<?> sequence = metadata_.inProgressSequences.get(self);
                                                         // We might have discovered a sequence we ourselves committed but got no response for
                                                         if (sequence == null || sequence.kind() != LEAVE)
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
    static void removeNode(NodeId toRemove, boolean force)
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
                                                     MultiStepOperation<?> sequence = metadata_.inProgressSequences.get(toRemove);
                                                     // We might have discovered a startup sequence we ourselves committed but got no response for
                                                     if (sequence == null || sequence.kind() != MultiStepOperation.Kind.REMOVE)
                                                     {
                                                         throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting removenode sequence.",
                                                                                                       reason));
                                                     }
                                                     return null;
                                                 });
        InProgressSequences.finishInProgressSequences(toRemove);
    }

    /**
     * move the node to new token or find a new token to boot to according to load
     *
     * @param newToken new token to boot to, or if null, find balanced token to boot to
     */
    public static void move(Token newToken)
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't move until that is done.");

        if (newToken == null)
            throw new IllegalArgumentException("Can't move to the undefined (null) token.");

        if (ClusterMetadata.current().tokenMap.tokens().contains(newToken))
            throw new IllegalArgumentException(String.format("target token %s is already owned by another node.", newToken));

        // address of the current node
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();
        // This doesn't make any sense in a vnodes environment.
        if (metadata.tokenMap.tokens(self).size() > 1)
        {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }

        ClusterMetadataService.instance().commit(new PrepareMove(self,
                                                                 Collections.singleton(newToken),
                                                                 ClusterMetadataService.instance().placementProvider(),
                                                                 true),
                                                 (metadata_) -> null,
                                                 (metadata_, code, reason) -> {
                                                     MultiStepOperation<?> sequence = metadata_.inProgressSequences.get(self);
                                                     // We might have discovered a startup sequence we ourselves committed but got no response for
                                                     if (sequence == null || sequence.kind() != MOVE)
                                                     {
                                                         throw new IllegalStateException(String.format("Can not commit event to metadata service: %s. Interrupting leave sequence.",
                                                                                                       reason));
                                                     }
                                                     return null;
                                                 });
        InProgressSequences.finishInProgressSequences(self);

        if (logger.isDebugEnabled())
            logger.debug("Successfully moved to new token {}", StorageService.instance.getLocalTokens().iterator().next());
    }

}
