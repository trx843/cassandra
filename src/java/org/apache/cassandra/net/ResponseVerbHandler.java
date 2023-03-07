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
package org.apache.cassandra.net;

import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

class ResponseVerbHandler implements IVerbHandler
{
    public static final ResponseVerbHandler instance = new ResponseVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ResponseVerbHandler.class);
    private static final Set<Verb> SKIP_CATCHUP_FOR = EnumSet.of(Verb.TCM_REPLAY_RSP,
                                                                 Verb.TCM_COMMIT_RSP,
                                                                 Verb.TCM_REPLICATION,
                                                                 Verb.TCM_NOTIFY_RSP,
                                                                 Verb.TCM_DISCOVER_RSP,
                                                                 Verb.TCM_INIT_MIG_RSP);

    @Override
    public void doVerb(Message message)
    {

        if (message.epoch().isAfter(ClusterMetadata.current().epoch) &&
            !SKIP_CATCHUP_FOR.contains(message.verb()) &&
            // Gossip stage is single-threaded, so we may end up in a deadlock with after-commit hook
            // that executes something on the gossip stage as well.
            !Stage.GOSSIP.executor().inExecutor())
        {
            boolean caughtUp = ClusterMetadataService.instance().maybeCatchup(message.epoch());
            if (caughtUp)
                logger.debug("Learned about next epoch {} from {} in {}", message.epoch(), message.from(), message.verb());
        }

        RequestCallbacks.CallbackInfo callbackInfo = MessagingService.instance().callbacks.remove(message.id(), message.from());
        if (callbackInfo == null)
        {
            String msg = "Callback already removed for {} (from {})";
            logger.trace(msg, message.id(), message.from());
            Tracing.trace(msg, message.id(), message.from());
            return;
        }

        long latencyNanos = approxTime.now() - callbackInfo.createdAtNanos;
        Tracing.trace("Processing response from {}", message.from());

        RequestCallback cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
            cb.onFailure(message.from(), (RequestFailureReason) message.payload);
        }
        else
        {
            MessagingService.instance().latencySubscribers.maybeAdd(cb, message.from(), latencyNanos, NANOSECONDS);
            cb.onResponse(message);
        }
    }
}
