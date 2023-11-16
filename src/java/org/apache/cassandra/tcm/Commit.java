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

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Replication;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.tcm.ClusterMetadataService.State.*;

public class Commit
{
    private static final Logger logger = LoggerFactory.getLogger(Commit.class);

    public static final Serializer serializer = new Serializer();

    private final Version metadataVersion = Version.V0;
    private final Entry.Id entryId;
    private final Transformation transform;
    private final Epoch lastKnown;

    public Commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
    {
        this.entryId = entryId;
        this.transform = transform;
        this.lastKnown = lastKnown;
    }

    public String toString()
    {
        return "Commit{" +
               "transformation=" + transform +
               ", lastKnown=" + lastKnown +
               '}';
    }

    static class Serializer implements IVersionedSerializer<Commit>
    {
        public void serialize(Commit t, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(t.metadataVersion.asInt());
            Entry.Id.serializer.serialize(t.entryId, out, t.metadataVersion);
            Transformation.serializer.serialize(t.transform, out, t.metadataVersion);
            Epoch.serializer.serialize(t.lastKnown, out, t.metadataVersion);
        }

        public Commit deserialize(DataInputPlus in, int version) throws IOException
        {
            Version metadataVersion = Version.fromInt(in.readInt());
            Entry.Id entryId = Entry.Id.serializer.deserialize(in, metadataVersion);
            Transformation transform = Transformation.serializer.deserialize(in, metadataVersion);
            Epoch lastKnown = Epoch.serializer.deserialize(in, metadataVersion);
            return new Commit(entryId, transform, lastKnown);
        }

        public long serializedSize(Commit t, int version)
        {
            return TypeSizes.sizeof(t.metadataVersion.asInt()) +
                   Transformation.serializer.serializedSize(t.transform, t.metadataVersion) +
                   Entry.Id.serializer.serializedSize(t.entryId, t.metadataVersion) +
                   Epoch.serializer.serializedSize(t.lastKnown, t.metadataVersion);
        }
    }

    public static interface Result
    {
        boolean isSuccess();
        boolean isFailure();

        default Success success()
        {
            return (Success) this;
        }

        default Failure failure()
        {
            return (Failure) this;
        }
        Serializer serializer = new Serializer();

        final class Success implements Result
        {
            public final Version metadataVersion = Version.V0;
            public final Epoch epoch;
            public final Replication replication;

            public Success(Epoch epoch, Replication replication)
            {
                this.epoch = epoch;
                this.replication = replication;
            }

            @Override
            public String toString()
            {
                return "Success{" +
                       "epoch=" + epoch +
                       ", replication=" + replication +
                       '}';
            }

            public boolean isSuccess()
            {
                return true;
            }

            public boolean isFailure()
            {
                return false;
            }
        }

        final class Failure implements Result
        {
            public final ExceptionCode code;
            public final String message;
            // Rejection means that we were able to linearize the operation,
            // but it was rejected by the internal logic of the transformation.
            public final boolean rejected;

            public Failure(Transformation.Rejected rejection)
            {
                this(rejection.code, rejection.reason, true);
            }

            public Failure(ExceptionCode code, String message)
            {
                this(code, message, false);
            }
            public Failure(ExceptionCode code, String message, boolean rejected)
            {
                this.code = code;
                this.message = message;
                this.rejected = rejected;
            }

            @Override
            public String toString()
            {
                return "Failure{" +
                       "code='" + code + '\'' +
                       "message='" + message + '\'' +
                       "rejected=" + rejected +
                       '}';
            }

            public boolean isSuccess()
            {
                return false;
            }

            public boolean isFailure()
            {
                return true;
            }
        }

        class Serializer implements IVersionedSerializer<Result>
        {
            private static final byte SUCCESS = 1;
            private static final byte REJECTED = 2;
            private static final byte FAILED = 3;
            @Override
            public void serialize(Result t, DataOutputPlus out, int version) throws IOException
            {
                if (t instanceof Success)
                {
                    out.writeByte(SUCCESS);
                    Version metadataVersion = t.success().metadataVersion;
                    out.writeUnsignedVInt32(metadataVersion.asInt());
                    Replication.serializer.serialize(t.success().replication, out, metadataVersion);
                    Epoch.serializer.serialize(t.success().epoch, out, metadataVersion);
                }
                else
                {
                    assert t instanceof Failure;
                    Failure failure = (Failure) t;
                    out.writeByte(failure.rejected ? REJECTED : FAILED);
                    out.writeUnsignedVInt32(failure.code.value);
                    out.writeUTF(failure.message);
                }
            }

            @Override
            public Result deserialize(DataInputPlus in, int version) throws IOException
            {
                int b = in.readByte();
                if (b == SUCCESS)
                {
                    Version metadataVersion = Version.fromInt(in.readUnsignedVInt32());
                    Replication delta = Replication.serializer.deserialize(in, metadataVersion);
                    Epoch epoch = Epoch.serializer.deserialize(in, metadataVersion);
                    return new Success(epoch, delta);
                }
                else
                {
                    return new Failure(ExceptionCode.fromValue(in.readUnsignedVInt32()),
                                       in.readUTF(),
                                       b == REJECTED);
                }
            }

            @Override
            public long serializedSize(Result t, int version)
            {
                long size = TypeSizes.BYTE_SIZE;
                if (t instanceof Success)
                {
                    Version metadataVersion = t.success().metadataVersion;
                    size += VIntCoding.computeUnsignedVIntSize(metadataVersion.asInt());
                    size += Replication.serializer.serializedSize(t.success().replication, metadataVersion);
                    size += Epoch.serializer.serializedSize(t.success().epoch, metadataVersion);
                }
                else
                {
                    assert t instanceof Failure;
                    size += VIntCoding.computeUnsignedVIntSize(((Failure) t).code.value);
                    size += TypeSizes.sizeof(((Failure)t).message);
                }
                return size;
            }
        }
    }

    @VisibleForTesting
    public static IVerbHandler<Commit> handlerForTests(Processor processor, Replicator replicator, BiConsumer<Message<?>, InetAddressAndPort> messagingService)
    {
        return new Handler(processor, replicator, messagingService, () -> LOCAL);
    }

    static class Handler implements IVerbHandler<Commit>
    {
        private final Processor processor;
        private final Replicator replicator;
        private final BiConsumer<Message<?>, InetAddressAndPort> messagingService;
        private final Supplier<ClusterMetadataService.State> cmsStateSupplier;

        Handler(Processor processor, Replicator replicator, Supplier<ClusterMetadataService.State> cmsStateSupplier)
        {
            this(processor, replicator, MessagingService.instance()::send, cmsStateSupplier);
        }

        Handler(Processor processor, Replicator replicator, BiConsumer<Message<?>, InetAddressAndPort> messagingService, Supplier<ClusterMetadataService.State> cmsStateSupplier)
        {
            this.processor = processor;
            this.replicator = replicator;
            this.messagingService = messagingService;
            this.cmsStateSupplier = cmsStateSupplier;
        }

        public void doVerb(Message<Commit> message) throws IOException
        {
            checkCMSState();
            logger.info("Received commit request {} from {}", message.payload, message.from());
            Retry.Deadline retryPolicy = Retry.Deadline.at(message.expiresAtNanos(), new Retry.Jitter(TCMMetrics.instance.commitRetries));
            Result result = processor.commit(message.payload.entryId, message.payload.transform, message.payload.lastKnown, retryPolicy);
            if (result.isSuccess())
            {
                Result.Success success = result.success();
                replicator.send(success, message.from());
                logger.info("Responding with full result {} to sender {}", result, message.from());
                // TODO: this response message can get lost; how do we re-discover this on the other side?
                // TODO: what if we have holes after replaying?
                messagingService.accept(message.responseWith(result), message.from());
            }
            else
            {
                Result.Failure failure = result.failure();
                messagingService.accept(message.responseWith(failure), message.from());
            }
        }

        private void checkCMSState()
        {
            switch (cmsStateSupplier.get())
            {
                case RESET:
                case LOCAL:
                    break;
                case REMOTE:
                    throw new NotCMSException("Not currently a member of the CMS, can't commit");
                case GOSSIP:
                    String msg = "Tried to commit when in gossip mode";
                    logger.error(msg);
                    throw new IllegalStateException(msg);
                default:
                    throw new IllegalStateException("Illegal state: " + cmsStateSupplier.get());
            }
        }
    }

    public interface Replicator
    {
        Replicator NO_OP = (a,b) -> {};
        void send(Result result, InetAddressAndPort source);
    }

    public static class DefaultReplicator implements Replicator
    {
        private final Supplier<Directory> directorySupplier;

        public DefaultReplicator(Supplier<Directory> directorySupplier)
        {
            this.directorySupplier = directorySupplier;
        }

        public void send(Result result, InetAddressAndPort source)
        {
            if (!result.isSuccess())
                return;

            Result.Success success = result.success();
            Directory directory = directorySupplier.get();

            // Filter the log entries from the commit result for the purposes of replicating to members of the cluster
            // other than the original submitter. We only need to include the sublist of entries starting at the one
            // which was newly committed. We exclude entries before that one as the submitter may have been lagging and
            // supplied a last known epoch arbitrarily in the past. We include entries after the first newly committed
            // one as there may have been a new period automatically triggered and we'd like to push that out to all
            // peers too. Of course, there may be other entries interspersed with these but it doesn't harm anything to
            // include those too, it may simply be redundant.
            Replication newlyCommitted = success.replication.retainFrom(success.epoch);
            assert !newlyCommitted.isEmpty() : String.format("Nothing to replicate after retaining epochs since %s from %s",
                                                             success.epoch, success.replication);

            for (NodeId peerId : directory.peerIds())
            {
                InetAddressAndPort endpoint = directory.endpoint(peerId);
                boolean upgraded = directory.version(peerId).isUpgraded();
                // Do not replicate to self and to the peer that has requested to commit this message
                if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()) ||
                    (source != null && source.equals(endpoint)) ||
                    !upgraded)
                {
                    continue;
                }

                logger.info("Replicating newly committed transformations up to {} to {}", newlyCommitted, endpoint);
                MessagingService.instance().send(Message.out(Verb.TCM_REPLICATION, newlyCommitted), endpoint);
            }
        }
    }

}
