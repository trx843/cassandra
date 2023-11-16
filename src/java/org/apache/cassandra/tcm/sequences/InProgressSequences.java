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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.membership.NodeId;

import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.tcm.sequences.InProgressSequences.Kind.LEAVE;
import static org.apache.cassandra.tcm.serialization.Version.V2;

public class InProgressSequences implements MetadataValue<InProgressSequences>
{
    public static final Serializer serializer = new Serializer();

    public static InProgressSequences EMPTY = new InProgressSequences(Epoch.EMPTY, ImmutableMap.of());
    private final ImmutableMap<SequenceKey, InProgressSequence<?>> state;
    private final Epoch lastModified;

    private InProgressSequences(Epoch lastModified, ImmutableMap<SequenceKey, InProgressSequence<?>> state)
    {
        this.lastModified = lastModified;
        this.state = state;
    }

    public static void finishInProgressSequences(SequenceKey sequenceKey)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        while (true)
        {
            InProgressSequence<?> sequence = metadata.inProgressSequences.get(sequenceKey);
            if (sequence == null)
                break;
            if (isLeave(sequence))
                StorageService.instance.maybeInitializeServices();
            if (resume(sequence))
                metadata = ClusterMetadata.current();
            else
                return;
        }
    }

    public static boolean cancelInProgressSequences(String sequenceOwner, String expectedSequenceKind)
    {
        NodeId owner = NodeId.fromString(sequenceOwner);
        InProgressSequence<?> seq = ClusterMetadata.current().inProgressSequences.get(owner);
        if (seq == null)
            throw new IllegalArgumentException("No in progress sequence for "+sequenceOwner);
        Kind expectedKind = Kind.valueOf(expectedSequenceKind);
        if (seq.kind() != expectedKind)
            throw new IllegalArgumentException("No in progress sequence of kind " + expectedKind + " for " + owner + " (only " + seq.kind() +" in progress)");

        return StorageService.cancelInProgressSequences(owner);
    }

    @Override
    public InProgressSequences withLastModified(Epoch epoch)
    {
        return new InProgressSequences(epoch, state);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public boolean contains(SequenceKey nodeId)
    {
        return state.containsKey(nodeId);
    }

    public InProgressSequence<?> get(SequenceKey nodeId)
    {
        return state.get(nodeId);
    }

    public boolean isEmpty()
    {
        return state.isEmpty();
    }

    public InProgressSequences with(SequenceKey nodeId, InProgressSequence<?> sequence)
    {
        ImmutableMap.Builder<SequenceKey, InProgressSequence<?>> builder = ImmutableMap.builder();
        builder.put(nodeId, sequence);
        for (Map.Entry<SequenceKey, InProgressSequence<?>> e : state.entrySet())
        {
            if (e.getKey().equals(nodeId))
                continue;
            builder.put(e.getKey(), e.getValue());
        }
        return new InProgressSequences(lastModified, builder.build());
    }

    public InProgressSequences with(SequenceKey nodeId, Function<InProgressSequence<?>, InProgressSequence<?>> update)
    {
        ImmutableMap.Builder<SequenceKey, InProgressSequence<?>> builder = ImmutableMap.builder();

        for (Map.Entry<SequenceKey, InProgressSequence<?>> e : state.entrySet())
        {
            if (e.getKey().equals(nodeId))
                builder.put(e.getKey(), update.apply(e.getValue()));
            else
                builder.put(e.getKey(), e.getValue());
        }
        return new InProgressSequences(lastModified, builder.build());
    }

    public InProgressSequences without(SequenceKey nodeId)
    {
        ImmutableMap.Builder<SequenceKey, InProgressSequence<?>> builder = ImmutableMap.builder();
        boolean removed = false;
        for (Map.Entry<SequenceKey, InProgressSequence<?>> e : state.entrySet())
        {
            if (e.getKey().equals(nodeId))
                removed = true;
            else
                builder.put(e.getKey(), e.getValue());
        }
        assert removed : String.format("Expected to remove node %s, but it wasn't found in in-progress sequences", nodeId);
        return new InProgressSequences(lastModified, builder.build());

    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InProgressSequences that = (InProgressSequences) o;
        return Objects.equals(state, that.state) && Objects.equals(lastModified, that.lastModified);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, lastModified);
    }

    public static Set<Kind> STARTUP_SEQUENCE_KINDS = ImmutableSet.of(InProgressSequences.Kind.JOIN, InProgressSequences.Kind.REPLACE);

    public enum Kind
    {
        JOIN_OWNERSHIP_GROUP(AddToCMS.serializer),

        JOIN(BootstrapAndJoin.serializer),
        MOVE(Move.serializer),
        REPLACE(BootstrapAndReplace.serializer),
        LEAVE(UnbootstrapAndLeave.serializer),
        REMOVE(UnbootstrapAndLeave.serializer),

        RECONFIGURE_CMS(ReconfigureCMS.serializer)
        ;

        public final AsymmetricMetadataSerializer<InProgressSequence<?>, ? extends InProgressSequence<?>> serializer;

        Kind(AsymmetricMetadataSerializer<InProgressSequence<?>, ? extends InProgressSequence<?>> serializer)
        {
            this.serializer = serializer;
        }
    }

    @VisibleForTesting
    public static BiFunction<InProgressSequence<?>, SequenceState, SequenceState> listener = (s, o) -> o;

    @VisibleForTesting
    public static BiFunction<InProgressSequence<?>, SequenceState, SequenceState> replaceListener(BiFunction<InProgressSequence<?>, SequenceState, SequenceState> newListener)
    {
        BiFunction<InProgressSequence<?>, SequenceState, SequenceState> prev = listener;
        listener = newListener;
        return prev;
    }

    public static boolean resume(InProgressSequence<?> sequence)
    {
        SequenceState state;
        if (sequence.barrier().await())
            state = listener.apply(sequence, sequence.executeNext());
        else
            state = listener.apply(sequence, SequenceState.blocked());

        if (state.isError())
            throw ((SequenceState.Error)state).cause();

        return state.isContinuable();
    }

    public static boolean isLeave(InProgressSequence<?> sequence)
    {
        return sequence.kind() == LEAVE;
    }

    public static class Serializer implements MetadataSerializer<InProgressSequences>
    {
        public void serialize(InProgressSequences t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            out.writeInt(t.state.size());
            for (Map.Entry<SequenceKey, InProgressSequence<?>> entry : t.state.entrySet())
            {
                if (Version.UNKNOWN.isBefore(V2))
                {
                    NodeId.serializer.serialize((NodeId) entry.getKey(), out, version);
                    InProgressSequence<?> seq = entry.getValue();
                    out.writeUTF(seq.kind().name());
                    entry.getValue().kind().serializer.serialize(seq, out, version);
                }
                else
                {
                    // Starting V2, we serialize the sequence first since we rely on the type during deserialization
                    InProgressSequence<?> seq = entry.getValue();
                    out.writeUTF(seq.kind().name());
                    entry.getValue().kind().serializer.serialize(seq, out, version);
                    MetadataSerializer<SequenceKey> keySerializer = (MetadataSerializer<SequenceKey>) seq.keySerializer();
                    keySerializer.serialize(entry.getKey(), out, version);
                }
            }
        }

        public InProgressSequences deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            int ipsSize = in.readInt();
            ImmutableMap.Builder<SequenceKey, InProgressSequence<?>> res = ImmutableMap.builder();
            for (int i = 0; i < ipsSize; i++)
            {
                if (Version.UNKNOWN.isBefore(V2))
                {
                    NodeId nodeId = NodeId.serializer.deserialize(in, version);
                    Kind kind = Kind.valueOf(in.readUTF());
                    InProgressSequence<?> ips = kind.serializer.deserialize(in, version);
                    res.put(nodeId, ips);
                }
                else
                {
                    Kind kind = Kind.valueOf(in.readUTF());
                    InProgressSequence<?> ips = kind.serializer.deserialize(in, version);
                    SequenceKey key = ips.keySerializer().deserialize(in, version);
                    res.put(key, ips);
                }
            }
            return new InProgressSequences(lastModified, res.build());
        }

        public long serializedSize(InProgressSequences t, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.lastModified, version);
            size += sizeof(t.state.size());
            for (Map.Entry<SequenceKey, InProgressSequence<?>> entry : t.state.entrySet())
            {
                if (Version.UNKNOWN.isBefore(V2))
                {
                    size += NodeId.serializer.serializedSize((NodeId) entry.getKey(), version);
                    InProgressSequence<?> seq = entry.getValue();
                    size += sizeof(seq.kind().name());
                    size += entry.getValue().kind().serializer.serializedSize(seq, version);
                }
                else
                {
                    InProgressSequence<?> seq = entry.getValue();
                    size += sizeof(seq.kind().name());
                    size += entry.getValue().kind().serializer.serializedSize(seq, version);
                    MetadataSerializer<SequenceKey> keySerializer = (MetadataSerializer<SequenceKey>) seq.keySerializer();
                    size += keySerializer.serializedSize(entry.getKey(), version);
                }
            }
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "InProgressSequences{" +
               "lastModified=" + lastModified +
               ", state=" + state +
               '}';
    }

    /**
     * Opaque key identifying the sequence
     */
    public interface SequenceKey
    {
    }
}
