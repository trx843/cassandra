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

package org.apache.cassandra.tcm.listeners;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;

public class PlacementsChangeListener implements ChangeListener
{
    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        if (shouldInvalidate(prev, next))
            StorageService.instance.invalidateLocalRanges();
    }

    private boolean shouldInvalidate(ClusterMetadata prev, ClusterMetadata next)
    {
        if (!prev.placements.lastModified().equals(next.placements.lastModified()) &&
            !prev.placements.equals(next.placements)) // <- todo should we update lastModified if the result is the same?
            return true;

        if (prev.schema.getKeyspaces().size() != next.schema.getKeyspaces().size())
            return true;

        // can't rely only on placements alone since we can move a ks from rf=1 to rf=3 and the rf=3 params might already exist in the placements:
        for (KeyspaceMetadata ksm : prev.schema.getKeyspaces())
        {
            KeyspaceMetadata newKsm = next.schema.getKeyspaceMetadata(ksm.name);
            if (newKsm == null || !ksm.params.equals(newKsm.params))
                return true;
        }
        return false;
    }
}
