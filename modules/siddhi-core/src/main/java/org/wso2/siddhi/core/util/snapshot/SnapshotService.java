/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.core.util.snapshot;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SnapshotableElementsHolder;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotService {
    private static final Logger LOGGER = Logger.getLogger(SnapshotService.class);
    private static final String SNAPSHOTABLE_STATES_KEY = "snapshotable.states";
    private static final SnapshotableElementsHolder snapshotableElementsHolder = new SnapshotableElementsHolder();
    private List<Snapshotable> snapshotableList = new ArrayList<Snapshotable>();
    private ExecutionPlanContext executionPlanContext;
    private static PersistenceStore persistenceStore;

    public SnapshotService(ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
    }

    public static void persistSnapshotableElements() {
        if (persistenceStore != null) {
            HashMap<String, Map<String, Object>> snapshots =
                    new HashMap<String, Map<String, Object>>(snapshotableElementsHolder.getSnapshotableElements().size());
            LOGGER.debug("Taking snapshots of snapshotable elements...");
            try {
                for (SnapshotableElement snapshotable : snapshotableElementsHolder.getSnapshotableElements()) {
                    snapshotable.freeze();
                    snapshots.put(snapshotable.getElementId(), snapshotable.currentState());
                }
                byte[] serializedSnapshots = ByteSerializer.OToB(snapshots);
                LOGGER.debug("Finished taking snapshots of snapshotable elements.");
                persistenceStore.save(SNAPSHOTABLE_STATES_KEY, String.valueOf(System.currentTimeMillis()), serializedSnapshots);
                SnapshotService.nofityReceiversOnSave(serializedSnapshots);
            } finally {
                for (SnapshotableElement snapshotable : snapshotableElementsHolder.getSnapshotableElements()) {
                    snapshotable.unfreeze();
                }
            }
        }
    }

    public static void restoreSnapshotableElements() {
        if (persistenceStore != null) {
            String revision = persistenceStore.getLastRevision(SNAPSHOTABLE_STATES_KEY);
            if (revision != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Restoring snapshotable elements revision: " + revision + " ...");
                }
                byte[] snapshot = persistenceStore.load(SNAPSHOTABLE_STATES_KEY, revision);
                HashMap<String, Map<String, Object>> snapshots =
                        (HashMap<String, Map<String, Object>>) ByteSerializer.BToO(snapshot);
                if (snapshots != null) {
                    for (Map.Entry<String, Map<String, Object>> entry : snapshots.entrySet()) {
                        String elementName = entry.getKey();
                        Map<String, Object> savedState = entry.getValue();
                        SnapshotableElement snapshotable = snapshotableElementsHolder.getSnapshotableElement(elementName);
                        if (snapshotable == null) {
                            SnapshotableElementsHolder.putExistingState(elementName, savedState);
                        } else {
                            try {
                                snapshotable.freeze();
                                snapshotable.restoreState(savedState);
                            } finally {
                                snapshotable.unfreeze();
                            }
                        }
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Restored snapshotable elements revision: " + revision);
                    }
                }
            }
        }
    }

    private static void nofityReceiversOnSave(byte[] snapshot) {
        HashMap<String, Map<String, Object>> snapshots =
                (HashMap<String, Map<String, Object>>) ByteSerializer.BToO(snapshot);
        for (SnapshotableElement snapshotable : snapshotableElementsHolder.getSnapshotableElements()) {
            if (snapshots != null && snapshots.get(snapshotable.getElementId()) != null) {
                snapshotable.onSave(snapshots.get(snapshotable.getElementId()));
            }
        }
    }

    public void addSnapshotable(Snapshotable snapshotable) {
        snapshotableList.add(snapshotable);
    }

    public byte[] snapshot() {
        HashMap<String, Object[]> snapshots = new HashMap<String, Object[]>(snapshotableList.size());
        LOGGER.debug("Taking snapshot ...");
        try {
            executionPlanContext.getThreadBarrier().lock();
            for (Snapshotable snapshotable : snapshotableList) {
                snapshots.put(snapshotable.getElementId(), snapshotable.currentState());
            }
        } finally {
            executionPlanContext.getThreadBarrier().unlock();
        }
        LOGGER.info("Snapshot taken of Execution Plan '" + executionPlanContext.getName() + "'");

        LOGGER.debug("Snapshot serialization started ...");
        byte[] serializedSnapshots = ByteSerializer.OToB(snapshots);
        LOGGER.debug("Snapshot serialization finished.");
        return serializedSnapshots;

    }

    public void restore(byte[] snapshot) {
        HashMap<String, Object[]> snapshots = (HashMap<String, Object[]>) ByteSerializer.BToO(snapshot);
        try {
            this.executionPlanContext.getThreadBarrier().lock();
            for (Snapshotable snapshotable : snapshotableList) {
                if (snapshots != null) {
                    snapshotable.restoreState(snapshots.get(snapshotable.getElementId()));
                }
            }
        } finally {
            executionPlanContext.getThreadBarrier().unlock();
        }
    }

    public static void setPersistenceStore(PersistenceStore persistenceStore) {
        SnapshotService.persistenceStore = persistenceStore;
    }

}
