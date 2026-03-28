package com.zhuanzhuan.lineage.storage;

import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public interface LineageStorage {
    void saveCapture(ExecutionCaptureEvent event);

    void saveLineage(NormalizedLineageResult result);

    default void saveBatch(List<ExecutionCaptureEvent> events, List<NormalizedLineageResult> results) {
        for (ExecutionCaptureEvent event : events) {
            saveCapture(event);
        }
        for (NormalizedLineageResult result : results) {
            saveLineage(result);
        }
    }

    default void clear() {
    }

    final class InMemoryLineageStorage implements LineageStorage {
        private final ConcurrentHashMap<String, ExecutionCaptureEvent> captureStore = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, NormalizedLineageResult> lineageStore = new ConcurrentHashMap<>();

        @Override
        public void saveCapture(ExecutionCaptureEvent event) {
            captureStore.putIfAbsent(event.getEventId(), event);
        }

        @Override
        public void saveLineage(NormalizedLineageResult result) {
            lineageStore.put(result.getEventId(), result);
        }

        public List<ExecutionCaptureEvent> captures() {
            List<ExecutionCaptureEvent> values = new ArrayList<>(captureStore.values());
            values.sort(Comparator.comparingLong(ExecutionCaptureEvent::getCaptureTimeEpochMs));
            return values;
        }

        public List<NormalizedLineageResult> results() {
            List<NormalizedLineageResult> values = new ArrayList<>(lineageStore.values());
            values.sort(Comparator.comparingLong(NormalizedLineageResult::getCaptureTimeEpochMs));
            return values;
        }

        @Override
        public void clear() {
            captureStore.clear();
            lineageStore.clear();
        }
    }
}
