/**
 *  Copyright 2020 - 2021 The JfrUnit authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.jfrunit;

import dev.morling.jfrunit.EnableEvent.StacktracePolicy;
import dev.morling.jfrunit.internal.SyncEvent;
import jdk.jfr.*;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class JfrEvents {

    private static final Logger LOGGER = System.getLogger(JfrEvents.class.getName());

    private Method testMethod;
    private Queue<RecordedEvent> events = new ConcurrentLinkedQueue<>();
    private AtomicLong sequence = new AtomicLong();
    private AtomicLong awaitEventsWatermark = new AtomicLong(-1);
    private Recording recording;

    public JfrEvents() {
    }

    void startRecordingEvents(String configurationName, List<EventConfiguration> enabledEvents, Method testMethod) {
        if (configurationName != null && !enabledEvents.isEmpty()) {
            throw new IllegalArgumentException("Either @EnableConfiguration or @EnableEvent may be given, but not both at the same time");
        }

        LOGGER.log(Level.INFO, "Starting recording");

        List<EventConfiguration> allEnabledEventTypes = matchEventTypes(enabledEvents);

        try {
            this.testMethod = testMethod;
            recording = startRecording(configurationName, allEnabledEventTypes);

            LOGGER.log(Level.INFO, "Recording started");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void stopRecordingEvents() {
        LOGGER.log(Level.INFO, "Stop recording: " + recording.getDestination());
        recording.stop();
        recording.close();
    }

    /**
     * Ensures all previously emitted events have been consumed.
     */
    public void awaitEvents() {
        SyncEvent event = new SyncEvent();
        event.begin();
        long seq = sequence.incrementAndGet();
        event.sequence = seq;
        event.cause = "awaiting events";
        event.commit();

        awaitEvents(seq);
    }

    public void reset() {
        awaitEvents();
        events.clear();
    }

    public Stream<RecordedEvent> getEvents() {
        return events.stream();
    }

    public Stream<RecordedEvent> filter(Predicate<RecordedEvent> predicate) {
        return events.stream().filter(predicate);
    }

    public Stream<RecordedEvent> stream() {
        return events.stream();
    }

    private Recording startRecording(String configurationName, List<EventConfiguration> enabledEvents) throws Exception {
        Recording recording;

        if (configurationName != null) {
            recording = new Recording(Configuration.getConfiguration(configurationName));
        } else {
            recording = new Recording();
            for (EventConfiguration enabledEvent : enabledEvents) {
                EventSettings settings = recording.enable(enabledEvent.name);
                if (enabledEvent.stackTrace == StacktracePolicy.INCLUDED) {
                    settings.withStackTrace();
                } else if (enabledEvent.stackTrace == StacktracePolicy.EXCLUDED) {
                    settings.withoutStackTrace();
                }

                if (enabledEvent.threshold != -1) {
                    settings.withThreshold(Duration.ofMillis(enabledEvent.threshold));
                }

                if (enabledEvent.period != -1) {
                    settings.withPeriod(Duration.ofMillis(enabledEvent.period));
                }
            }
        }

        recording.enable(SyncEvent.JFRUNIT_SYNC_EVENT_NAME);

        Path dumpDir = Files.createDirectories(Path.of(testMethod.getDeclaringClass().getProtectionDomain().getCodeSource().getLocation().toURI()).getParent().resolve("jfrunit"));
        dumpDir = Path.of(dumpDir.toString(), "test");
        recording.setDestination(dumpDir);
        recording.setToDisk(true);
        recording.setDumpOnExit(true);

        recording.start();
        return recording;
    }

    private boolean isSyncEvent(RecordedEvent re) {
        return re.getEventType().getName().equals(SyncEvent.JFRUNIT_SYNC_EVENT_NAME);
    }

    private boolean isInternalSleepEvent(RecordedEvent re) {
        return re.getEventType().getName().equals("jdk.ThreadSleep") &&
                re.getDuration("time").equals(Duration.ofMillis(100));
    }

    private List<EventConfiguration> matchEventTypes(List<EventConfiguration> enabledEvents) {
        List<EventConfiguration> allEvents = new ArrayList<>();
        List<EventType> allEventTypes = FlightRecorder.getFlightRecorder().getEventTypes();

        for (EventConfiguration event : enabledEvents) {
            if (event.name.contains("*")) {
                Pattern pattern = Pattern.compile(event.name.replace("*", ".*"));
                for (EventType eventType : allEventTypes) {
                    if (pattern.matcher(eventType.getName()).matches()) {
                        allEvents.add(new EventConfiguration(eventType.getName(), event.stackTrace, event.threshold, event.period));
                    }
                }
            } else {
                allEvents.add(event);
            }
        }

        return allEvents;
    }

    public void awaitEvents(long syncEventSequence) {
        while (!RecordingState.CLOSED.equals(recording.getState())) {
            try {
                recording.dump(recording.getDestination());
                RecordingFile recordingFile = new RecordingFile(recording.getDestination());
                boolean out = false;
                long eventCounter = -1;
                while (recordingFile.hasMoreEvents()) {
                    RecordedEvent recordedEvent = recordingFile.readEvent();
                    eventCounter++;
                    if (eventCounter <= awaitEventsWatermark.get()) {
                        continue;
                    } else {
                        awaitEventsWatermark.incrementAndGet();
                    }
                    consumeEvent(recordedEvent);
                    if (isSyncEvent(recordedEvent) && syncEventSequence <= recordedEvent.getLong("sequence")) {
                        out = true;
                    }
                }
                if (out) {
                    return;
                }
                Thread.sleep(100);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Failed to read recording.", e);
            }
        }
    }

    private void consumeEvent(RecordedEvent re) {
        if (!isInternalSleepEvent(re)) {
            events.add(re);
        }
    }
}
