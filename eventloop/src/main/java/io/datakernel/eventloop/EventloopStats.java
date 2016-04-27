/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.eventloop;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.util.Stopwatch;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class EventloopStats {

	private static final class DurationRunnable {
		private Runnable runnable;
		private long duration;

		void reset() {
			duration = 0;
			runnable = null;
		}

		void update(Runnable runnable, long duration) {
			this.duration = duration;
			this.runnable = runnable;
		}

		long getDuration() {
			return duration;
		}

		@Override
		public String toString() {
			return (runnable == null) ? "" : runnable.getClass().getName() + ": " + duration;
		}
	}

	private static final class StackTrace {
		private final StackTraceElement[] stackTraceElements;

		public StackTrace(StackTraceElement[] stackTraceElements) {
			this.stackTraceElements = stackTraceElements;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof StackTrace)) return false;

			StackTrace that = (StackTrace) o;

			return Arrays.equals(stackTraceElements, that.stackTraceElements);
		}

		@Override
		public int hashCode() {
			return stackTraceElements != null ? Arrays.hashCode(stackTraceElements) : 0;
		}
	}

	private final ValueStats selectorSelectTime = new ValueStats();
	private final ValueStats businessLogicTime = new ValueStats();
	private final EventStats selectedKeys = new EventStats();
	private final EventStats invalidKeys = new EventStats();
	private final EventStats acceptKeys = new EventStats();
	private final EventStats connectKeys = new EventStats();
	private final EventStats readKeys = new EventStats();
	private final EventStats writeKeys = new EventStats();
	private final EventStats localTasks = new EventStats();
	private final EventStats concurrentTasks = new EventStats();
	private final EventStats scheduledTasks = new EventStats();

	private final ValueStats localTaskDuration = new ValueStats();
	private final DurationRunnable lastLongestLocalRunnable = new DurationRunnable();
	private final ValueStats concurrentTaskDuration = new ValueStats();
	private final DurationRunnable lastLongestConcurrentRunnable = new DurationRunnable();
	private final ValueStats scheduledTaskDuration = new ValueStats();
	private final DurationRunnable lastLongestScheduledRunnable = new DurationRunnable();

	private final ValueStats selectedKeysTime = new ValueStats();
	private final ValueStats localTasksTime = new ValueStats();
	private final ValueStats concurrentTasksTime = new ValueStats();
	private final ValueStats scheduledTasksTime = new ValueStats();

	private final Map<StackTrace, ExceptionStats> fatalErrors = new HashMap<>();
	private final ExceptionStats ioErrors = new ExceptionStats();

	public void updateBusinessLogicTime(long businessLogicTime) {
		this.businessLogicTime.recordValue((int) businessLogicTime);
	}

	public void updateSelectorSelectTime(long selectorSelectTime) {
		this.selectorSelectTime.recordValue((int) selectorSelectTime);
	}

	public void updateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys) {
		this.selectedKeys.recordEvents(lastSelectedKeys);
		this.invalidKeys.recordEvents(invalidKeys);
		this.acceptKeys.recordEvents(acceptKeys);
		this.connectKeys.recordEvents(connectKeys);
		this.readKeys.recordEvents(readKeys);
		this.writeKeys.recordEvents(writeKeys);
	}

	public void updateSelectedKeysTimeStats(@Nullable Stopwatch sw) {
		if (sw != null)
			selectedKeysTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
	}

	private void updateTaskDuration(ValueStats counter, DurationRunnable longestCounter, Runnable runnable, @Nullable Stopwatch sw) {
		if (sw != null) {
			int elapsed = (int) sw.elapsed(TimeUnit.MICROSECONDS);
			counter.recordValue(elapsed);
			if (elapsed > longestCounter.getDuration()) {
				longestCounter.update(runnable, elapsed);
			}
		}
	}

	public void updateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(localTaskDuration, lastLongestLocalRunnable, runnable, sw);
	}

	public void updateLocalTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			localTasksTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		localTasks.recordEvents(newTasks);
	}

	public void updateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(concurrentTaskDuration, lastLongestConcurrentRunnable, runnable, sw);
	}

	public void updateConcurrentTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			concurrentTasksTime.recordValue((int) sw.elapsed(TimeUnit.MICROSECONDS));
		concurrentTasks.recordEvents(newTasks);
	}

	public void updateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(scheduledTaskDuration, lastLongestScheduledRunnable, runnable, sw);
	}

	public void updateScheduledTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			scheduledTasksTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		scheduledTasks.recordEvents(newTasks);
	}

	public void recordFatalError(Throwable throwable, Object causedObject) {
		StackTrace stackTrace = new StackTrace(throwable.getStackTrace());
		if (!fatalErrors.containsKey(stackTrace)) {
			fatalErrors.put(stackTrace, new ExceptionStats());
		}
		ExceptionStats stats = fatalErrors.get(stackTrace);
		stats.recordException(throwable, causedObject);
	}

	public void recordIoError(Throwable throwable, Object causedObject) {
		ioErrors.recordException(throwable, causedObject);
	}

	public void resetStats() {
		selectorSelectTime.resetStats();
		businessLogicTime.resetStats();

		selectedKeys.resetStats();
		invalidKeys.resetStats();
		acceptKeys.resetStats();
		connectKeys.resetStats();
		readKeys.resetStats();
		writeKeys.resetStats();

		localTasks.resetStats();
		concurrentTasks.resetStats();
		scheduledTasks.resetStats();

		localTaskDuration.resetStats();
		concurrentTaskDuration.resetStats();
		scheduledTaskDuration.resetStats();

		selectedKeysTime.resetStats();
		localTasksTime.resetStats();
		concurrentTasksTime.resetStats();
		scheduledTasksTime.resetStats();

		fatalErrors.clear();
		ioErrors.resetStats();

		lastLongestLocalRunnable.reset();
		lastLongestConcurrentRunnable.reset();
		lastLongestScheduledRunnable.reset();
	}

	@JmxAttribute
	public ValueStats getSelectorSelectTime() {
		return selectorSelectTime;
	}

	@JmxAttribute
	public ValueStats getBusinessLogicTime() {
		return businessLogicTime;
	}

	@JmxAttribute
	public EventStats getSelectedKeys() {
		return selectedKeys;
	}

	@JmxAttribute
	public EventStats getInvalidKeys() {
		return invalidKeys;
	}

	@JmxAttribute
	public EventStats getAcceptKeys() {
		return acceptKeys;
	}

	@JmxAttribute
	public EventStats getConnectKeys() {
		return connectKeys;
	}

	@JmxAttribute
	public EventStats getReadKeys() {
		return readKeys;
	}

	@JmxAttribute
	public EventStats getWriteKeys() {
		return writeKeys;
	}

	@JmxAttribute
	public EventStats getLocalTasks() {
		return localTasks;
	}

	@JmxAttribute
	public EventStats getConcurrentTasks() {
		return concurrentTasks;
	}

	@JmxAttribute
	public EventStats getScheduledTasks() {
		return scheduledTasks;
	}

	@JmxAttribute
	public ValueStats getLocalTaskDuration() {
		return localTaskDuration;
	}

	public DurationRunnable getLastLongestLocalRunnable() {
		return lastLongestLocalRunnable;
	}

	@JmxAttribute
	public ValueStats getConcurrentTaskDuration() {
		return concurrentTaskDuration;
	}

	public DurationRunnable getLastLongestConcurrentRunnable() {
		return lastLongestConcurrentRunnable;
	}

	@JmxAttribute
	public ValueStats getScheduledTaskDuration() {
		return scheduledTaskDuration;
	}

	public DurationRunnable getLastLongestScheduledRunnable() {
		return lastLongestScheduledRunnable;
	}

	@JmxAttribute
	public ValueStats getSelectedKeysTime() {
		return selectedKeysTime;
	}

	@JmxAttribute
	public ValueStats getLocalTasksTime() {
		return localTasksTime;
	}

	@JmxAttribute
	public ValueStats getConcurrentTasksTime() {
		return concurrentTasksTime;
	}

	@JmxAttribute
	public ValueStats getScheduledTasksTime() {
		return scheduledTasksTime;
	}

	@JmxAttribute
	public ExceptionStats getIoErrors() {
		return ioErrors;
	}

	@JmxAttribute
	public List<ExceptionStats> getFatalErrors() {
		return new ArrayList<>(fatalErrors.values());
	}
}
