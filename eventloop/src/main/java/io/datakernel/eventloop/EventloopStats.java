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
import io.datakernel.jmx.*;
import io.datakernel.util.Stopwatch;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.datakernel.jmx.ValueStats.POWERS_OF_TEN;

public final class EventloopStats {
	private final EventStats loops;
	private final SelectorEvents selectorEvents;
	private final TaskEvents taskEvents;
	private final ErrorStats errorStats;
	private final DurationStats durationStats;
	private final OverdueStats overdueStats;

	private final DurationRunnable longestLocalTask;
	private final DurationRunnable longestConcurrentTask;
	private final DurationRunnable longestScheduledTask;

	private EventloopStats(double smoothingWindow) {
		loops = EventStats.create(smoothingWindow);
		selectorEvents = new SelectorEvents(smoothingWindow);
		taskEvents = new TaskEvents(smoothingWindow);
		errorStats = new ErrorStats();
		durationStats = new DurationStats(smoothingWindow);
		overdueStats = new OverdueStats(smoothingWindow);

		longestConcurrentTask = new DurationRunnable();
		longestScheduledTask = new DurationRunnable();
		longestLocalTask = new DurationRunnable();
	}

	public static EventloopStats create(double smoothingWindow) {
		return new EventloopStats(smoothingWindow);
	}

	public void setSmoothingWindow(double smoothingWindow) {
		loops.setSmoothingWindow(smoothingWindow);
		selectorEvents.setSmoothingWindow(smoothingWindow);
		taskEvents.setSmoothingWindow(smoothingWindow);
		durationStats.setSmoothingWindow(smoothingWindow);
		overdueStats.setSmoothingWindow(smoothingWindow);
	}

	public void resetStats() {
		loops.resetStats();
		selectorEvents.reset();
		taskEvents.reset();
		durationStats.reset();
		errorStats.reset();
		overdueStats.reset();

		longestLocalTask.reset();
		longestConcurrentTask.reset();
		longestScheduledTask.reset();
	}

	public void updateBusinessLogicTime(long businessLogicTime) {
		loops.recordEvent();
		this.durationStats.businessLogicTime.recordValue((int) businessLogicTime);
	}

	public void updateSelectorSelectTime(long selectorSelectTime) {
		this.durationStats.selectorSelectTime.recordValue((int) selectorSelectTime);
	}

	public void updateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys) {
		this.selectorEvents.allSelectedKeys.recordValue(lastSelectedKeys);
		this.selectorEvents.invalidKeys.recordValue(invalidKeys);
		this.selectorEvents.acceptKeys.recordValue(acceptKeys);
		this.selectorEvents.connectKeys.recordValue(connectKeys);
		this.selectorEvents.readKeys.recordValue(readKeys);
		this.selectorEvents.writeKeys.recordValue(writeKeys);
	}

	public void updateSelectedKeysTimeStats(@Nullable Stopwatch sw) {
		if (sw != null)
			durationStats.selectedKeysTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
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
		updateTaskDuration(durationStats.localTaskDuration, longestLocalTask, runnable, sw);
	}

	public void updateLocalTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			durationStats.localTasksTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		taskEvents.localTasks.recordValue(newTasks);
	}

	public void updateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(durationStats.concurrentTaskDuration, longestConcurrentTask, runnable, sw);
	}

	public void updateConcurrentTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			durationStats.concurrentTasksTime.recordValue((int) sw.elapsed(TimeUnit.MICROSECONDS));
		taskEvents.concurrentTasks.recordValue(newTasks);
	}

	public void updateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(durationStats.scheduledTaskDuration, longestScheduledTask, runnable, sw);
	}

	public void updateScheduledTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			durationStats.scheduledTasksTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		taskEvents.scheduledTasks.recordValue(newTasks);
	}

	public void recordFatalError(Throwable throwable, Object causedObject) {
		StackTrace stackTrace = new StackTrace(throwable.getStackTrace());

		errorStats.fatalErrors.recordException(throwable, causedObject);

		ExceptionStats stats = errorStats.allFatalErrors.get(stackTrace);
		if (stats == null) {
			stats = ExceptionStats.create();
			errorStats.allFatalErrors.put(stackTrace, stats);
		}
		stats.recordException(throwable, causedObject);
	}

	public void recordIoError(Throwable throwable, Object causedObject) {
		errorStats.ioErrors.recordException(throwable, causedObject);
	}

	public void recordScheduledTaskOverdue(int overdue) {
		overdueStats.scheduledTasks.recordValue(overdue);
	}

	public void recordBackgroundTaskOverdue(int overdue) {
		overdueStats.backgroundTasks.recordValue(overdue);
	}

	@JmxAttribute
	public EventStats getLoops() {
		return loops;
	}

	@JmxAttribute(description = "total count and smoothed rate of specified key selections " +
			"starting from launching eventloop")
	public SelectorEvents getSelectorEvents() {
		return selectorEvents;
	}

	@JmxAttribute(description = " total count and smoothed rate of specified tasks that were already executed " +
			"starting from launching eventloop")
	public TaskEvents getTaskEvents() {
		return taskEvents;
	}

	@JmxAttribute
	public DurationStats getDurationStats() {
		return durationStats;
	}

	@JmxAttribute(description = "all error starting from launching eventloop")
	public ErrorStats getErrorStats() {
		return errorStats;
	}

	@JmxAttribute(description = "local task with longest duration (in microseconds)")
	public DurationRunnable getLongestLocalTask() {
		return longestLocalTask;
	}

	@JmxAttribute(description = "concurrent task with longest duration (in microseconds)")
	public DurationRunnable getLongestConcurrentTask() {
		return longestConcurrentTask;
	}

	@JmxAttribute(description = "scheduled task with longest duration (in microseconds)")
	public DurationRunnable getLongestScheduledTask() {
		return longestScheduledTask;
	}

	@JmxAttribute(
			description = "overdue is difference between actual and specified timestamps of task execution " +
					"(in milliseconds)"
	)
	public OverdueStats getOverdueStats() {
		return overdueStats;
	}

	public static final class DurationRunnable implements JmxStats<DurationRunnable> {
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

		@JmxAttribute()
		public long getDuration() {
			return duration;
		}

		@JmxAttribute
		public String getClassName() {
			return (runnable == null) ? "" : runnable.getClass().getName();
		}

		@Override
		public void add(DurationRunnable another) {
			if (another.duration > this.duration) {
				this.duration = another.duration;
				this.runnable = another.runnable;
			}
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

	public static final class SelectorEvents {
		private final ValueStats allSelectedKeys;
		private final ValueStats invalidKeys;
		private final ValueStats acceptKeys;
		private final ValueStats connectKeys;
		private final ValueStats readKeys;
		private final ValueStats writeKeys;

		public SelectorEvents(double smoothingWindow) {
			allSelectedKeys = ValueStats.create(smoothingWindow);
			invalidKeys = ValueStats.create(smoothingWindow);
			acceptKeys = ValueStats.create(smoothingWindow);
			connectKeys = ValueStats.create(smoothingWindow);
			readKeys = ValueStats.create(smoothingWindow);
			writeKeys = ValueStats.create(smoothingWindow);
		}

		public void setSmoothingWindow(double smoothingWindow) {
			allSelectedKeys.setSmoothingWindow(smoothingWindow);
			invalidKeys.setSmoothingWindow(smoothingWindow);
			acceptKeys.setSmoothingWindow(smoothingWindow);
			connectKeys.setSmoothingWindow(smoothingWindow);
			readKeys.setSmoothingWindow(smoothingWindow);
			writeKeys.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			allSelectedKeys.resetStats();
			invalidKeys.resetStats();
			acceptKeys.resetStats();
			connectKeys.resetStats();
			readKeys.resetStats();
			writeKeys.resetStats();
		}

		@JmxAttribute
		public ValueStats getAllSelectedKeys() {
			return allSelectedKeys;
		}

		@JmxAttribute
		public ValueStats getInvalidKeys() {
			return invalidKeys;
		}

		@JmxAttribute
		public ValueStats getAcceptKeys() {
			return acceptKeys;
		}

		@JmxAttribute
		public ValueStats getConnectKeys() {
			return connectKeys;
		}

		@JmxAttribute
		public ValueStats getReadKeys() {
			return readKeys;
		}

		@JmxAttribute
		public ValueStats getWriteKeys() {
			return writeKeys;
		}
	}

	public static final class TaskEvents {
		private final ValueStats localTasks;
		private final ValueStats concurrentTasks;
		private final ValueStats scheduledTasks;

		public TaskEvents(double smoothingWindow) {
			localTasks = ValueStats.create(smoothingWindow);
			concurrentTasks = ValueStats.create(smoothingWindow);
			scheduledTasks = ValueStats.create(smoothingWindow);
		}

		public void setSmoothingWindow(double smoothingWindow) {
			localTasks.setSmoothingWindow(smoothingWindow);
			concurrentTasks.setSmoothingWindow(smoothingWindow);
			scheduledTasks.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			localTasks.resetStats();
			concurrentTasks.resetStats();
			scheduledTasks.resetStats();
		}

		@JmxAttribute
		public ValueStats getLocalTasks() {
			return localTasks;
		}

		@JmxAttribute
		public ValueStats getConcurrentTasks() {
			return concurrentTasks;
		}

		@JmxAttribute
		public ValueStats getScheduledTasks() {
			return scheduledTasks;
		}
	}

	public static final class ErrorStats {
		private final ExceptionStats fatalErrors = ExceptionStats.create();
		private final Map<StackTrace, ExceptionStats> allFatalErrors = new HashMap<>();
		private final ExceptionStats ioErrors = ExceptionStats.create();

		public void reset() {
			fatalErrors.resetStats();
			allFatalErrors.clear();
			ioErrors.resetStats();
		}

		@JmxAttribute
		public ExceptionStats getFatalErrors() {
			return fatalErrors;
		}

		@JmxAttribute
		public Map<StackTrace, ExceptionStats> getAllFatalErrors() {
			return allFatalErrors;
		}

		@JmxAttribute
		public ExceptionStats getIoErrors() {
			return ioErrors;
		}
	}

	public static final class DurationStats {
		private final ValueStats selectorSelectTime;
		private final ValueStats businessLogicTime;

		private final ValueStats selectedKeysTime;
		private final ValueStats localTasksTime;
		private final ValueStats concurrentTasksTime;
		private final ValueStats scheduledTasksTime;

		private final ValueStats localTaskDuration;
		private final ValueStats concurrentTaskDuration;
		private final ValueStats scheduledTaskDuration;

		public DurationStats(double smoothingWindow) {
			selectorSelectTime = ValueStats.create(smoothingWindow);
			businessLogicTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TEN);

			selectedKeysTime = ValueStats.create(smoothingWindow);
			localTasksTime = ValueStats.create(smoothingWindow);
			concurrentTasksTime = ValueStats.create(smoothingWindow);
			scheduledTasksTime = ValueStats.create(smoothingWindow);

			localTaskDuration = ValueStats.create(smoothingWindow);
			concurrentTaskDuration = ValueStats.create(smoothingWindow);
			scheduledTaskDuration = ValueStats.create(smoothingWindow);
		}

		public void setSmoothingWindow(double smoothingWindow) {
			selectorSelectTime.setSmoothingWindow(smoothingWindow);
			businessLogicTime.setSmoothingWindow(smoothingWindow);

			selectedKeysTime.setSmoothingWindow(smoothingWindow);
			localTasksTime.setSmoothingWindow(smoothingWindow);
			concurrentTasksTime.setSmoothingWindow(smoothingWindow);
			scheduledTasksTime.setSmoothingWindow(smoothingWindow);

			localTaskDuration.setSmoothingWindow(smoothingWindow);
			concurrentTaskDuration.setSmoothingWindow(smoothingWindow);
			scheduledTaskDuration.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			selectorSelectTime.resetStats();
			businessLogicTime.resetStats();

			selectedKeysTime.resetStats();
			localTasksTime.resetStats();
			concurrentTasksTime.resetStats();
			scheduledTasksTime.resetStats();

			localTaskDuration.resetStats();
			concurrentTaskDuration.resetStats();
			scheduledTaskDuration.resetStats();
		}

		@JmxAttribute(description = "duration of selector.select() call in one eventloop cycle (in milliseconds)")
		public ValueStats getSelectorSelectTime() {
			return selectorSelectTime;
		}

		@JmxAttribute(
				description = "duration of all localTasks, concurrentTasks, scheduledTasks" +
						" and handing of keys in one eventloop cycle (in milliseconds)",
				extraSubAttributes = {"histogram"}
		)
		public ValueStats getBusinessLogicTime() {
			return businessLogicTime;
		}

		@JmxAttribute(description = "duration of handling of all selected keys in one eventloop cycle (in milliseconds)")
		public ValueStats getSelectedKeysTime() {
			return selectedKeysTime;
		}

		@JmxAttribute(description = "duration of all local tasks in one eventloop cycle (in milliseconds)")
		public ValueStats getLocalTasksTime() {
			return localTasksTime;
		}

		@JmxAttribute(description = "duration of all concurrent tasks in one eventloop cycle (in milliseconds)")
		public ValueStats getConcurrentTasksTime() {
			return concurrentTasksTime;
		}

		@JmxAttribute(description =
				" duration of all scheduled tasks (including background tasks) " +
						"in one eventloop cycle (in milliseconds)")
		public ValueStats getScheduledTasksTime() {
			return scheduledTasksTime;
		}

		@JmxAttribute(description = "duration of one local task (in microseconds)")
		public ValueStats getLocalTaskDuration() {
			return localTaskDuration;
		}

		@JmxAttribute(description = "duration of one concurrent task (in microseconds)")
		public ValueStats getConcurrentTaskDuration() {
			return concurrentTaskDuration;
		}

		@JmxAttribute(description = "duration of one scheduled task (in microseconds)")
		public ValueStats getScheduledTaskDuration() {
			return scheduledTaskDuration;
		}
	}

	public static final class OverdueStats {
		private final ValueStats scheduledTasks;
		private final ValueStats backgroundTasks;

		public OverdueStats(double smoothingWindow) {
			this.scheduledTasks = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TEN);
			this.backgroundTasks = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TEN);
		}

		public void setSmoothingWindow(double smoothingWindow) {
			scheduledTasks.setSmoothingWindow(smoothingWindow);
			backgroundTasks.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			scheduledTasks.resetStats();
			backgroundTasks.resetStats();
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getScheduledTasks() {
			return scheduledTasks;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getBackgroundTasks() {
			return backgroundTasks;
		}
	}
}
