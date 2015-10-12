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

package io.datakernel.stream;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.examples.ScheduledProducer;
import io.datakernel.stream.processor.StreamMergeSorterStorage;
import io.datakernel.stream.processor.StreamMergeSorterStorageStub;
import io.datakernel.stream.processor.StreamSorter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.StreamStatus.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSorterTest {
	@Test
	public void test() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
		//		assertNull(source.getWiredConsumerStatus());
	}

	@Test
	public void testCollision() {
		NioEventloop eventloop = new NioEventloop();
		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);

		final StreamProducer<Integer> scheduledSource = new ScheduledProducer(eventloop) {
			@Override
			protected void onDataReceiverChanged() {

			}

			@Override
			public void scheduleNext() {
				if (numberToSend > 9) {
					abort();
					return;
				}
				if (scheduledRunnable != null && getProducerStatus().isClosed())
					return;
				if (numberToSend >= 5) {
					scheduledRunnable = eventloop.schedule(eventloop.currentTimeMillis() + 1000L, new Runnable() {
						@Override
						public void run() {
							send(numberToSend++);
							scheduleNext();
						}
					});
				} else {
					send(numberToSend++);
					scheduleNext();
				}
			}
		};
		StreamProducer<Integer> iterableSource = StreamProducers.ofIterable(eventloop, asList(30, 10, 30, 20, 50, 10, 40, 30, 20));

		StreamSorter<Integer, Integer> sorter1 = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);
		StreamSorter<Integer, Integer> sorter2 = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		List<Integer> list1 = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList1 = TestStreamConsumers.toListRandomlySuspending(eventloop, list1);
		List<Integer> list2 = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList2 = TestStreamConsumers.toListRandomlySuspending(eventloop, list2);

		iterableSource.streamTo(sorter2);
		scheduledSource.streamTo(sorter1);

		sorter1.getSortedStream().streamTo(consumerToList1);
		sorter2.getSortedStream().streamTo(consumerToList2);

		eventloop.run();
		storage.cleanup();

		assertEquals(consumerToList1.getList(), asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
		assertEquals(consumerToList2.getList(), asList(10, 20, 30, 40, 50));

		assertEquals(END_OF_STREAM, iterableSource.getProducerStatus());
		assertEquals(END_OF_STREAM, scheduledSource.getProducerStatus());

		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter1.getSortedStream()).getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter1.getSortedStream()).getConsumerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter2.getSortedStream()).getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter2.getSortedStream()).getConsumerStatus());
	}

	@Test
	public void testErrorOnSorter() {
		NioEventloop eventloop = new NioEventloop();
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<Integer, Integer>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2) {
			@SuppressWarnings("AssertWithSideEffects")
			@Override
			public void onData(Integer value) {
				assert jmxItems != ++jmxItems;
				if (value == 5) {
					closeWithError(new Exception());
					return;
				}
				list.add(value);
				if (list.size() >= itemsInMemorySize) {
					nextState();
				}
			}
		};
		final List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop, list);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());

		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
	}

	@Test
	public void testBadStorage() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<Integer>(eventloop) {
			@Override
			public void write(StreamProducer<Integer> producer, CompletionCallback completionCallback) {
				final List<Integer> list = new ArrayList<>();
				storage.put(partition++, list);
				TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
					@Override
					public void onData(Integer item) {
						list.add(item);
						if (list.size() == 2) {
							closeWithError(new Exception());
						}
					}
				};
				producer.streamTo(consumer);
			}
		};
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop, list);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
	}

	@Test
	public void testErrorOnConsumer() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (list.size() == 2) {
					closeWithError(new Exception());
				}
			}
		};

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 2);
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
	}

	@Test
	public void testErrorOnProducer() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception())
		);

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList = new StreamConsumers.ToList<>(eventloop, list);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertTrue(sorter.getItems() == 4);

		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
	}
}