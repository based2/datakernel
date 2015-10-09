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

package io.datakernel.stream.processor;

import com.google.common.base.Functions;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.AbstractStreamProducer.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSharderTest {

	private static final Sharder<Integer> SHARDER = new Sharder<Integer>() {
		@Override
		public int shard(Integer object) {
			return object % 2;
		}
	};

	@Test
	public void test1() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamSharder<Integer, Integer> streamSharder = new StreamSharder<>(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4));
		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(streamSharder);
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertTrue(((AbstractStreamProducer) source).getStatus() == END_OF_STREAM);
		assertTrue(streamSharder.getUpstreamConsumerStatus() == AbstractStreamConsumer.CLOSED);
		assertArrayEquals(streamSharder.getDownstreamProducersStatus(), new byte[]{END_OF_STREAM, END_OF_STREAM});
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED);
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED);
	}

	@Test
	public void test2() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamSharder<Integer, Integer> streamSharder = new StreamSharder<>(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4));
		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(streamSharder);
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertTrue(((AbstractStreamProducer) source).getStatus() == END_OF_STREAM);
		assertTrue(streamSharder.getUpstreamConsumerStatus() == AbstractStreamConsumer.CLOSED);
		assertArrayEquals(streamSharder.getDownstreamProducersStatus(), new byte[]{END_OF_STREAM, END_OF_STREAM});
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED);
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED);
	}

	@Test
	public void testWithError() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamSharder<Integer, Integer> streamSharder = new StreamSharder<>(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4));

		List<Integer> list1 = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumer1 = new StreamConsumers.ToList<Integer>(eventloop, list1);

		List<Integer> list2 = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list2) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 3) {
					closeWithError(new Exception());
					return;
				}
				upstreamProducer.onConsumerSuspended();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.onConsumerResumed();
					}
				});
			}
		};

		source.streamTo(streamSharder);
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();

		assertTrue(list1.size() == 1);
		assertTrue(list2.size() == 2);
		assertTrue(((AbstractStreamProducer) source).getStatus() == CLOSED_WITH_ERROR);
		assertTrue(streamSharder.getUpstreamConsumerStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
		assertArrayEquals(streamSharder.getDownstreamProducersStatus(), new byte[]{CLOSED_WITH_ERROR, CLOSED_WITH_ERROR});
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
	}

//	@Test
//	public void testEndofStream() throws Exception {
//		NioEventloop eventloop = new NioEventloop();
//
//		StreamSharder<Integer, Integer> streamSharder = new StreamSharder<>(eventloop, SHARDER, Functions.<Integer>identity());
//
//		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4));
//
//		List<Integer> list1 = new ArrayList<>();
//		StreamConsumers.ToList<Integer> consumer1 = new StreamConsumers.ToList<Integer>(eventloop, list1);
//
//		List<Integer> list2 = new ArrayList<>();
//		StreamConsumers.ToList<Integer> consumer2 = new StreamConsumers.ToList<Integer>(eventloop, list2) {
//			@Override
//			public void onData(Integer item) {
//				super.onData(item);
//				if (item == 3) {
//					onProducerEndOfStream();
//					return;
//				}
//				upstreamProducer.onConsumerSuspended();
//				eventloop.post(new Runnable() {
//					@Override
//					public void run() {
//						upstreamProducer.onConsumerResumed();
//					}
//				});
//			}
//		};
//
//		source.streamTo(streamSharder);
//		streamSharder.newOutput().streamTo(consumer1);
//		streamSharder.newOutput().streamTo(consumer2);
//
//		eventloop.run();
//
//		assertTrue(list1.size() == 1);
//		assertTrue(list2.size() == 2);
//		assertTrue(((AbstractStreamProducer)source).getStatus() == AbstractStreamProducer.CLOSED);
//	}

	@Test
	public void testProducerWithError() throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		StreamSharder<Integer, Integer> streamSharder = new StreamSharder<>(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 1),
				StreamProducers.ofValue(eventloop, 2),
				StreamProducers.ofValue(eventloop, 3),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception())
		);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumer<Integer> consumer1 = TestStreamConsumers.toListOneByOne(eventloop, list1);
		List<Integer> list2 = new ArrayList<>();
		StreamConsumer<Integer> consumer2 = TestStreamConsumers.toListOneByOne(eventloop, list2);

		source.streamTo(streamSharder);
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();

		assertTrue(list1.size() == 1);
		assertTrue(list2.size() == 2);

		assertTrue(streamSharder.getUpstreamConsumerStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
		assertArrayEquals(streamSharder.getDownstreamProducersStatus(), new byte[]{CLOSED_WITH_ERROR, CLOSED_WITH_ERROR});
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
		assertTrue(((AbstractStreamConsumer) consumer1).getStatus() == AbstractStreamConsumer.CLOSED_WITH_ERROR);
	}
}
