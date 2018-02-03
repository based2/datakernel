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

package io.datakernel.http;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsyncHttpServerTest {

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	public static AsyncHttpServer blockingHttpServer(Eventloop primaryEventloop, int port) {
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public Stage<HttpResponse> serve(HttpRequest request) {
				HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				return Stage.of(content);
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenAddress(new InetSocketAddress("localhost", port));
	}

	public static AsyncHttpServer asyncHttpServer(Eventloop primaryEventloop, int port) {
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public Stage<HttpResponse> serve(HttpRequest request) {
				SettableStage<HttpResponse> stage = SettableStage.create();
				HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				stage.set(content);
				return stage;
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenAddress(new InetSocketAddress("localhost", port));
	}

	public static AsyncHttpServer delayedHttpServer(Eventloop primaryEventloop, int port) {
		Random random = new Random();
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public Stage<HttpResponse> serve(HttpRequest request) {
				SettableStage<HttpResponse> stage = SettableStage.create();
				HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				primaryEventloop.delay(random.nextInt(3), () -> stage.set(content));
				return stage;
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenAddress(new InetSocketAddress("localhost", port));
	}

	public static void writeByRandomParts(Socket socket, String string) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(string));
		Random random = new Random();
		while (buf.canRead()) {
			int count = min(1 + random.nextInt(5), buf.readRemaining());
			socket.getOutputStream().write(buf.array(), buf.readPosition(), count);
			buf.moveReadPosition(count);
		}
	}

	public static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		Assert.assertEquals(expected, decodeAscii(bytes));
	}

	@Test
	public void testKeepAlive_Http_1_0() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);

		doTestKeepAlive_Http_1_0(eventloop, blockingHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_0(eventloop, asyncHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_0(eventloop, delayedHttpServer(eventloop, port), port);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private void doTestKeepAlive_Http_1_0(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 200; i++) {
			writeByRandomParts(socket, "GET /abc HTTP1.0\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		}

		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc"); // ?

		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testKeepAlive_Http_1_1() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);

		doTestKeepAlive_Http_1_1(eventloop, blockingHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_1(eventloop, asyncHttpServer(eventloop, port), port);
		doTestKeepAlive_Http_1_1(eventloop, delayedHttpServer(eventloop, port), port);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private void doTestKeepAlive_Http_1_1(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 200; i++) {
			writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nHost: localhost\r\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		}

		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc"); // ?

		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testClosed() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\n");
		socket.close();

		server.closeFuture().get();
		thread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testNoKeepAlive_Http_1_0() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
		server.withListenAddress(new InetSocketAddress("localhost", port));
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "GET /abc HTTP/1.0\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc");
		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		socket.close();

		server.closeFuture().get();
		thread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testNoKeepAlive_Http_1_1() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
		server.withListenAddress(new InetSocketAddress("localhost", port));
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress("localhost", port));
		writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nConnection: close\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n/abc");
		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		socket.close();

		server.closeFuture().get();
		thread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testPipelining() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
//		doTestPipelining(eventloop, blockingHttpServer(eventloop));
//		doTestPipelining(eventloop, asyncHttpServer(eventloop));
		doTestPipelining(eventloop, delayedHttpServer(eventloop, port), port);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private void doTestPipelining(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.withListenAddress(new InetSocketAddress("localhost", port));
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP/1.1\r\nConnection: Keep-Alive\r\nHost: localhost\r\n\r\n"
					+ "GET /123456 HTTP/1.1\r\nHost: localhost\r\n\r\n" +
					"POST /post1 HTTP/1.1\r\n" +
					"Host: localhost\r\n" +
					"Content-Length: 8\r\n" +
					"Content-Type: application/json\r\n\r\n" +
					"{\"at\":2}" +
					"POST /post2 HTTP/1.1\r\n" +
					"Host: localhost\r\n" +
					"Content-Length: 8\r\n" +
					"Content-Type: application/json\r\n\r\n" +
					"{\"at\":2}" +
					"");
		}

		for (int i = 0; i < 100; i++) {
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 7\r\n\r\n/123456");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\n/post1");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\n/post2");
		}

		server.closeFuture().get();
		thread.join();
	}

	//	@Test
	public void testPipelining2() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
//		doTestPipelining(eventloop, blockingHttpServer(eventloop));
//		doTestPipelining(eventloop, asyncHttpServer(eventloop));
		doTestPipelining2(eventloop, delayedHttpServer(eventloop, port), port);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private void doTestPipelining2(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.withListenAddress(new InetSocketAddress("localhost", port));
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP/1.0\r\nHost: localhost\r\n\r\n"
					+ "GET /123456 HTTP/1.1\r\nHost: localhost\r\n\r\n" +
					"POST /post1 HTTP/1.1\r\n" +
					"Host: localhost\r\n" +
					"Content-Length: 8\r\n" +
					"Content-Type: application/json\r\n\r\n" +
					"{\"at\":2}" +
					"POST /post2 HTTP/1.1\r\n" +
					"Host: localhost\r\n" +
					"Content-Length: 8\r\n" +
					"Content-Type: application/json\r\n\r\n" +
					"{\"at\":2}" +
					"");
		}

		for (int i = 0; i < 100; i++) {
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 7\r\n\r\n/123456");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\n/post1");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\n/post2");
		}

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testBigHttpMessage() throws Exception {
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ByteBuf buf =
				HttpRequest.post("http://127.0.0.1:" + port)
						.withBody(ByteBuf.wrapForReading(encodeAscii("Test big HTTP message body")))
						.toByteBuf();

		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public Stage<HttpResponse> serve(HttpRequest request) {
				SettableStage<HttpResponse> stage = SettableStage.create();
				HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				stage.set(content);
				return stage;
			}
		};

		AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withMaxHttpMessageSize(25)
				.withListenAddress(new InetSocketAddress("localhost", port));
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress("localhost", port));
			socket.getOutputStream().write(buf.array(), buf.readPosition(), buf.readRemaining());
			buf.recycle();
			Thread.sleep(100);
		}
		server.closeFuture().get();
		thread.join();
//		assertEquals(1, server.getStats().getHttpErrors().getTotal());
//		assertEquals(AbstractHttpConnection.TOO_BIG_HTTP_MESSAGE,
//				server.getStats().getHttpErrors().getLastException());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testExpectContinue() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				new AsyncServlet() {
					@Override
					public Stage<HttpResponse> serve(HttpRequest request) {
						return Stage.of(HttpResponse.ok200().withBody(request.detachBody()));
					}
				})
				.withListenAddress(new InetSocketAddress("localhost", port));

		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(new InetSocketAddress("localhost", port));

		writeByRandomParts(socket, "POST /abc HTTP/1.0\r\nHost: localhost\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 100 Continue\r\n\r\n");

		writeByRandomParts(socket, "abcde");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\nabcde");

		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().get();
		thread.join();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		AsyncHttpServer server = blockingHttpServer(eventloop, 8888);
		server.listen();
		eventloop.run();
	}
}
