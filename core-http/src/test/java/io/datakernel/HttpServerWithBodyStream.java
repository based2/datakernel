package io.datakernel;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.ref.RefInt;
import io.datakernel.common.ref.RefLong;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.promise.Promise;
import org.junit.Ignore;

import java.io.IOException;
import java.time.Duration;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.promise.Promises.repeat;

@Ignore
public final class HttpServerWithBodyStream {
	private static final int PORT = 8080;
	private static final boolean USE_KEEP_ALIVE = true;
	private static final boolean USE_BODY_STREAM = true;

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, request -> HttpResponse.ok200())
				.withListenPort(PORT);

		AsyncHttpClient client = AsyncHttpClient.create(eventloop)
				.withKeepAliveTimeout(Duration.ofSeconds(USE_KEEP_ALIVE ? 1 : 0));

		try {
			server.listen();
		} catch (IOException e) {
			e.printStackTrace();
		}

		RefInt counter = new RefInt(0);
		RefLong time = new RefLong(System.currentTimeMillis());
		eventloop.post(() -> repeat(() -> {
					HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT);
					ByteBuf buf = wrapUtf8("Hello");
					if (USE_BODY_STREAM) {
						request.withBodyStream(ChannelSupplier.of(buf));
					} else {
						request.withBody(buf);
					}
					return client.request(request)
							.thenEx((response, e) -> {
								if (e == null) {
									if (++counter.value % 1000 == 0) {
										long now = System.currentTimeMillis();
										System.out.printf("Count: %,d, took %,d ms\n", counter.value, now - time.value);
										time.value = now;
									}
								} else {
									System.out.println("Failed at count: " + (counter.value + 1));
									e.printStackTrace();
								}
								return Promise.of(null, e);
							});
				})
						.whenComplete(server::close)
		);

		eventloop.run();
	}
}
