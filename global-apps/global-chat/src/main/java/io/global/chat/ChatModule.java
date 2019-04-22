package io.global.chat;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.ChatMultiOperation;
import io.global.common.SimKey;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;
import io.global.ot.friendlist.ContactsOperation;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.service.ServiceEnsuringServlet;
import io.global.ot.service.messaging.MessagingServlet;
import io.global.ot.shared.SharedReposOperation;

import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.chat.Utils.CHAT_ROOM_CODEC;
import static io.global.chat.Utils.createMergedOTSystem;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class ChatModule extends AbstractModule {
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});

	@Override
	protected void configure() {
		bind(new TypeLiteral<OTSystem<ChatMultiOperation>>() {}).toInstance(createMergedOTSystem());
		bind(new TypeLiteral<StructuredCodec<ChatMultiOperation>>() {}).toInstance(CHAT_ROOM_CODEC);
		super.configure();
	}

	@Provides
	@Singleton
	@Named("Chat")
	AsyncHttpServer provideServer(Eventloop eventloop, ServiceEnsuringServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	MiddlewareServlet provideMainServlet(
			DynamicOTNodeServlet<ContactsOperation> contactsServlet,
			DynamicOTNodeServlet<SharedReposOperation> roomListServlet,
			DynamicOTNodeServlet<ChatMultiOperation> roomServlet,
			MessagingServlet messagingServlet
	) {
		return MiddlewareServlet.create()
				.with("/contacts", contactsServlet)
				.with("/index", roomListServlet)
				.with("/room/:suffix", roomServlet)
				.with("/rooms", messagingServlet);
	}

	@Provides
	@Singleton
	OTDriver provideDriver(Eventloop eventloop, GlobalOTNodeImpl node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		return new OTDriver(node, simKey);
	}
}
