package io.datakernel.stream.stats;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialConsumerModifier;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSupplierModifier;
import io.datakernel.stream.*;

public interface StreamStats<T> extends
		StreamProducerModifier<T, T>, StreamConsumerModifier<T, T>,
		SerialSupplierModifier<T, T>, SerialConsumerModifier<T, T> {
	StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver);

	void onStarted();

	void onProduce();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable throwable);

	@Override
	default StreamConsumer<T> applyTo(StreamConsumer<T> consumer) {
		return consumer.apply(StreamStatsForwarder.create(this));
	}

	@Override
	default StreamProducer<T> applyTo(StreamProducer<T> producer) {
		return producer.apply(StreamStatsForwarder.create(this));
	}

	@Override
	default SerialSupplier<T> applyTo(SerialSupplier<T> supplier) {
		return supplier; // TODO
	}

	@Override
	default SerialConsumer<T> applyTo(SerialConsumer<T> consumer) {
		return consumer; // TODO
	}

	static <T> StreamStatsBasic<T> basic() {
		return new StreamStatsBasic<>();
	}

	static <T> StreamStatsDetailed<T> detailed() {
		return new StreamStatsDetailed<>(null);
	}

	static <T> StreamStatsDetailed<T> detailed(StreamStatsSizeCounter<T> sizeCounter) {
		return new StreamStatsDetailed<>(sizeCounter);
	}

	static <T> StreamStatsDetailedEx<T> detailedEx() {
		return new StreamStatsDetailedEx<>(null);
	}

	static <T> StreamStatsDetailedEx detailedEx(StreamStatsSizeCounter<T> sizeCounter) {
		return new StreamStatsDetailedEx<>(sizeCounter);
	}

}
