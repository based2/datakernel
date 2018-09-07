package io.datakernel.serial;

@FunctionalInterface
public interface SerialSupplierModifier<T, R> {
	SerialSupplier<R> applyTo(SerialSupplier<T> supplier);

	static <T> SerialSupplierModifier<T, T> identity() {
		return supplier -> supplier;
	}
}
