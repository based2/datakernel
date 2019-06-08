package io.datakernel.di.error;

import io.datakernel.di.core.Key;

public final class NoBindingsForKeyException extends IllegalStateException {
	private final Key<?> key;

	public NoBindingsForKeyException(Key<?> key) {
		super("Provided key " + key + " with no associated bindings");
		this.key = key;
	}

	public Key<?> getKey() {
		return key;
	}
}
