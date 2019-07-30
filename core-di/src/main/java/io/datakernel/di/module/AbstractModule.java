package io.datakernel.di.module;

import io.datakernel.di.annotation.*;
import io.datakernel.di.core.*;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.util.Constructors.*;
import io.datakernel.di.util.LocationInfo;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.module.UniqueNameImpl.uniqueName;
import static io.datakernel.di.util.ReflectionUtils.*;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

/**
 * This class provides DSL's for making bindings, transformers, generators or multibinders
 * fluently and declaratively.
 */
public abstract class AbstractModule implements Module {
	private final List<BindingBuilder<Object>> builders = new ArrayList<>();

	private Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());
	private Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
	private Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

	private boolean configuring, configured;

	/**
	 * This method scans given object for {@link Provides provider methods} and adds them as bindings or generators to this module.
	 */
	protected final void addDeclarativeBindingsFrom(@NotNull Object module) {
		addDeclarativeBindingsImpl(module.getClass(), module);
	}

	protected final void addDeclarativeBindingsFrom(@NotNull Class<?> moduleClass) {
		addDeclarativeBindingsImpl(moduleClass, null);
	}

	private void addKeyToSet(Name name, Key<?> key) {
		Key<Set<Key<?>>> setKey = new Key<Set<Key<?>>>(name) {};
		bind(setKey).toInstance(singleton(key));
		multibind(setKey, Multibinder.toSet());
	}

	private void addDeclarativeBindingsImpl(@NotNull Class<?> moduleClass, @Nullable Object module) {
		for (Method method : getAnnotatedElements(moduleClass, Provides.class, Class::getDeclaredMethods, false)) {
			Name name = nameOf(method);
			Set<Name> keySets = keySetsOf(method);
			Scope[] methodScope = getScope(method);
			boolean exported = method.isAnnotationPresent(Export.class);

			Type type = Types.resolveTypeVariables(method.getGenericReturnType(), moduleClass);
			TypeVariable<Method>[] typeVars = method.getTypeParameters();

			if (typeVars.length == 0) {
				Key<Object> key = Key.ofType(type, name);

				BindingBuilder<Object> methodBindingBuilder = bind(key).to(bindingFromMethod(module, method)).in(methodScope);
				if (exported) {
					methodBindingBuilder.export();
				}

				keySets.forEach(keySet -> addKeyToSet(keySet, key));
			} else {
				Set<TypeVariable<?>> unused = Arrays.stream(typeVars)
						.filter(typeVar -> !Types.contains(type, typeVar))
						.collect(toSet());
				if (!unused.isEmpty()) {
					throw new IllegalStateException("Generic type variables " + unused + " are not used in return type of templated provider method " + method);
				}
				if (!keySets.isEmpty()) {
					throw new IllegalStateException("Key set annotations are not supported by templated methods, method " + method);
				}
				if (exported) {
					throw new IllegalStateException("@Export annotation is not applicable for templated methods because they are generators and thus are always exported");
				}
				generate(method.getReturnType(), (bindings, scope, key) -> {
					if (scope.length < methodScope.length || !Objects.equals(key.getName(), name) || !Types.matches(key.getType(), type)) {
						return null;
					}
					for (int i = 0; i < methodScope.length; i++) {
						if (!scope[i].equals(methodScope[i])) {
							return null;
						}
					}
					return bindingFromGenericMethod(module, key, method);
				});
			}
		}
		for (Method method : getAnnotatedElements(moduleClass, ProvidesIntoSet.class, Class::getDeclaredMethods, false)) {
			if (method.getTypeParameters().length != 0) {
				throw new IllegalStateException("@ProvidesIntoSet does not support templated methods, method " + method);
			}

			Type type = Types.resolveTypeVariables(method.getGenericReturnType(), moduleClass);
			Scope[] scope = getScope(method);

			Key<Object> key = Key.ofType(type, uniqueName());
			bind(key).to(bindingFromMethod(module, method)).in(scope);

			Key<Set<Object>> setKey = Key.ofType(Types.parameterized(Set.class, type), nameOf(method));
			bind(setKey).to(Collections::singleton, key).in(scope);
			multibind(setKey, Multibinder.toSet());

			keySetsOf(method).forEach(keySet -> {
				addKeyToSet(keySet, key);
				addKeyToSet(keySet, setKey);
			});
		}
	}

	@SuppressWarnings({"unchecked", "UnusedReturnValue", "WeakerAccess"})
	public final class BindingBuilder<T> {
		private Scope[] scope = UNSCOPED;
		private boolean exported = false;
		private Key<T> key;

		private Binding<? extends T> binding = (Binding<? extends T>) new Binding<>(emptySet(), Preprocessor.TO_BE_GENERATED).at(LocationInfo.from(AbstractModule.this));

		public BindingBuilder(@NotNull Key<T> key) {
			this.key = key;
		}

		/**
		 * If bound key does not have a name already then sets it to a given one
		 */
		public BindingBuilder<T> annotatedWith(@NotNull Name name) {
			if (key.getName() != null) {
				throw new IllegalStateException("Already annotated with " + key.getName().getDisplayString());
			}
			key = key.named(name);
			return this;
		}

		/**
		 * @see #annotatedWith(Name)
		 */
		public BindingBuilder<T> annotatedWith(@NotNull Annotation annotation) {
			return annotatedWith(Name.of(annotation));
		}

		/**
		 * @see #annotatedWith(Name)
		 */
		public BindingBuilder<T> annotatedWith(@NotNull Class<? extends Annotation> annotationType) {
			return annotatedWith(Name.of(annotationType));
		}

		/**
		 * The binding being built by this builder will be added to the binding graph trie at given scope path
		 */
		public BindingBuilder<T> in(@NotNull Scope[] scope) {
			if (this.scope.length != 0) {
				throw new IllegalStateException("Already bound to scope " + getScopeDisplayString(this.scope));
			}
			this.scope = scope;
			return this;
		}

		/**
		 * @see #in(Scope[])
		 */
		public BindingBuilder<T> in(@NotNull Scope scope, @NotNull Scope... scopes) {
			Scope[] joined = new Scope[scopes.length + 1];
			joined[0] = scope;
			System.arraycopy(scopes, 0, joined, 1, scopes.length);
			return in(joined);
		}

		/**
		 * @see #in(Scope[])
		 */
		@SafeVarargs
		public final BindingBuilder<T> in(@NotNull Class<? extends Annotation> annotationClass, @NotNull Class<? extends Annotation>... annotationClasses) {
			return in(Stream.concat(Stream.of(annotationClass), Arrays.stream(annotationClasses)).map(Scope::of).toArray(Scope[]::new));
		}

		/**
		 * Sets a binding which would be bound to a given key and added to the binding graph trie
		 */
		public BindingBuilder<T> to(@NotNull Binding<? extends T> binding) {
			if (this.binding.getCompiler() != Preprocessor.TO_BE_GENERATED) {
				throw new IllegalStateException("Already mapped to a binding");
			}
			if (binding.getLocation() == null) {
				binding.at(LocationInfo.from(AbstractModule.this));
			}
			this.binding = binding;
			return this;
		}

		/**
		 * DSL shortcut for creating a binding that just calls a binding at given key
		 * and {@link #to(Binding) binding it} to current key.
		 */
		public BindingBuilder<T> to(@NotNull Key<? extends T> implementation) {
			return to(Binding.to(implementation));
		}

		/**
		 * @see #to(Key)
		 */
		public BindingBuilder<T> to(@NotNull Class<? extends T> implementation) {
			return to(Binding.to(implementation));
		}

		/**
		 * DSL shortcut for creating a binding from a given instance
		 * and {@link #to(Binding) binding it} to current key.
		 */
		public <U extends T> BindingBuilder<T> toInstance(@NotNull U instance) {
			return to(Binding.toInstance(instance));
		}

		/**
		 * DSL shortcut for creating a dummy binding that will not create any instances
		 * because they will be added later dynamically by calling {@link Injector#putInstance}.
		 */
		public BindingBuilder<T> toDynamic() {
			return to(() -> {
				throw new AssertionError("No instance was put into the injector dynamically for key " + key.getDisplayString() + " in scope " + getScopeDisplayString(scope));
			});
		}

		/**
		 * DSL shortcut for creating a binding that calls a supplier from binding at given key
		 * and {@link #to(Binding) binding it} to current key.
		 */
		public BindingBuilder<T> toSupplier(@NotNull Key<? extends Supplier<? extends T>> supplierKey) {
			return to(Binding.toSupplier(supplierKey));
		}

		/**
		 * @see #toSupplier(Key)
		 */
		public BindingBuilder<T> toSupplier(@NotNull Class<? extends Supplier<? extends T>> supplierType) {
			return to(Binding.toSupplier(supplierType));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public BindingBuilder<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Class<?>[] dependencies) {
			return to(Binding.to(factory, dependencies));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public BindingBuilder<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Key<?>[] dependencies) {
			return to(Binding.to(factory, dependencies));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public BindingBuilder<T> to(@NotNull ConstructorN<? extends T> factory, @NotNull Dependency[] dependencies) {
			return to(Binding.to(factory, dependencies));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public BindingBuilder<T> to(@NotNull Constructor0<? extends T> constructor) {
			return to(Binding.to(constructor));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1> BindingBuilder<T> to(@NotNull Constructor1<T1, ? extends T> constructor,
				@NotNull Class<T1> dependency1) {
			return to(Binding.to(constructor, dependency1));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2> BindingBuilder<T> to(@NotNull Constructor2<T1, T2, ? extends T> constructor,
				@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2) {
			return to(Binding.to(constructor, dependency1, dependency2));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3> BindingBuilder<T> to(@NotNull Constructor3<T1, T2, T3, ? extends T> constructor,
				@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3, T4> BindingBuilder<T> to(@NotNull Constructor4<T1, T2, T3, T4, ? extends T> constructor,
				@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3, T4, T5> BindingBuilder<T> to(@NotNull Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
				@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3, T4, T5, T6> BindingBuilder<T> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
				@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5, @NotNull Class<T6> dependency6) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1> BindingBuilder<T> to(@NotNull Constructor1<T1, ? extends T> constructor,
				@NotNull Key<T1> dependency1) {
			return to(Binding.to(constructor, dependency1));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2> BindingBuilder<T> to(@NotNull Constructor2<T1, T2, ? extends T> constructor,
				@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2) {
			return to(Binding.to(constructor, dependency1, dependency2));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3> BindingBuilder<T> to(@NotNull Constructor3<T1, T2, T3, ? extends T> constructor,
				@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3, T4> BindingBuilder<T> to(@NotNull Constructor4<T1, T2, T3, T4, ? extends T> constructor,
				@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3, T4, T5> BindingBuilder<T> to(@NotNull Constructor5<T1, T2, T3, T4, T5, ? extends T> constructor,
				@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5));
		}

		/**
		 * DSL shortcut for creating a binding and {@link #to(Binding) binding it} to current key.
		 */
		public <T1, T2, T3, T4, T5, T6> BindingBuilder<T> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, ? extends T> constructor,
				@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5, @NotNull Key<T6> dependency6) {
			return to(Binding.to(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6));
		}

		/**
		 * Adds bound key to a key set named with given name.
		 * <p>
		 * A key set is a special key of type Set&lt;Key&lt;?&gt;&gt; and name with annotation that is
		 * marked with {@link KeySetAnnotation} instead of {@link NameAnnotation}
		 *
		 * @see KeySetAnnotation
		 */
		public BindingBuilder<T> as(@NotNull Name name) {
			checkArgument(name.isMarkedBy(KeySetAnnotation.class), "Should be a key set name");

			Key<Set<Key<?>>> setKey = new Key<Set<Key<?>>>(name) {};
			bind(setKey).toInstance(singleton(key));
			multibind(setKey, Multibinder.toSet());
			return this;
		}

		/**
		 * @see #as(Name)
		 */
		public BindingBuilder<T> as(@NotNull Class<? extends Annotation> annotationType) {
			return as(Name.of(annotationType));
		}

		/**
		 * @see #as(Name)
		 */
		public BindingBuilder<T> as(@NotNull Annotation annotation) {
			return as(Name.of(annotation));
		}

		/**
		 * @see #as(Name)
		 */
		public BindingBuilder<T> as(@NotNull String name) {
			return as(Name.of(name));
		}

		/**
		 * A shortcut for <code>as({@link EagerSingleton}.class)</code>
		 */
		public BindingBuilder<T> asEagerSingleton() {
			return as(EagerSingleton.class);
		}

		/**
		 * Adds given dependencies to the underlying binding
		 */
		public BindingBuilder<T> withExtraDependencies(Dependency... dependencies) {
			this.binding = this.binding.addDependencies(dependencies);
			return this;
		}

		/**
		 * @see #withExtraDependencies(Dependency...)
		 */
		public BindingBuilder<T> withExtraDependencies(Key<?>... dependencies) {
			this.binding = this.binding.addDependencies(dependencies);
			return this;
		}

		/**
		 * @see #withExtraDependencies(Dependency...)
		 */
		public BindingBuilder<T> withExtraDependencies(Class<?>... dependencies) {
			this.binding = this.binding.addDependencies(dependencies);
			return this;
		}

		public BindingBuilder<T> export() {
			checkState(!exported, "Binding was already exported");
			exported = true;
			return this;
		}
	}

	/**
	 * This method is meant to be overridden to call all the <code>bind(...)</code> methods.
	 * Those methods can be called at any time before the module is given to the injector,
	 * so this method is simply called on first call to any of {@link Module} methods.
	 * <p>
	 * It exists for consistency and code purity.
	 * <p>
	 * For quick-and-dirty modules <code>bind(...)</code> methods can be called using double-brace initialization.
	 */
	protected void configure() {
	}

	/**
	 * This method simply adds all bindings, transformers, generators and multibinders from given module to this one.
	 */
	protected final void install(Module module) {
		checkState(configuring, "Cannot install modules before or after configure() call");

		bindings.addAll(module.getBindings(), multimapMerger());
		combineMultimap(bindingTransformers, module.getBindingTransformers());
		combineMultimap(bindingGenerators, module.getBindingGenerators());
		mergeMultibinders(multibinders, module.getMultibinders());
	}

	/**
	 * This method begins a chain of binding builder DSL calls.
	 * <p>
	 * You can use generics in it, only those that are defined at the module class.
	 * And you need to subclass the module at the usage point to 'bake' those generics
	 * into subclass bytecode so that they could be fetched by this bind call.
	 */
	@SuppressWarnings("unchecked")
	protected final <T> BindingBuilder<T> bind(@NotNull Key<T> key) {
		checkState(configuring, "Cannot bind before or after configure() call");

		// to support abstract modules with generics
		Key<T> fullKey = Key.ofType(Types.resolveTypeVariables(key.getType(), getClass()), key.getName());
		BindingBuilder<T> builder = new BindingBuilder<>(fullKey);
		builders.add((BindingBuilder<Object>) builder);
		return builder;
	}

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinder#toSet multibinds} each of such sets together.
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Binding<T> binding) {
		Key<Set<S>> set = Key.ofType(Types.parameterized(Set.class, setOf.getType()), setOf.getName());
		multibind(set, Multibinder.toSet());
		bind(set).to(binding.mapInstance(Collections::singleton));
	}

	/**
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#to(Key)
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Key<T> item) {
		bindIntoSet(setOf, Binding.to(item));
	}

	/**
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#toInstance(Object)
	 */
	protected final <S, T extends S> void bindIntoSet(@NotNull Key<S> setOf, @NotNull T element) {
		bindIntoSet(setOf, Binding.toInstance(element));
	}

	/**
	 * @see #bind(Key)
	 */
	@SuppressWarnings("unchecked")
	protected final <T> BindingBuilder<T> bind(Class<T> type) {
		checkState(configuring, "Cannot add bindings before or after configure() call");

		BindingBuilder<T> builder = new BindingBuilder<>(Key.of(type));
		builders.add((BindingBuilder<Object>) builder);
		return builder;
	}

	/**
	 * {@link #bindIntoSet(Key, Key) Binds into set} a key of instance injector for given type at a {@link Injector#postInjectInstances special}
	 * key Set&lt;InstanceInjector&lt;?&gt;&gt;.
	 * <p>
	 * Instance injector bindings are {@link DefaultModule generated automatically}.
	 *
	 * @see Injector#postInjectInstances
	 */
	protected final <T> void postInjectInto(Key<T> key) {
		bindIntoSet(new Key<InstanceInjector<?>>() {}, Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName()));
	}

	/**
	 * @see #postInjectInto(Key)
	 */
	protected final <T> void postInjectInto(Class<T> type) {
		postInjectInto(Key.of(type));
	}

	/**
	 * Adds a {@link Multibinder multibinder} for a given key to this module.
	 */
	protected final <T> void multibind(Key<T> key, Multibinder<T> multibinder) {
		checkState(configuring, "Cannot add multibinders before or after configure() call");

		multibinders.put(key, multibinder);
	}

	/**
	 * Adds a {@link BindingGenerator generator} for a given class to this module.
	 */
	protected final <T> void generate(Class<?> pattern, BindingGenerator<T> bindingGenerator) {
		checkState(configuring, "Cannot add generators before or after configure() call");

		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>()).add(bindingGenerator);
	}

	/**
	 * Adds a {@link BindingTransformer transformer} with a given priority to this module.
	 */
	protected final <T> void transform(int priority, BindingTransformer<T> bindingTransformer) {
		checkState(configuring, "Cannot add transformers before or after configure() call");

		bindingTransformers.computeIfAbsent(priority, $ -> new HashSet<>()).add(bindingTransformer);
	}

	synchronized private void doConfigure() {
		if (configured) {
			return;
		}
		configuring = true;
		configure();
		addDeclarativeBindingsFrom(this);
		configuring = false;
		configured = true;

		builders.forEach(b ->
				bindings.computeIfAbsent(b.scope, $ -> new HashMap<>())
						.get()
						.computeIfAbsent(b.key, $ -> new HashSet<>())
						.add(b.binding));

		Set<Key<?>> exportedKeys = builders.stream().filter(b -> b.exported).map(b -> b.key).collect(toSet());

		if (!exportedKeys.isEmpty()) {
			Module exported = Modules.of(bindings, bindingTransformers, bindingGenerators, multibinders).export(exportedKeys);
			bindings = exported.getBindings();
			bindingTransformers = exported.getBindingTransformers();
			bindingGenerators = exported.getBindingGenerators();
			multibinders = exported.getMultibinders();
		}
	}

	@Override
	public final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
		doConfigure();
		return bindings;
	}

	@Override
	public final Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
		doConfigure();
		return bindingTransformers;
	}

	@Override
	public final Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		doConfigure();
		return bindingGenerators;
	}

	@Override
	public final Map<Key<?>, Multibinder<?>> getMultibinders() {
		doConfigure();
		return multibinders;
	}

	@Override
	public final Module combineWith(Module another) {
		return Module.super.combineWith(another);
	}

	@Override
	public final Module overrideWith(Module another) {
		return Module.super.overrideWith(another);
	}

	@Override
	public final Module transformWith(Function<Module, Module> fn) {
		return Module.super.transformWith(fn);
	}

	@Override
	public final Trie<Scope, Map<Key<?>, Binding<?>>> resolveBindings() {
		return Module.super.resolveBindings();
	}
}
