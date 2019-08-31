/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.codegen;

import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.codegen.ExpressionCast.THIS_TYPE;
import static io.datakernel.codegen.ExpressionComparator.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.objectweb.asm.Type.getType;

/**
 * Defines list of possibilities for creating dynamic objects
 */
public class Expressions {
	/**
	 * Returns new constant for the value
	 *
	 * @param value value which will be created as constant
	 * @return new instance of the ExpressionConstant
	 */
	public static Expression value(Object value) {
		return new ExpressionConstant(value);
	}

	/**
	 * Returns sequence of operations which will be processed one after the other
	 *
	 * @param parts list of operations
	 * @return new instance of the ExpressionSequence
	 */
	public static Expression sequence(Expression... parts) {
		return new ExpressionSequence(asList(parts));
	}

	public static Expression sequence(List<Expression> parts) {
		List<Expression> list = new ArrayList<>(parts.size());
		for (Expression part : parts) {
			if (part instanceof ExpressionSequence) {
				list.addAll(((ExpressionSequence) part).expressions);
			} else {
				list.add(part);
			}
		}
		return new ExpressionSequence(list);
	}

	public static Expression sequence(Consumer<List<Expression>> consumer) {
		List<Expression> expressions = new ArrayList<>();
		consumer.accept(expressions);
		return new ExpressionSequence(expressions);
	}

	/**
	 * Returns a new variable which will process expression
	 *
	 * @param expression expression which will be processed when variable will be used
	 * @return new instance of the Expression
	 */
	public static Variable var(Expression expression) {
		return new ExpressionLet(expression);
	}

	public static Expression let(Expression expression, Function<Variable, Expression> fn) {
		Variable variable = var(expression);
		return sequence(variable, fn.apply(variable));
	}

	public static Expression let(Expression[] expressions, Function<Variable[], Expression> fn) {
		Variable[] variables = Arrays.stream(expressions).map(Expressions::var).toArray(Variable[]::new);
		List<Expression> sequence = new ArrayList<>(variables.length + 1);
		sequence.addAll(asList(variables));
		sequence.add(fn.apply(variables));
		return sequence(sequence);
	}

	/**
	 * Sets the value from the argument 'from' to the argument 'to'
	 *
	 * @param to   variable which will be changed
	 * @param from variable which changes
	 * @return new instance of the Expression
	 */
	public static Expression set(StoreDef to, Expression from) {
		return new ExpressionSet(to, from);
	}

	/**
	 * Casts expression to the type
	 *
	 * @param expression expressions which will be casted
	 * @param type       expression will be casted to the 'type'
	 * @return new instance of the Expression which is casted to the type
	 */
	public static Expression cast(Expression expression, Type type) {
		return new ExpressionCast(expression, type);
	}

	/**
	 * Casts expression to the type
	 *
	 * @param expression expressions which will be casted
	 * @param type       expression will be casted to the 'type'
	 * @return new instance of the Expression which is casted to the type
	 */
	public static Expression cast(Expression expression, Class<?> type) {
		return cast(expression, getType(type));
	}

	/**
	 * Returns the property from {@code owner}
	 *
	 * @param owner    owner of the property
	 * @param property name of the property which will be returned
	 * @return new instance of the Property
	 */
	public static Variable property(Expression owner, String property) {
		return new Property(owner, property);
	}

	/**
	 * Returns the static field from {@code owner}
	 *
	 * @param owner owner of the field
	 * @param field name of the static field which will be returned
	 * @return new instance of the ExpressionStaticField
	 */
	public static Variable staticField(Class<?> owner, String field) {
		return new ExpressionStaticField(owner, field);
	}

	/**
	 * Returns current instance
	 *
	 * @return current instance of the Expression
	 */
	public static Expression self() {
		return new VarThis();
	}

	/**
	 * Returns value which ordinal number is 'argument'
	 *
	 * @param argument ordinal number in list of arguments
	 * @return new instance of the VarArg
	 */
	public static Variable arg(int argument) {
		return new VarArg(argument);
	}

	public static PredicateDef alwaysFalse() {
		return new PredicateDefConst(false);
	}

	public static PredicateDef alwaysTrue() {
		return new PredicateDefConst(true);
	}

	public static PredicateDef not(PredicateDef predicateDef) {
		return new PredicateDefNot(predicateDef);
	}

	/**
	 * Compares arguments
	 *
	 * @param eq    operation which will be used for the arguments
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return new instance of the PredicateDefCmp
	 */
	public static PredicateDef cmp(CompareOperation eq, Expression left, Expression right) {
		return new PredicateDefCmp(eq, left, right);
	}

	public static PredicateDef cmp(String eq, Expression left, Expression right) {
		return new PredicateDefCmp(CompareOperation.operation(eq), left, right);
	}

	/**
	 * Verifies that the arguments are equal
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return new instance of the PredicateDefCmp
	 */
	public static PredicateDef cmpEq(Expression left, Expression right) {
		return cmp(CompareOperation.EQ, left, right);
	}

	public static PredicateDef cmpGe(Expression left, Expression right) {
		return cmp(CompareOperation.GE, left, right);
	}

	public static PredicateDef cmpLe(Expression left, Expression right) {
		return cmp(CompareOperation.LE, left, right);
	}

	public static PredicateDef cmpLt(Expression left, Expression right) {
		return cmp(CompareOperation.LT, left, right);
	}

	public static PredicateDef cmpGt(Expression left, Expression right) {
		return cmp(CompareOperation.GT, left, right);
	}

	public static PredicateDef cmpNe(Expression left, Expression right) {
		return cmp(CompareOperation.NE, left, right);
	}

	/**
	 * Returns result of logical 'and' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefAnd
	 */
	public static PredicateDef and(List<PredicateDef> predicateDefs) {
		return new PredicateDefAnd(predicateDefs);
	}

	public static PredicateDef and(Stream<PredicateDef> predicateDefs) {
		return and(predicateDefs.collect(toList()));
	}

	public static PredicateDef and(PredicateDef... predicateDefs) {
		return and(asList(predicateDefs));
	}

	/**
	 * Returns result of logical 'or' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefOr
	 */
	public static PredicateDefOr or(List<PredicateDef> predicateDefs) {
		return new PredicateDefOr(predicateDefs);
	}

	public static PredicateDefOr or(Stream<PredicateDef> predicateDefs) {
		return or(predicateDefs.collect(toList()));
	}

	public static PredicateDefOr or(PredicateDef... predicateDefs) {
		return or(asList(predicateDefs));
	}

	/**
	 * Verifies that the properties are equal
	 *
	 * @param properties list of the properties
	 * @return new instance of the Expression
	 */
	public static Expression equalsImpl(String... properties) {
		return equalsImpl(asList(properties));
	}

	/**
	 * Verifies that the properties are equal
	 *
	 * @param properties list of the properties
	 * @return new instance of the Expression
	 */
	public static Expression equalsImpl(List<String> properties) {
		return and(properties.stream()
				.map(property -> cmpEq(
						property(self(), property),
						property(cast(arg(0), THIS_TYPE), property))));
	}

	/**
	 * Returns the string which was constructed from properties
	 *
	 * @param properties list of the properties
	 * @return new instance of the ExpressionToString
	 */
	public static Expression toStringImpl(String... properties) {
		return toStringImpl(asList(properties));
	}

	/**
	 * Returns the string which was constructed from properties
	 *
	 * @param properties list of the properties
	 * @return new instance of the ExpressionToString
	 */
	public static Expression toStringImpl(List<String> properties) {
		ExpressionToString toString = ExpressionToString.create();
		for (String property : properties) {
			toString.with(property + "=", property(self(), property));
		}
		return toString;
	}

	/**
	 * Compares the properties
	 *
	 * @param type       type of the properties
	 * @param properties properties which will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static Expression compare(Class<?> type, String... properties) {
		return compare(type, asList(properties));
	}

	/**
	 * Compares the properties
	 *
	 * @param type       type of the properties
	 * @param properties properties which will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static Expression compare(Class<?> type, List<String> properties) {
		ExpressionComparator comparator = ExpressionComparator.create();
		for (String property : properties) {
			comparator.with(leftProperty(type, property), rightProperty(type, property), true);
		}
		return comparator;
	}

	/**
	 * Compares the properties
	 *
	 * @param properties list of the properties with will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static Expression compareToImpl(String... properties) {
		return compareToImpl(asList(properties));
	}

	/**
	 * Compares the properties
	 *
	 * @param properties list of the properties with will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static Expression compareToImpl(List<String> properties) {
		ExpressionComparator comparator = ExpressionComparator.create();
		for (String property : properties) {
			comparator.with(thisProperty(property), thatProperty(property), true);
		}
		return comparator;
	}

	/**
	 * Returns a hash code which was calculated from the {@code properties}
	 *
	 * @param properties list of the properties which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static Expression hash(Expression... properties) {
		return hash(asList(properties));
	}

	/**
	 * Returns a hash code which was calculated from the {@code properties}
	 *
	 * @param properties list of the properties which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static Expression hash(List<Expression> properties) {
		return new ExpressionHash(properties);
	}

	public static Expression hashCodeImpl(String... properties) {
		return hashCodeImpl(asList(properties));
	}

	/**
	 * Returns hash of the properties
	 *
	 * @param properties list of the properties which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static Expression hashCodeImpl(List<String> properties) {
		return new ExpressionHash(properties
				.stream()
				.map(property -> property(self(), property))
				.collect(toList()));
	}

	public static Class<?> unifyArithmeticTypes(Class<?>... types) {
		return ExpressionArithmeticOp.unifyArithmeticTypes(types);
	}

	public static Class<?> unifyArithmeticTypes(List<Class<?>> types) {
		return ExpressionArithmeticOp.unifyArithmeticTypes(types.toArray(new Class<?>[0]));
	}

	public static ExpressionArithmeticOp arithmeticOp(ArithmeticOperation op, Expression left, Expression right) {
		return new ExpressionArithmeticOp(op, left, right);
	}

	public static ExpressionArithmeticOp arithmeticOp(String op, Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.operation(op), left, right);
	}

	/**
	 * Returns sum of arguments
	 *
	 * @param left  first argument which will be added
	 * @param right second argument which will be added
	 * @return new instance of the ExpressionArithmeticOp
	 */
	public static ExpressionArithmeticOp add(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.ADD, left, right);
	}

	public static ExpressionArithmeticOp inc(Expression value) {
		return new ExpressionArithmeticOp(ArithmeticOperation.ADD, value, value(1));
	}

	public static ExpressionArithmeticOp sub(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.SUB, left, right);
	}

	public static ExpressionArithmeticOp dec(Expression value) {
		return new ExpressionArithmeticOp(ArithmeticOperation.SUB, value, value(1));
	}

	public static ExpressionArithmeticOp mul(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.MUL, left, right);
	}

	public static ExpressionArithmeticOp div(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.DIV, left, right);
	}

	public static ExpressionArithmeticOp rem(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ArithmeticOperation.REM, left, right);
	}

	/**
	 * Returns new instance of class
	 *
	 * @param type   type of the constructor
	 * @param fields fields for constructor
	 * @return new instance of the ExpressionConstructor
	 */
	public static ExpressionConstructor constructor(Class<?> type, Expression... fields) {
		return new ExpressionConstructor(type, asList(fields));
	}

	/**
	 * Returns a new {@link ExpressionCall expression call}
	 * which allows to use static methods from other classes
	 *
	 * @param owner      owner of the method
	 * @param methodName name of the method in the class
	 * @param arguments  list of the arguments for the method
	 * @return new instance of the ExpressionCall
	 */
	public static Expression call(Expression owner, String methodName, Expression... arguments) {
		return new ExpressionCall(owner, methodName, Arrays.asList(arguments));
	}

	public static Expression ifThenElse(PredicateDef condition, Expression left, Expression right) {
		return new ExpressionIf(condition, left, right);
	}

	public static Expression length(Expression field) {
		return new ExpressionLength(field);
	}

	public static Expression newArray(Class<?> type, Expression length) {
		return new ExpressionNewArray(type, length);
	}

	/**
	 * Returns a new local variable which ordinal number is 'local'
	 *
	 * @param local ordinal number of local variable
	 * @return new instance of the VarArg
	 */
	public static VarLocal local(int local) {
		return new VarLocal(local);
	}

	/**
	 * Returns a new local variable from a given context
	 *
	 * @param ctx  context of a dynamic class
	 * @param type the type of the local variable to be created
	 * @return new instance of {@link VarLocal}
	 */
	public static VarLocal newLocal(Context ctx, Type type) {
		int local = ctx.getGeneratorAdapter().newLocal(type);
		return new VarLocal(local);
	}

	public static Expression callStatic(Class<?> owner, String method, Expression... arguments) {
		return new ExpressionCallStatic(owner, method, asList(arguments));
	}

	public static Expression callStaticSelf(String method, Expression... arguments) {
		return new ExpressionCallStaticSelf(method, asList(arguments));
	}

	public static Expression getArrayItem(Expression array, Expression nom) {
		return new ExpressionArrayGet(array, nom);
	}

	public static PredicateDef isNull(Expression field) {
		return new ExpressionCmpNull(field);
	}

	public static PredicateDef isNotNull(Expression field) {
		return new ExpressionCmpNotNull(field);
	}

	public static Expression nullRef(Class<?> type) {
		return new ExpressionNull(type);
	}

	public static Expression nullRef(Type type) {
		return new ExpressionNull(type);
	}

	public static Expression voidExp() {
		return ExpressionVoid.INSTANCE;
	}

	public static Expression exception(Class<? extends Throwable> exception) {
		return new ExpressionThrow(exception, null);
	}

	public static Expression exception(Class<? extends Throwable> exception, Expression message) {
		return new ExpressionThrow(exception, message);
	}

	public static Expression switchByIndex(Expression index, List<Expression> expressions) {
		return new ExpressionSwitchByIndex(index, expressions, ExpressionSwitchByIndex.DEFAULT_EXPRESSION);
	}

	public static Expression switchByIndex(Expression index, Expression... expressions) {
		return new ExpressionSwitchByIndex(index, asList(expressions), ExpressionSwitchByIndex.DEFAULT_EXPRESSION);
	}

	public static Expression switchByIndex(Expression index, List<Expression> expressions, Expression defaultExpression) {
		return new ExpressionSwitchByIndex(index, expressions, defaultExpression);
	}

	public static Expression switchByKey(Expression key, List<Expression> matchCases, List<Expression> matchExpressions) {
		return new ExpressionSwitchByKey(key, matchCases, matchExpressions, ExpressionSwitchByKey.DEFAULT_EXPRESSION);
	}

	public static Expression switchByKey(Expression key, List<Expression> matchCases, List<Expression> matchExpressions, Expression defaultExpression) {
		return new ExpressionSwitchByKey(key, matchCases, matchExpressions, defaultExpression);
	}

	public static Expression switchByKey(Expression key, Map<Expression, Expression> cases) {
		return switchByKey(key, cases, ExpressionSwitchByKey.DEFAULT_EXPRESSION);
	}

	public static Expression switchByKey(Expression key, Map<Expression, Expression> cases, Expression defaultExpression) {
		List<Expression> matchCases = new ArrayList<>();
		List<Expression> matchExpressions = new ArrayList<>();
		for (Map.Entry<Expression, Expression> entry : cases.entrySet()) {
			matchCases.add(entry.getKey());
			matchExpressions.add(entry.getValue());
		}
		return new ExpressionSwitchByKey(key, matchCases, matchExpressions, defaultExpression);
	}

	public static Expression setArrayItem(Expression array, Expression position, Expression newElement) {
		return new ExpressionArraySet(array, position, newElement);
	}

	public static Expression forEach(Expression collection, Function<Expression, Expression> it) {
		return forEach(collection, Object.class, it);
	}

	public static Expression forEach(Expression collection, Class<?> type, Function<Expression, Expression> it) {
		return new ExpressionIteratorForEach(collection, type, it);
	}

	public static Expression loop(Expression from, Expression to, Function<Expression, Expression> it) {
		return new ExpressionFor(from, to, it);
	}

	public static Expression forEach(Expression collection, Function<Expression, Expression> forEachKey, Function<Expression, Expression> forEachValue) {
		return new ExpressionMapForEach(collection, forEachKey, forEachValue);
	}

	public static Expression neg(Expression arg) {
		return new ExpressionNeg(arg);
	}

	public static Expression bitOp(BitOperation op, Expression value, Expression shift) {
		return new ExpressionBitOp(op, value, shift);
	}

	public static Expression bitOp(String op, Expression value, Expression shift) {
		return new ExpressionBitOp(BitOperation.operation(op), value, shift);
	}

	public static Expression setListItem(Expression list, Expression index, Expression value) {
		return call(list, "set", index, value);
	}

	public static Expression getListItem(Expression list, Expression index) {
		return call(cast(list, List.class), "get", index);
	}

	public static Expression addListItem(Expression list, Expression value) {
		return call(list, "add", value);
	}

}
