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
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.codegen.ExpressionComparator.*;
import static java.util.Arrays.asList;
import static org.objectweb.asm.Type.getType;

/**
 * Defines list of possibilities for creating the dynamic object
 */
public final class Expressions {
	private Expressions() {
	}

	public static Expression sequence(List<Expression> parts) {
		return new ExpressionSequence(parts);
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

	/**
	 * Return new variable which will process expression
	 *
	 * @param expression expression which will be processed when variable will be used
	 * @return new instance of the Expression
	 */
	public static Variable let(Expression expression) {
		return new ExpressionLet(expression);
	}

	public static Expression let(Expression expression, Function<Variable, Expression> fn) {
		return fn.apply(new ExpressionLet(expression));
	}

	public static Expression let(Expression expression1, Expression expression2, BiFunction<Variable, Variable, Expression> fn) {
		return fn.apply(new ExpressionLet(expression1), new ExpressionLet(expression2));
	}

	/**
	 * Sets the value from argument 'from' in argument 'to'
	 *
	 * @param to   variable which will be changed
	 * @param from variable which will change
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
	 * Returns the field from owner
	 *
	 * @param owner owner of the field
	 * @param field name of the field which will be returned
	 * @return new instance of the VarField
	 */
	public static VarField field(Expression owner, String field) {
		return new VarField(owner, field);
	}

	/**
	 * Sets value to the field in owner
	 *
	 * @param owner owner of the field
	 * @param field name of field which will be changed
	 * @param value new value for the field
	 * @return new instance of the ExpressionSet
	 */
	public static Expression set(Expression owner, String field, Expression value) {
		return set(field(owner, field), value);
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
	public static VarArg arg(int argument) {
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
		return PredicateDefAnd.create(predicateDefs);
	}

	/**
	 * Returns result of logical 'and' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefAnd
	 */
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
		return PredicateDefOr.create(predicateDefs);
	}

	/**
	 * Returns result of logical 'or' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefAnd
	 */
	public static PredicateDefOr or(PredicateDef... predicateDefs) {
		return or(asList(predicateDefs));
	}

	/**
	 * Verifies that the fields are equal
	 *
	 * @param fields list of the fields
	 * @return new instance of the Expression
	 */
	public static Expression asEquals(List<String> fields) {
		PredicateDefAnd predicate = PredicateDefAnd.create();
		for (String field : fields) {
			predicate.add(cmpEq(
					field(self(), field),
					field(cast(arg(0), ExpressionCast.THIS_TYPE), field)));
		}
		return predicate;
	}

	/**
	 * Returns the string which was constructed from fields
	 *
	 * @param fields list of the fields
	 * @return new instance of the ExpressionToString
	 */
	public static Expression asString(List<String> fields) {
		ExpressionToString toString = new ExpressionToString();
		for (String field : fields) {
			toString.withArgument(field + "=", field(self(), field));
		}
		return toString;
	}

	/**
	 * Returns the string which was constructed from fields
	 *
	 * @param fields list of the fields
	 * @return new instance of the ExpressionToString
	 */
	public static Expression asString(String... fields) {
		return asString(asList(fields));
	}

	/**
	 * Verifies that the fields are equal
	 *
	 * @param fields list of the fields
	 * @return new instance of the Expression
	 */
	public static Expression asEquals(String... fields) {
		return asEquals(asList(fields));
	}

	/**
	 * Compares the fields
	 *
	 * @param type   type of the fields
	 * @param fields fields which will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compare(Class<?> type, List<String> fields) {
		ExpressionComparator comparator = ExpressionComparator.create();
		for (String field : fields) {
			comparator = comparator.with(leftField(type, field), rightField(type, field), true);
		}
		return comparator;
	}

	/**
	 * Compares the fields
	 *
	 * @param type   type of the fields
	 * @param fields fields which will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compare(Class<?> type, String... fields) {
		return compare(type, asList(fields));
	}

	/**
	 * Compares the fields
	 *
	 * @param fields list of the fields with will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compareTo(List<String> fields) {
		ExpressionComparator comparator = ExpressionComparator.create();
		for (String field : fields) {
			comparator = comparator.with(thisField(field), thatField(field), true);
		}
		return comparator;
	}

	/**
	 * Compares the fields
	 *
	 * @param fields list of the fields with will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compareTo(String... fields) {
		return compareTo(asList(fields));
	}

	/**
	 * Returns new constant for the value
	 *
	 * @param value value which will be created as constant
	 * @return new instance of the ExpressionConstant
	 */
	public static ExpressionConstant value(Object value) {
		return new ExpressionConstant(value);
	}

	/**
	 * Returns hash of the fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static ExpressionHash hashCodeOfThis(List<String> fields) {
		List<Expression> arguments = new ArrayList<>();
		for (String field : fields) {
			arguments.add(field(new VarThis(), field));
		}
		return new ExpressionHash(arguments);
	}

	public static ExpressionHash hashCodeOfThis(String... fields) {
		return hashCodeOfThis(asList(fields));
	}

	/**
	 * Returns a hash code which calculated from fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static ExpressionHash hashCodeOfArgs(List<Expression> fields) {
		return new ExpressionHash(fields);
	}

	/**
	 * Returns a hash code which calculated from fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static ExpressionHash hashCodeOfArgs(Expression... fields) {
		return hashCodeOfArgs(asList(fields));
	}

	public static Class<?> unifyArithmeticTypes(Class<?>... types) {
		return ExpressionArithmeticOp.unifyArithmeticTypes(types);
	}

	public static Class<?> unifyArithmeticTypes(List<Class<?>> types) {
		return ExpressionArithmeticOp.unifyArithmeticTypes(types.toArray(new Class[0]));
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
	 * Calls method which defines static in the class
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

	public static Expression switchForPosition(Expression position, List<Expression> list) {
		return new ExpressionSwitch(position, null, list);
	}

	public static Expression switchForPosition(Expression position, Expression... expressions) {
		return new ExpressionSwitch(position, null, asList(expressions));
	}

	public static Expression switchForPosition(Expression position, Expression defaultExp, List<Expression> list) {
		return new ExpressionSwitch(position, defaultExp, list);
	}

	public static Expression switchForPosition(Expression position, Expression defaultExp, Expression... expressions) {
		return new ExpressionSwitch(position, defaultExp, asList(expressions));
	}

	public static Expression switchForKey(Expression key, List<Expression> listKey, List<Expression> listValue) {
		return new ExpressionSwitchForKey(key, null, listKey, listValue);
	}

	public static Expression switchForKey(Expression key, Expression defaultExp, List<Expression> listKey, List<Expression> listValue) {
		return new ExpressionSwitchForKey(key, defaultExp, listKey, listValue);
	}

	public static Expression setArrayItem(Expression array, Expression position, Expression newElement) {
		return new ExpressionArraySet(array, position, newElement);
	}

	public static Expression forEach(Expression collection, Function<ExpressionParameter, Expression> it) {
		return forEach(collection, Object.class, it);
	}

	public static Expression forEach(Expression collection, Class<?> type, Function<ExpressionParameter, Expression> it) {
		return new ExpressionIteratorForEach(collection, type,
				ExpressionParameter.bind("it", it));
	}

	public static Expression expressionFor(Expression from, Expression to, Function<ExpressionParameter, Expression> it) {
		return new ExpressionFor(from, to,
				ExpressionParameter.bind("it", it));
	}

	public static Expression mapForEach(Expression collection,
			Function<ExpressionParameter, Expression> key, Function<ExpressionParameter, Expression> value) {
		return new ExpressionMapForEach(collection,
				ExpressionParameter.bind("key", key), ExpressionParameter.bind("value", value));
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
