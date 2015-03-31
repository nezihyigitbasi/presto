/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.createBlock;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayContains
        extends ParametricScalar
{
    public static final ArrayContains ARRAY_CONTAINS = new ArrayContains();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BOOLEAN);
    private static final String FUNCTION_NAME = "contains";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(comparableTypeParameter("T")), StandardTypes.BOOLEAN, ImmutableList.of("array<T>", "T"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Determines whether given value exists in the array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        TypeSignature valueType = type.getTypeSignature();
        TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, valueType);
        MethodHandle methodHandle = methodHandle(ArrayContains.class, "contains", Type.class, Slice.class, type.getJavaType());
        Signature signature = new Signature(FUNCTION_NAME, RETURN_TYPE, arrayType, valueType);

        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle.bindTo(type), isDeterministic(), false, ImmutableList.of(false, false));
    }

    public static boolean contains(Type type, Slice slice, Slice value)
    {
        Block arrayBlock = readStructuralBlock(slice);
        Block valueBlock = createBlock(type, value);
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (type.equalTo(arrayBlock, i, valueBlock, 0)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(Type type, Slice slice, long value)
    {
        Block arrayBlock = readStructuralBlock(slice);
        Block valueBlock = createBlock(type, value);
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (type.equalTo(arrayBlock, i, valueBlock, 0)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(Type type, Slice slice, boolean value)
    {
        Block arrayBlock = readStructuralBlock(slice);
        Block valueBlock = createBlock(type, value);
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (type.equalTo(arrayBlock, i, valueBlock, 0)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(Type type, Slice slice, double value)
    {
        Block arrayBlock = readStructuralBlock(slice);
        Block valueBlock = createBlock(type, value);
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (type.equalTo(arrayBlock, i, valueBlock, 0)) {
                return true;
            }
        }
        return false;
    }
}
