#!/usr/bin/env python

# Copyright (C) 2006-2013  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# This file is part of Augustus.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module defines as many Numpy classes and functions as are
needed to run Augusuts."""

try:
    import java.lang.IllegalArgumentException
    import java.lang.IndexOutOfBoundsException

    from com.opendatagroup.NumpySubset import Numpy, ArrayBoolean1d, ArrayInteger1d, ArrayDouble1d

    class ndarray(object):
        def __init__(self, array):
            self.array = array
            if isinstance(self.array, ArrayBoolean1d):
                self.dtype = bool
            elif isinstance(self.array, ArrayInteger1d):
                self.dtype = int
            elif isinstance(self.array, ArrayDouble1d):
                self.dtype = double
            else:
                raise Exception

        def __len__(self):
            return self.array.len()

        def __repr__(self):
            get = self.array.get
            return "array(%r)" % [get(i) for i in xrange(self.array.len())]

        def __iter__(self):
            for i in xrange(len(self)):
                yield self.array.get(i)

        def copy(self):
            return Numpy.copy(self.array)

        def __add__(self, other):
            return add(self, other)

        def __sub__(self, other):
            return subtract(self, other)

        def __mul__(self, other):
            return multiply(self, other)

        def __div__(self, other):
            return divide(self, other)

        def __pow__(self, other):
            return power(self, other)

        def __radd__(self, other):
            return add(self, other)

        def __rsub__(self, other):
            return subtract(self, other)

        def __rmul__(self, other):
            return multiply(self, other)

        def __rdiv__(self, other):
            return divide(self, other)

        def __rpow__(self, other):
            return power(self, other)

        def __iadd__(self, other):
            add(self, other, self.array)
            return self

        def __isub__(self, other):
            subtract(self, other, self.array)
            return self

        def __imul__(self, other):
            multiply(self, other, self.array)
            return self

        def __idiv__(self, other):
            divide(self, other, self.array)
            return self

        def __neg__(self):
            return negative(self)

        def __lt__(self, other):
            return less(self, other)

        def __le__(self, other):
            return less_equal(self, other)

        def __eq__(self, other):
            return equal(self, other)

        def __ne__(self, other):
            return not_equal(self, other)

        def __gt__(self, other):
            return greater(self, other)

        def __ge__(self, other):
            return greater_equal(self, other)

        def __getitem__(self, i):
            if isinstance(i, (int, long)):
                return self.array.get(i)

            elif isinstance(i, slice):
                return ndarray(Numpy.getslice(self.array, i.start or 0, i.stop or len(self), i.step or 1))

            elif isinstance(i, ndarray):
                try:
                    return ndarray(Numpy.getfancy(self.array, i.array));
                except java.lang.IndexOutOfBoundsException:
                    raise IndexError("Index out of bounds")

            else:
                raise NotImplementedError

        def __setitem__(self, i, value):
            if isinstance(i, (int, long)):
                self.array.set(i, value)

            elif isinstance(i, slice):
                if isinstance(value, ObjectArray):
                    raise Exception
                elif isinstance(value, ndarray):
                    what = value.array
                else:
                    what = value

                try:
                    Numpy.setslice(what, self.array, i.start or 0, i.stop or len(self), i.step or 1)
                except java.lang.IllegalArgumentException:
                    raise ValueError("Could not broadcast arrays of different shapes")

            elif isinstance(i, ndarray):
                if isinstance(value, ObjectArray):
                    raise Exception
                elif isinstance(value, ndarray):
                    what = value.array
                else:
                    what = value

                try:
                    Numpy.setfancy(what, self.array, i.array)
                except java.lang.IllegalArgumentException:
                    raise ValueError("Could not broadcast arrays of different shapes")
                except java.lang.IndexOutOfBoundsException:
                    raise IndexError("Index out of bounds")

            else:
                raise NotImplementedError

    class ObjectArray(ndarray):
        def __init__(self, pythonList):
            self.pythonList = pythonList
            self.indexArray = ndarray(Numpy.indexArray(len(pythonList)))

        def __len__(self):
            return len(self.pythonList)

        def __repr__(self):
            return "array(%r)" % self.pythonList

        def __iter__(self):
            for item in self.pythonList:
                yield item

        def __getitem__(self, i):
            out = self.indexArray[i]
            if isinstance(out, int):
                return self.pythonList[out]
            else:
                return ObjectArray([self.pythonList[x] for x in out])

        def __setitem__(self, i, value):
            if isinstance(i, (int, long)):
                self.pythonList[i] = value

            elif isinstance(i, slice):
                if isinstance(value, ndarray):
                    p = self.pythonList
                    if isinstance(value, ObjectArray):
                        g = value.pythonList.__getitem__
                    else:
                        g = value.array.get
                    for index in xrange(i.start or 0, i.stop or len(self), i.step or 1):
                        p[index] = g(index)

                else:
                    p = self.pythonList
                    for index in xrange(i.start or 0, i.stop or len(self), i.step or 1):
                        p[index] = value

            elif isinstance(i, ndarray):
                if isinstance(i.array, ArrayBoolean1d):
                    if len(i) != len(self):
                        raise ValueError("Could not broadcast arrays of different shapes")

                    if isinstance(value, ndarray):
                        if Numpy.count_nonzero(i.array) != len(value):
                            raise ValueError("Could not broadcast arrays of different shapes")

                        j = 0
                        p = self.pythonList
                        if isinstance(value, ObjectArray):
                            g = value.pythonList.__getitem__
                        else:
                            g = value.array.get
                        for ii in xrange(len(self)):
                            if i.array.get(ii):
                                p[ii] = g(j)
                                j += 1

                    else:
                        p = self.pythonList
                        for ii in xrange(len(self)):
                            if i.array.get(ii):
                                p[ii] = value

                elif isinstance(i.array, ArrayInteger1d):
                    if isinstance(value, ndarray):
                        if len(i) != len(value):
                            raise ValueError("Could not broadcast arrays of different shapes")

                        l = len(self)
                        p = self.pythonList
                        for ii in xrange(len(i)):
                            index = i.array.get(ii)
                            if index < 0:
                                index = l + index
                            p[index] = value[ii]

                    else:
                        l = len(self)
                        p = self.pythonList
                        for ii in xrange(len(i)):
                            index = i.array.get(ii)
                            if index < 0:
                                index = l + index
                            p[index] = value

                else:
                    raise Exception

    bool = bool
    bool_ = bool
    bool8 = bool
    double = float
    float = float
    float_ = float
    float128 = float
    float16 = float
    float32 = float
    float64 = float
    int = int
    int_ = int
    int0 = int
    int16 = int
    int32 = int
    int64 = int
    int8 = int
    integer = int
    object = object
    object_ = object
    object0 = object
    str = str
    str_ = str
    string_ = str
    string0 = str
    uint8 = object()

    def dtype(obj):
        return obj

    def zeros(shape, dtype=float, order="C"):
        if isinstance(shape, int):
            if dtype is bool:
                return ndarray(Numpy.zerosBoolean(shape))
            elif dtype is float:
                return ndarray(Numpy.zerosDouble(shape))
            elif dtype is int:
                return ndarray(Numpy.zerosInteger(shape))
            elif dtype is object:
                return ObjectArray([0] * shape)
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

    def ones(shape, dtype=float, order="C"):
        if isinstance(shape, int):
            if dtype is bool:
                return ndarray(Numpy.onesBoolean(shape))
            elif dtype is float:
                return ndarray(Numpy.onesDouble(shape))
            elif dtype is int:
                return ndarray(Numpy.onesInteger(shape))
            elif dtype is object:
                return ObjectArray([1] * shape)
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

    def empty(shape, dtype=float, order="C"):
        if isinstance(shape, int):
            if dtype is bool:
                return ndarray(ArrayBoolean1d(shape))
            elif dtype is float:
                return ndarray(ArrayDouble1d(shape))
            elif dtype is int:
                return ndarray(ArrayInteger1d(shape))
            elif dtype is object:
                return ObjectArray([None] * shape)
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

    def array(obj, dtype=float):
        if dtype is object:
            return ObjectArray(obj[:])
        else:
            out = empty(len(obj), dtype=dtype)
            set = out.array.set
            for i, x in enumerate(obj):
                set(i, x)
            return out

    def fromiter(obj, dtype=float, count=None):
        if count is None:
            raise Exception
        out = empty(len(obj), dtype=dtype)
        set = out.array.set
        for i, x in enumerate(obj):
            set(i, x)
        return out

    def _makeUnary(name, func):
        def _unary(x, out=None):
            if out is None:
                out = x.array.__class__(len(x))
                func(x.array, out)
                return ndarray(out)
            else:
                func(x.array, out.array)
        _unary.func_name = name
        return _unary

    negative = _makeUnary("negative", Numpy.negative)
    absolute = _makeUnary("absolute", Numpy.absolute)
    rint = _makeUnary("rint", Numpy.rint)
    exp = _makeUnary("exp", Numpy.exp)
    log = _makeUnary("log", Numpy.log)
    log10 = _makeUnary("log10", Numpy.log10)
    sqrt = _makeUnary("sqrt", Numpy.sqrt)
    square = _makeUnary("square", Numpy.square)
    reciprocal = _makeUnary("reciprocal", Numpy.reciprocal)
    sin = _makeUnary("sin", Numpy.sin)
    cos = _makeUnary("cos", Numpy.cos)
    tan = _makeUnary("tan", Numpy.tan)
    arcsin = _makeUnary("arcsin", Numpy.arcsin)
    arccos = _makeUnary("arccos", Numpy.arccos)
    arctan = _makeUnary("arctan", Numpy.arctan)
    sinh = _makeUnary("sinh", Numpy.sinh)
    cosh = _makeUnary("cosh", Numpy.cosh)
    tanh = _makeUnary("tanh", Numpy.tanh)
    arcsinh = _makeUnary("arcsinh", Numpy.arcsinh)
    arccosh = _makeUnary("arccosh", Numpy.arccosh)
    arctanh = _makeUnary("arctanh", Numpy.arctanh)
    isfinite = _makeUnary("isfinite", Numpy.isfinite)
    isinf = _makeUnary("isinf", Numpy.isinf)
    isnan = _makeUnary("isnan", Numpy.isnan)

    def _makeBinary(name, func, integer=False, double=False):
        def _binary(x, y, out=None):
            if out is None:
                if isinstance(x, ndarray) and isinstance(y, ndarray):
                    length = min(len(x), len(y))
                    if isinstance(x.array, ArrayInteger1d) and isinstance(y.array, ArrayInteger1d):
                        atype = ArrayInteger1d
                    else:
                        atype = ArrayDouble1d
                    xarg = x.array
                    yarg = y.array

                elif isinstance(x, ndarray):
                    length = len(x)
                    if isinstance(x.array, ArrayInteger1d) and isinstance(y, (int, long)):
                        atype = ArrayInteger1d
                    else:
                        atype = ArrayDouble1d
                    xarg = x.array
                    yarg = y

                elif isinstance(y, ndarray):
                    length = len(y)
                    if isinstance(y.array, ArrayInteger1d) and isinstance(x, (int, long)):
                        atype = ArrayInteger1d
                    else:
                        atype = ArrayDouble1d
                    xarg = x
                    yarg = y.array

                if integer and double:
                    out = atype(length)
                elif integer:
                    out = ArrayInteger1d(length)
                elif double:
                    out = ArrayDouble1d(length)
                else:
                    out = ArrayBoolean1d(length)

                func(xarg, yarg, out)
                return ndarray(out)

            else:
                if isinstance(x, ndarray):
                    xarg = x.array
                else:
                    xarg = x

                if isinstance(y, ndarray):
                    yarg = y.array
                else:
                    yarg = y

                if isinstance(out, ndarray):
                    func(xarg, yarg, out.array)
                else:
                    func(xarg, yarg, out)

        _binary.func_name = name
        return _binary

    add = _makeBinary("add", Numpy.add, integer=True, double=True)
    subtract = _makeBinary("subtract", Numpy.subtract, integer=True, double=True)
    multiply = _makeBinary("multiply", Numpy.multiply, integer=True, double=True)
    divide = _makeBinary("divide", Numpy.divide, double=True)
    floor_divide = _makeBinary("floor_divide", Numpy.floor_divide, integer=True, double=True)
    true_divide = _makeBinary("true_divide", Numpy.true_divide, double=True)
    power = _makeBinary("power", Numpy.power, integer=True, double=True)
    mod = _makeBinary("mod", Numpy.mod, integer=True)
    fmod = _makeBinary("fmod", Numpy.fmod, double=True)
    arctan2 = _makeBinary("arctan2", Numpy.arctan2, double=True)
    greater = _makeBinary("greater", Numpy.greater)
    greater_equal = _makeBinary("greater_equal", Numpy.greater_equal)
    less = _makeBinary("less", Numpy.less)
    less_equal = _makeBinary("less_equal", Numpy.less_equal)
    not_equal = _makeBinary("not_equal", Numpy.not_equal)
    equal = _makeBinary("equal", Numpy.equal)
    logical_and = _makeBinary("logical_and", Numpy.logical_and)
    logical_or = _makeBinary("logical_or", Numpy.logical_or)
    logical_xor = _makeBinary("logical_xor", Numpy.logical_xor)
    logical_not = _makeBinary("logical_not", Numpy.logical_not)
    maximum = _makeBinary("maximum", Numpy.maximum, integer=True, double=True)
    minimum = _makeBinary("minimum", Numpy.minimum, integer=True, double=True)
    floor = _makeBinary("floor", Numpy.floor, double=True)
    ceil = _makeBinary("ceil", Numpy.ceil, double=True)

    def copy(x): return Numpy.copy(x.array)
    def count_nonzero(x): return Numpy.count_nonzero(x.array)
    def mean(x): return Numpy.mean(x.array)
    def nanmax(x): return Numpy.nanmax(x.array)
    def nanmin(x): return Numpy.nanmin(x.array)
    def std(x, ddof=0): return Numpy.std(x.array, ddof)
    def sum(x): return Numpy.sum(x.array)

except ImportError:
    pass
