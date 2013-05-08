// Copyright (C) 2006-2013  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
//
// This file is part of Augustus.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.NumpySubset;

import java.lang.Math;
import java.lang.Double;
import java.lang.IllegalArgumentException;

import com.opendatagroup.NumpySubset.Array1d;
import com.opendatagroup.NumpySubset.ArrayNumber1d;
import com.opendatagroup.NumpySubset.ArrayBoolean1d;
import com.opendatagroup.NumpySubset.ArrayInteger1d;
import com.opendatagroup.NumpySubset.ArrayDouble1d;

public class Numpy {
    public static ArrayInteger1d indexArray(int length) {
        ArrayInteger1d out = new ArrayInteger1d(length);
        for (int i = 0;  i < length;  i++) {
            out.set(i, i);
        }
        return out;
    }

    public static ArrayBoolean1d zerosBoolean(int length) {
        ArrayBoolean1d out = new ArrayBoolean1d(length);
        for (int i = 0;  i < length;  i++) {
            out.set(i, false);
        }
        return out;
    }
    public static ArrayBoolean1d onesBoolean(int length) {
        ArrayBoolean1d out = new ArrayBoolean1d(length);
        for (int i = 0;  i < length;  i++) {
            out.set(i, 1);
        }
        return out;
    }
    public static ArrayInteger1d zerosInteger(int length) {
        ArrayInteger1d out = new ArrayInteger1d(length);
        for (int i = 0;  i < length;  i++) {
            out.set(i, 0);
        }
        return out;
    }
    public static ArrayInteger1d onesInteger(int length) {
        ArrayInteger1d out = new ArrayInteger1d(length);
        for (int i = 0;  i < length;  i++) {
            out.set(i, 1);
        }
        return out;
    }
    public static ArrayDouble1d zerosDouble(int length) {
        ArrayDouble1d out = new ArrayDouble1d(length);
        for (int i = 0;  i < length;  i++) {
            out.set(i, 0.0);
        }
        return out;
    }
    public static ArrayDouble1d onesDouble(int length) {
        ArrayDouble1d out = new ArrayDouble1d(length);
        for (int i = 0;  i < length;  i++) {
            out.set(i, 1.0);
        }
        return out;
    }

    public static ArrayBoolean1d getslice(ArrayBoolean1d array, int min, int max, int step) {
        if (min < 0) {
            min = array.len() + min;
        }
        if (max < 0) {
            max = array.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        ArrayBoolean1d out = new ArrayBoolean1d(length);
        int j = 0;
        for (int i = min;  i < max;  i += step) {
            out.set(j, array.get(i));
            j += 1;
        }
        return out;
    }

    public static void setslice(ArrayBoolean1d from, Array1d to, int min, int max, int step) throws IllegalArgumentException {
        if (min < 0) {
            min = to.len() + min;
        }
        if (max < 0) {
            max = to.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        if (length != from.len()) {
            throw new IllegalArgumentException();
        }

        int j = 0;
        for (int i = min;  i < max;  i += step) {
            to.set(i, from.get(j));
            j += 1;
        }
    }

    public static void setslice(boolean from, Array1d to, int min, int max, int step) throws IllegalArgumentException {
        if (min < 0) {
            min = to.len() + min;
        }
        if (max < 0) {
            max = to.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        for (int i = min;  i < max;  i += step) {
            to.set(i, from);
        }
    }

    public static ArrayBoolean1d getfancy(ArrayBoolean1d array, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (array.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        int count = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) { count += 1; }
        }

        ArrayBoolean1d out = new ArrayBoolean1d(count);
        int j = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                out.set(j, array.get(i));
                j += 1;
            }
        }
        return out;
    }

    public static ArrayBoolean1d getfancy(ArrayBoolean1d array, ArrayInteger1d selection) {
        ArrayBoolean1d out = new ArrayBoolean1d(selection.len());
        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = array.len() + index;
            }
            out.set(i, array.get(index));
        }
        return out;
    }

    public static void setfancy(ArrayBoolean1d from, ArrayBoolean1d to, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (to.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        int count = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) { count += 1; }
        }

        if (from.len() != count) {
            throw new IllegalArgumentException();
        }

        int j = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                to.set(i, from.get(j));
                j += 1;
            }
        }
    }

    public static void setfancy(boolean from, ArrayBoolean1d to, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (to.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                to.set(i, from);
            }
        }
    }

    public static void setfancy(ArrayBoolean1d from, ArrayBoolean1d to, ArrayInteger1d selection) throws IllegalArgumentException {
        if (from.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = to.len() + index;
            }
            to.set(index, from.get(i));
        }
    }

    public static void setfancy(boolean from, ArrayBoolean1d to, ArrayInteger1d selection) throws IllegalArgumentException {
        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = to.len() + index;
            }
            to.set(index, from);
        }
    }

    public static ArrayInteger1d getslice(ArrayInteger1d array, int min, int max, int step) {
        if (min < 0) {
            min = array.len() + min;
        }
        if (max < 0) {
            max = array.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        ArrayInteger1d out = new ArrayInteger1d(length);
        int j = 0;
        for (int i = min;  i < max;  i += step) {
            out.set(j, array.get(i));
            j += 1;
        }
        return out;
    }

    public static void setslice(ArrayInteger1d from, Array1d to, int min, int max, int step) throws IllegalArgumentException {
        if (min < 0) {
            min = to.len() + min;
        }
        if (max < 0) {
            max = to.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        if (length != from.len()) {
            throw new IllegalArgumentException();
        }

        int j = 0;
        for (int i = min;  i < max;  i += step) {
            to.set(i, from.get(j));
            j += 1;
        }
    }

    public static void setslice(int from, Array1d to, int min, int max, int step) throws IllegalArgumentException {
        if (min < 0) {
            min = to.len() + min;
        }
        if (max < 0) {
            max = to.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        for (int i = min;  i < max;  i += step) {
            to.set(i, from);
        }
    }

    public static ArrayInteger1d getfancy(ArrayInteger1d array, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (array.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        int count = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) { count += 1; }
        }

        ArrayInteger1d out = new ArrayInteger1d(count);
        int j = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                out.set(j, array.get(i));
                j += 1;
            }
        }
        return out;
    }

    public static ArrayInteger1d getfancy(ArrayInteger1d array, ArrayInteger1d selection) {
        ArrayInteger1d out = new ArrayInteger1d(selection.len());
        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = array.len() + index;
            }
            out.set(i, array.get(index));
        }
        return out;
    }

    public static void setfancy(ArrayInteger1d from, ArrayInteger1d to, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (to.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        int count = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) { count += 1; }
        }

        if (from.len() != count) {
            throw new IllegalArgumentException();
        }

        int j = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                to.set(i, from.get(j));
                j += 1;
            }
        }
    }

    public static void setfancy(int from, ArrayInteger1d to, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (to.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                to.set(i, from);
            }
        }
    }

    public static void setfancy(ArrayInteger1d from, ArrayInteger1d to, ArrayInteger1d selection) throws IllegalArgumentException {
        if (from.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = to.len() + index;
            }
            to.set(index, from.get(i));
        }
    }

    public static void setfancy(int from, ArrayInteger1d to, ArrayInteger1d selection) throws IllegalArgumentException {
        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = to.len() + index;
            }
            to.set(index, from);
        }
    }

    public static ArrayDouble1d getslice(ArrayDouble1d array, int min, int max, int step) {
        if (min < 0) {
            min = array.len() + min;
        }
        if (max < 0) {
            max = array.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        ArrayDouble1d out = new ArrayDouble1d(length);
        int j = 0;
        for (int i = min;  i < max;  i += step) {
            out.set(j, array.get(i));
            j += 1;
        }
        return out;
    }

    public static void setslice(ArrayDouble1d from, Array1d to, int min, int max, int step) throws IllegalArgumentException {
        if (min < 0) {
            min = to.len() + min;
        }
        if (max < 0) {
            max = to.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        if (length != from.len()) {
            throw new IllegalArgumentException();
        }

        int j = 0;
        for (int i = min;  i < max;  i += step) {
            to.set(i, from.get(j));
            j += 1;
        }
    }

    public static void setslice(double from, Array1d to, int min, int max, int step) throws IllegalArgumentException {
        if (min < 0) {
            min = to.len() + min;
        }
        if (max < 0) {
            max = to.len() + max;
        }

        int length = max - min;
        if (length < 0) { length = 0; }
        length = (int)Math.ceil((double)length / (double)step);

        for (int i = min;  i < max;  i += step) {
            to.set(i, from);
        }
    }

    public static ArrayDouble1d getfancy(ArrayDouble1d array, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (array.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        int count = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) { count += 1; }
        }

        ArrayDouble1d out = new ArrayDouble1d(count);
        int j = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                out.set(j, array.get(i));
                j += 1;
            }
        }
        return out;
    }

    public static ArrayDouble1d getfancy(ArrayDouble1d array, ArrayInteger1d selection) {
        ArrayDouble1d out = new ArrayDouble1d(selection.len());
        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = array.len() + index;
            }
            out.set(i, array.get(index));
        }
        return out;
    }

    public static void setfancy(ArrayDouble1d from, ArrayDouble1d to, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (to.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        int count = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) { count += 1; }
        }

        if (from.len() != count) {
            throw new IllegalArgumentException();
        }

        int j = 0;
        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                to.set(i, from.get(j));
                j += 1;
            }
        }
    }

    public static void setfancy(double from, ArrayDouble1d to, ArrayBoolean1d selection) throws IllegalArgumentException {
        if (to.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        for (int i = 0;  i < selection.len();  i++) {
            if (selection.get(i)) {
                to.set(i, from);
            }
        }
    }

    public static void setfancy(ArrayDouble1d from, ArrayDouble1d to, ArrayInteger1d selection) throws IllegalArgumentException {
        if (from.len() != selection.len()) {
            throw new IllegalArgumentException();
        }

        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = to.len() + index;
            }
            to.set(index, from.get(i));
        }
    }

    public static void setfancy(double from, ArrayDouble1d to, ArrayInteger1d selection) throws IllegalArgumentException {
        for (int i = 0;  i < selection.len();  i++) {
            int index = selection.get(i);
            if (index < 0) {
                index = to.len() + index;
            }
            to.set(index, from);
        }
    }

    // add(x1, x2[, out])	Add arguments element-wise.
    public static void add(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) + x2.get(i));
        }
    }
    public static void add(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) + x2);
        }
    }
    public static void add(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 + x2.get(i));
        }
    }
    public static void add(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) + x2.getNumber(i));
        }
    }
    public static void add(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) + x2);
        }
    }
    public static void add(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 + x2.getNumber(i));
        }
    }

    // subtract(x1, x2[, out])	Subtract arguments, element-wise.
    public static void subtract(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) - x2.get(i));
        }
    }
    public static void subtract(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) - x2);
        }
    }
    public static void subtract(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 - x2.get(i));
        }
    }
    public static void subtract(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) - x2.getNumber(i));
        }
    }
    public static void subtract(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) - x2);
        }
    }
    public static void subtract(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 - x2.getNumber(i));
        }
    }

    // multiply(x1, x2[, out])	Multiply arguments element-wise.
    public static void multiply(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) * x2.get(i));
        }
    }
    public static void multiply(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) * x2);
        }
    }
    public static void multiply(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 * x2.get(i));
        }
    }
    public static void multiply(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) * x2.getNumber(i));
        }
    }
    public static void multiply(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) * x2);
        }
    }
    public static void multiply(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 * x2.getNumber(i));
        }
    }

    // divide(x1, x2[, out])	Divide arguments element-wise.
    public static void divide(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            if (x2.getNumber(i) == 0.0) {
                if (x1.getNumber(i) == 0.0) {
                    out.set(i, Double.NaN);
                }
                else if (x1.getNumber(i) > 0.0) {
                    out.set(i, Double.POSITIVE_INFINITY);
                }
                else {
                    out.set(i, Double.NEGATIVE_INFINITY);
                }
            }
            else {
                out.set(i, x1.getNumber(i) / x2.getNumber(i));
            }
        }
    }
    public static void divide(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        if (x2 == 0.0) {
            for (int i = 0;  i < out.len();  i++) {
                if (x1.getNumber(i) == 0.0) {
                    out.set(i, Double.NaN);
                }
                else if (x1.getNumber(i) > 0.0) {
                    out.set(i, Double.POSITIVE_INFINITY);
                }
                else {
                    out.set(i, Double.NEGATIVE_INFINITY);
                }
            }
        }
        else {
            for (int i = 0;  i < out.len();  i++) {
                out.set(i, x1.getNumber(i) / x2);
            }
        }
    }
    public static void divide(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            if (x2.getNumber(i) == 0.0) {
                if (x1 == 0.0) {
                    out.set(i, Double.NaN);
                }
                else if (x1 > 0.0) {
                    out.set(i, Double.POSITIVE_INFINITY);
                }
                else {
                    out.set(i, Double.NEGATIVE_INFINITY);
                }
            }
            else {
                out.set(i, x1 / x2.getNumber(i));
            }
        }
    }

    // logaddexp(x1, x2[, out])	Logarithm of the sum of exponentiations of the inputs.
    // logaddexp2(x1, x2[, out])	Logarithm of the sum of exponentiations of the inputs in base-2.

    // true_divide(x1, x2[, out])	Returns a true division of the inputs, element-wise.
    public static void true_divide(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        Numpy.divide(x1, x2, out);
    }

    // floor_divide(x1, x2[, out])	Return the largest integer smaller or equal to the division of the inputs.
    public static void floor_divide(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            if (x2.get(i) == 0) {
                out.set(i, 0);
            }
            else {
                out.set(i, x1.get(i) / x2.get(i));
            }
        }
    }
    public static void floor_divide(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        if (x2 == 0) {
            for (int i = 0;  i < out.len();  i++) {
                out.set(i, 0);
            }
        }
        else {
            for (int i = 0;  i < out.len();  i++) {
                out.set(i, x1.get(i) / x2);
            }
        }
    }
    public static void floor_divide(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            if (x2.get(i) == 0) {
                out.set(i, 0);
            }
            else {
                out.set(i, x1 / x2.get(i));
            }
        }
    }
    public static void floor_divide(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            if (x2.getNumber(i) == 0.0) {
                if (x1.getNumber(i) == 0.0) {
                    out.set(i, Double.NaN);
                }
                else if (x1.getNumber(i) > 0.0) {
                    out.set(i, Double.POSITIVE_INFINITY);
                }
                else {
                    out.set(i, Double.NEGATIVE_INFINITY);
                }
            }
            else {
                out.set(i, Math.floor(x1.getNumber(i) / x2.getNumber(i)));
            }
        }
    }
    public static void floor_divide(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        if (x2 == 0.0) {
            for (int i = 0;  i < out.len();  i++) {
                if (x1.getNumber(i) == 0.0) {
                    out.set(i, Double.NaN);
                }
                else if (x1.getNumber(i) > 0.0) {
                    out.set(i, Double.POSITIVE_INFINITY);
                }
                else {
                    out.set(i, Double.NEGATIVE_INFINITY);
                }
            }
        }
        else {
            for (int i = 0;  i < out.len();  i++) {
                out.set(i, Math.floor(x1.getNumber(i) / x2));
            }
        }
    }
    public static void floor_divide(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            if (x2.getNumber(i) == 0.0) {
                if (x1 == 0.0) {
                    out.set(i, Double.NaN);
                }
                else if (x1 > 0.0) {
                    out.set(i, Double.POSITIVE_INFINITY);
                }
                else {
                    out.set(i, Double.NEGATIVE_INFINITY);
                }
            }
            else {
                out.set(i, Math.floor(x1 / x2.getNumber(i)));
            }
        }
    }

    // negative(x[, out])	Returns an array with the negative of each element of the original array.
    public static void negative(ArrayInteger1d x, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, -x.get(i));
        }
    }
    public static void negative(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, -x.getNumber(i));
        }
    }

    // power(x1, x2[, out])	First array elements raised to powers from second array, element-wise.
    public static void power(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.pow(x1.get(i), x2.get(i)));
        }
    }
    public static void power(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.pow(x1.get(i), x2));
        }
    }
    public static void power(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.pow(x1, x2.get(i)));
        }
    }
    public static void power(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.pow(x1.getNumber(i), x2.getNumber(i)));
        }
    }
    public static void power(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.pow(x1.getNumber(i), x2));
        }
    }
    public static void power(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.pow(x1, x2.getNumber(i)));
        }
    }

    // remainder(x1, x2[, out])	Return element-wise remainder of division.
    // mod(x1, x2[, out])	Return element-wise remainder of division.
    public static void mod(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) % x2.get(i));
        }
    }
    public static void mod(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) % x2);
        }
    }
    public static void mod(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 % x2.get(i));
        }
    }

    // fmod(x1, x2[, out])	Return the element-wise remainder of division.
    public static void fmod(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) % x2.getNumber(i));
        }
    }
    public static void fmod(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) % x2);
        }
    }
    public static void fmod(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 % x2.getNumber(i));
        }
    }

    // absolute(x[, out])	Calculate the absolute value element-wise.
    public static void absolute(ArrayInteger1d x, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.abs(x.get(i)));
        }
    }
    public static void absolute(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.abs(x.getNumber(i)));
        }
    }

    // rint(x[, out])	Round elements of the array to the nearest integer.
    public static void rint(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.round(x.getNumber(i)));
        }
    }

    // sign(x[, out])	Returns an element-wise indication of the sign of a number.
    // conj(x[, out])	Return the complex conjugate, element-wise.
    // exp(x[, out])	Calculate the exponential of all elements in the input array.
    public static void exp(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.exp(x.getNumber(i)));
        }
    }

    // exp2(x[, out])	Calculate 2**p for all p in the input array.
    // log(x[, out])	Natural logarithm, element-wise.
    public static void log(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.log(x.getNumber(i)));
        }
    }

    // log2(x[, out])	Base-2 logarithm of x.
    // log10(x[, out])	Return the base 10 logarithm of the input array, element-wise.
    public static void log10(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.log10(x.getNumber(i)));
        }
    }

    // expm1(x[, out])	Calculate exp(x) - 1 for all elements in the array.
    // log1p(x[, out])	Return the natural logarithm of one plus the input array, element-wise.
    // sqrt(x[, out])	Return the positive square-root of an array, element-wise.
    public static void sqrt(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.sqrt(x.getNumber(i)));
        }
    }

    // square(x[, out])	Return the element-wise square of the input.
    public static void square(ArrayInteger1d x, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x.get(i) * x.get(i));
        }
    }
    public static void square(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x.getNumber(i) * x.getNumber(i));
        }
    }

    // reciprocal(x[, out])	Return the reciprocal of the argument, element-wise.
    public static void reciprocal(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            if (x.getNumber(i) == 0.0) {
                out.set(i, Double.POSITIVE_INFINITY);
            }
            else {
                out.set(i, 1.0 * x.getNumber(i));
            }
        }
    }

    // ones_like(x[, out])	Returns an array of ones with the same shape and type as a given array.

    // sin(x[, out])	Trigonometric sine, element-wise.
    public static void sin(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.sin(x.getNumber(i)));
        }
    }

    // cos(x[, out])	Cosine elementwise.
    public static void cos(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.cos(x.getNumber(i)));
        }
    }

    // tan(x[, out])	Compute tangent element-wise.
    public static void tan(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.tan(x.getNumber(i)));
        }
    }

    // arcsin(x[, out])	Inverse sine, element-wise.
    public static void arcsin(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.asin(x.getNumber(i)));
        }
    }

    // arccos(x[, out])	Trigonometric inverse cosine, element-wise.
    public static void arccos(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.acos(x.getNumber(i)));
        }
    }

    // arctan(x[, out])	Trigonometric inverse tangent, element-wise.
    public static void arctan(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.atan(x.getNumber(i)));
        }
    }

    // arctan2(x1, x2[, out])	Element-wise arc tangent of x1/x2 choosing the quadrant correctly.
    public static void arctan2(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.atan2(x1.getNumber(i), x2.getNumber(i)));
        }
    }
    public static void arctan2(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.atan2(x1.getNumber(i), x2));
        }
    }
    public static void arctan2(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.atan2(x1, x2.getNumber(i)));
        }
    }

    // hypot(x1, x2[, out])	Given the “legs” of a right triangle, return its hypotenuse.
    // sinh(x[, out])	Hyperbolic sine, element-wise.
    public static void sinh(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.sinh(x.getNumber(i)));
        }
    }

    // cosh(x[, out])	Hyperbolic cosine, element-wise.
    public static void cosh(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.cosh(x.getNumber(i)));
        }
    }

    // tanh(x[, out])	Compute hyperbolic tangent element-wise.
    public static void tanh(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.tanh(x.getNumber(i)));
        }
    }

    // arcsinh(x[, out])	Inverse hyperbolic sine elementwise.
    public static void arcsinh(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            double xi = x.getNumber(i);
            out.set(i, Math.log(xi + Math.sqrt(xi*xi + 1.0)));
        }
    }

    // arccosh(x[, out])	Inverse hyperbolic cosine, elementwise.
    public static void arccosh(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            double xi = x.getNumber(i);
            out.set(i, Math.log(xi + Math.sqrt(xi*xi - 1.0)));
        }
    }

    // arctanh(x[, out])	Inverse hyperbolic tangent elementwise.
    public static void arctanh(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            double xi = x.getNumber(i);
            out.set(i, 0.5*Math.log((xi + 1.0)/(xi - 1.0)));
        }
    }

    // deg2rad(x[, out])	Convert angles from degrees to radians.
    // rad2deg(x[, out])	Convert angles from radians to degrees.

    // bitwise_and(x1, x2[, out])	Compute the bit-wise AND of two arrays element-wise.
    // bitwise_or(x1, x2[, out])	Compute the bit-wise OR of two arrays element-wise.
    // bitwise_xor(x1, x2[, out])	Compute the bit-wise XOR of two arrays element-wise.
    // invert(x[, out])	Compute bit-wise inversion, or bit-wise NOT, element-wise.
    // left_shift(x1, x2[, out])	Shift the bits of an integer to the left.
    // right_shift(x1, x2[, out])	Shift the bits of an integer to the right.

    // greater(x1, x2[, out])	Return the truth value of (x1 > x2) element-wise.
    public static void greater(ArrayInteger1d x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) > x2.get(i));
        }
    }
    public static void greater(ArrayInteger1d x1, int x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) > x2);
        }
    }
    public static void greater(int x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 > x2.get(i));
        }
    }
    public static void greater(ArrayNumber1d x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) > x2.getNumber(i));
        }
    }
    public static void greater(ArrayNumber1d x1, double x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) > x2);
        }
    }
    public static void greater(double x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 > x2.getNumber(i));
        }
    }

    // greater_equal(x1, x2[, out])	Return the truth value of (x1 >= x2) element-wise.
    public static void greater_equal(ArrayInteger1d x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) >= x2.get(i));
        }
    }
    public static void greater_equal(ArrayInteger1d x1, int x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) >= x2);
        }
    }
    public static void greater_equal(int x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 >= x2.get(i));
        }
    }
    public static void greater_equal(ArrayNumber1d x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) >= x2.getNumber(i));
        }
    }
    public static void greater_equal(ArrayNumber1d x1, double x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) >= x2);
        }
    }
    public static void greater_equal(double x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 >= x2.getNumber(i));
        }
    }

    // less(x1, x2[, out])	Return the truth value of (x1 < x2) element-wise.
    public static void less(ArrayInteger1d x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) < x2.get(i));
        }
    }
    public static void less(ArrayInteger1d x1, int x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) < x2);
        }
    }
    public static void less(int x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 < x2.get(i));
        }
    }
    public static void less(ArrayNumber1d x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) < x2.getNumber(i));
        }
    }
    public static void less(ArrayNumber1d x1, double x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) < x2);
        }
    }
    public static void less(double x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 < x2.getNumber(i));
        }
    }

    // less_equal(x1, x2[, out])	Return the truth value of (x1 =< x2) element-wise.
    public static void less_equal(ArrayInteger1d x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) <= x2.get(i));
        }
    }
    public static void less_equal(ArrayInteger1d x1, int x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) <= x2);
        }
    }
    public static void less_equal(int x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 <= x2.get(i));
        }
    }
    public static void less_equal(ArrayNumber1d x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) <= x2.getNumber(i));
        }
    }
    public static void less_equal(ArrayNumber1d x1, double x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) <= x2);
        }
    }
    public static void less_equal(double x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 <= x2.getNumber(i));
        }
    }

    // not_equal(x1, x2[, out])	Return (x1 != x2) element-wise.
    public static void not_equal(ArrayInteger1d x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) != x2.get(i));
        }
    }
    public static void not_equal(ArrayInteger1d x1, int x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) != x2);
        }
    }
    public static void not_equal(int x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 != x2.get(i));
        }
    }
    public static void not_equal(ArrayNumber1d x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) != x2.getNumber(i));
        }
    }
    public static void not_equal(ArrayNumber1d x1, double x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) != x2);
        }
    }
    public static void not_equal(double x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 != x2.getNumber(i));
        }
    }

    // equal(x1, x2[, out])	Return (x1 == x2) element-wise.
    public static void equal(ArrayInteger1d x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) == x2.get(i));
        }
    }
    public static void equal(ArrayInteger1d x1, int x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) == x2);
        }
    }
    public static void equal(int x1, ArrayInteger1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 == x2.get(i));
        }
    }
    public static void equal(ArrayNumber1d x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) == x2.getNumber(i));
        }
    }
    public static void equal(ArrayNumber1d x1, double x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.getNumber(i) == x2);
        }
    }
    public static void equal(double x1, ArrayNumber1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 == x2.getNumber(i));
        }
    }

    // logical_and(x1, x2[, out])	Compute the truth value of x1 AND x2 elementwise.
    public static void logical_and(ArrayBoolean1d x1, ArrayBoolean1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) && x2.get(i));
        }
    }
    public static void logical_and(ArrayBoolean1d x1, boolean x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) && x2);
        }
    }
    public static void logical_and(boolean x1, ArrayBoolean1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 && x2.get(i));
        }
    }

    // logical_or(x1, x2[, out])	Compute the truth value of x1 OR x2 elementwise.
    public static void logical_or(ArrayBoolean1d x1, ArrayBoolean1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) || x2.get(i));
        }
    }
    public static void logical_or(ArrayBoolean1d x1, boolean x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) || x2);
        }
    }
    public static void logical_or(boolean x1, ArrayBoolean1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 || x2.get(i));
        }
    }

    // logical_xor(x1, x2[, out])	Compute the truth value of x1 XOR x2, element-wise.
    public static void logical_xor(ArrayBoolean1d x1, ArrayBoolean1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) ^ x2.get(i));
        }
    }
    public static void logical_xor(ArrayBoolean1d x1, boolean x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1.get(i) ^ x2);
        }
    }
    public static void logical_xor(boolean x1, ArrayBoolean1d x2, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, x1 ^ x2.get(i));
        }
    }

    // logical_not(x[, out])	Compute the truth value of NOT x elementwise.
    public static void logical_not(ArrayBoolean1d x, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, !x.get(i));
        }
    }

    // maximum(x1, x2[, out])	Element-wise maximum of array elements.
    public static void maximum(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.max(x1.get(i), x2.get(i)));
        }
    }
    public static void maximum(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.max(x1.get(i), x2));
        }
    }
    public static void maximum(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.max(x1, x2.get(i)));
        }
    }
    public static void maximum(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.max(x1.getNumber(i), x2.getNumber(i)));
        }
    }
    public static void maximum(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.max(x1.getNumber(i), x2));
        }
    }
    public static void maximum(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.max(x1, x2.getNumber(i)));
        }
    }

    // minimum(x1, x2[, out])	Element-wise minimum of array elements.
    public static void minimum(ArrayInteger1d x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.min(x1.get(i), x2.get(i)));
        }
    }
    public static void minimum(ArrayInteger1d x1, int x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.min(x1.get(i), x2));
        }
    }
    public static void minimum(int x1, ArrayInteger1d x2, ArrayInteger1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.min(x1, x2.get(i)));
        }
    }
    public static void minimum(ArrayNumber1d x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.min(x1.getNumber(i), x2.getNumber(i)));
        }
    }
    public static void minimum(ArrayNumber1d x1, double x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.min(x1.getNumber(i), x2));
        }
    }
    public static void minimum(double x1, ArrayNumber1d x2, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.min(x1, x2.getNumber(i)));
        }
    }

    // isreal(x)	Returns a bool array, where True if input element is real.
    // iscomplex(x)	Returns a bool array, where True if input element is complex.
    // isfinite(x[, out])	Test element-wise for finite-ness (not infinity or not Not a Number).
    public static void isfinite(ArrayNumber1d x, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, !Double.isNaN(x.getNumber(i)) && !Double.isInfinite(x.getNumber(i)));
        }
    }

    // isinf(x[, out])	Test element-wise for positive or negative infinity.
    public static void isinf(ArrayNumber1d x, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Double.isInfinite(x.getNumber(i)));
        }
    }

    // isnan(x[, out])	Test element-wise for Not a Number (NaN), return result as a bool array.
    public static void isnan(ArrayNumber1d x, ArrayBoolean1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Double.isNaN(x.getNumber(i)));
        }
    }

    // signbit(x[, out])	Returns element-wise True where signbit is set (less than zero).
    // copysign(x1, x2[, out])	Change the sign of x1 to that of x2, element-wise.
    // nextafter(x1, x2[, out])	Return the next representable floating-point value after x1 in the direction of x2 element-wise.
    // modf(x[, out1, out2])	Return the fractional and integral parts of an array, element-wise.
    // ldexp(x1, x2[, out])	Compute y = x1 * 2**x2.
    // frexp(x[, out1, out2])	Split the number, x, into a normalized fraction (y1) and exponent (y2)
    // fmod(x1, x2[, out])	Return the element-wise remainder of division.
    // floor(x[, out])	Return the floor of the input, element-wise.
    public static void floor(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.floor(x.getNumber(i)));
        }
    }

    // ceil(x[, out])	Return the ceiling of the input, element-wise.
    public static void ceil(ArrayNumber1d x, ArrayNumber1d out) {
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, Math.ceil(x.getNumber(i)));
        }
    }

    // trunc(x[, out])	Return the truncated value of the input, element-wise.

    public static ArrayBoolean1d concatenate(ArrayBoolean1d x, ArrayBoolean1d y) {
        ArrayBoolean1d out = new ArrayBoolean1d(x.len() + y.len());
        for (int i = 0;  i < x.len();  i++) {
            out.set(i, x.get(i));
        }
        for (int i = 0;  i < y.len();  i++) {
            out.set(i, y.get(i));
        }
        return out;
    }
    public static ArrayInteger1d concatenate(ArrayInteger1d x, ArrayInteger1d y) {
        ArrayInteger1d out = new ArrayInteger1d(x.len() + y.len());
        for (int i = 0;  i < x.len();  i++) {
            out.set(i, x.get(i));
        }
        for (int i = 0;  i < y.len();  i++) {
            out.set(i, y.get(i));
        }
        return out;
    }
    public static ArrayDouble1d concatenate(ArrayDouble1d x, ArrayDouble1d y) {
        ArrayDouble1d out = new ArrayDouble1d(x.len() + y.len());
        for (int i = 0;  i < x.len();  i++) {
            out.set(i, x.get(i));
        }
        for (int i = 0;  i < y.len();  i++) {
            out.set(i, y.get(i));
        }
        return out;
    }

    public static ArrayBoolean1d copy(ArrayBoolean1d array) {
        ArrayBoolean1d out = new ArrayBoolean1d(array.len());
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, array.get(i));
        }
        return out;
    }

    public static ArrayInteger1d copy(ArrayInteger1d array) {
        ArrayInteger1d out = new ArrayInteger1d(array.len());
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, array.get(i));
        }
        return out;
    }

    public static ArrayDouble1d copy(ArrayDouble1d array) {
        ArrayDouble1d out = new ArrayDouble1d(array.len());
        for (int i = 0;  i < out.len();  i++) {
            out.set(i, array.get(i));
        }
        return out;
    }

    public static int count_nonzero(ArrayBoolean1d array) {
        int count = 0;
        for (int i = 0;  i < array.len();  i++) {
            if (array.get(i)) {
                count += 1;
            }
        }
        return count;
    }
    public static int count_nonzero(ArrayInteger1d array) {
        int count = 0;
        for (int i = 0;  i < array.len();  i++) {
            if (array.get(i) != 0) {
                count += 1;
            }
        }
        return count;
    }
    public static int count_nonzero(ArrayDouble1d array) {
        int count = 0;
        for (int i = 0;  i < array.len();  i++) {
            if (array.get(i) != 0.0) {
                count += 1;
            }
        }
        return count;
    }

    public static ArrayInteger1d cumsum(ArrayInteger1d array) {
        ArrayInteger1d out = new ArrayInteger1d(array.len());
        int sum = 0;
        for (int i = 0;  i < array.len();  i++) {
            out.set(i, sum);
            sum += array.get(i);
        }
        return out;
    }
    public static ArrayDouble1d cumsum(ArrayDouble1d array) {
        ArrayDouble1d out = new ArrayDouble1d(array.len());
        double sum = 0.0;
        for (int i = 0;  i < array.len();  i++) {
            out.set(i, sum);
            sum += array.get(i);
        }
        return out;
    }

    public static double mean(ArrayNumber1d array) {
        double numer = 0.0;
        double denom = 0.0;
        for (int i = 0;  i < array.len();  i++) {
            numer += array.getNumber(i);
            denom += 1.0;
        }
        if (denom == 0.0) {
            return Double.NaN;
        }
        else {
            return numer/denom;
        }
    }

    public static double nanmax(ArrayNumber1d array) {
        double out = array.getNumber(0);
        for (int i = 1;  i < array.len();  i++) {
            double x = array.getNumber(i);
            if (!Double.isNaN(x)) {
                if (x > out) {
                    out = x;
                }
            }
        }
        return out;
    }
    public static double nanmin(ArrayNumber1d array) {
        double out = array.getNumber(0);
        for (int i = 1;  i < array.len();  i++) {
            double x = array.getNumber(i);
            if (!Double.isNaN(x)) {
                if (x < out) {
                    out = x;
                }
            }
        }
        return out;
    }

    public static double std(ArrayNumber1d array, double ddof) {
        double sum1 = 0.0;
        double sumy = 0.0;
        double sumyy = 0.0;
        for (int i = 0;  i < array.len();  i++) {
            double y = array.getNumber(i);
            sum1 += 1.0;
            sumy += y;
            sumyy += y * y;
        }
        if (sum1 == 0.0) {
            return Double.NaN;
        }
        else {
            return Math.sqrt(((sumyy / sum1) - Math.pow(sumy / sum1, 2)) * sum1/(sum1 - ddof));
        }
    }

    public static double sum(ArrayNumber1d array) {
        double out = 0.0;
        for (int i = 0;  i < array.len();  i++) {
            out += array.getNumber(i);
        }
        return out;
    }

}
