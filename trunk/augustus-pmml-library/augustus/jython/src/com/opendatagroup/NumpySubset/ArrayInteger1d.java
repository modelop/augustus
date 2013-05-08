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
import com.opendatagroup.NumpySubset.ArrayNumber1d;

public class ArrayInteger1d implements Array1d, ArrayNumber1d {
    private int[] array;
    private int length;

    public ArrayInteger1d(int length) {
        this.array = new int[length];
        this.length = length;
    }
    
    public int len() {
        return this.length;
    }

    public int get(int i) {
        return this.array[i];
    }

    public double getNumber(int i) {
        return this.array[i];
    }

    public void set(int i, boolean value) {
        this.array[i] = (value ? 1 : 0);
    }

    public void set(int i, int value) {
        this.array[i] = value;
    }

    public void set(int i, double value) {
        this.array[i] = (int)value;
    }
}
