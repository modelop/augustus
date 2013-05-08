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

public class ArrayBoolean1d implements ArrayNumber1d {
    private boolean[] array;
    private int length;

    public ArrayBoolean1d(int length) {
        this.array = new boolean[length];
        this.length = length;
    }
    
    public int len() {
        return this.length;
    }

    public boolean get(int i) {
        return this.array[i];
    }

    public double getNumber(int i) {
        return (this.array[i] ? 1.0 : 0.0);
    }

    public void set(int i, boolean value) {
        this.array[i] = value;
    }

    public void set(int i, int value) {
        this.array[i] = (value != 0);
    }

    public void set(int i, double value) {
        this.array[i] = (value != 0.0);
    }
}
