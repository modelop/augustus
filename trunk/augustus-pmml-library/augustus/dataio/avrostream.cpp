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

#include <Python.h>
#include <structmember.h>
#include <numpy/arrayobject.h>

#include "boost/any.hpp"
#include "boost/lexical_cast.hpp"
#include "avro/Compiler.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Specific.hh"
#include "avro/Generic.hh"
#include "avro/Stream.hh"
#include "avro/buffer/Buffer.hh"
#include "avro/DataFile.hh"

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

#define STRING 0
#define CATEGORY 1
#define INTEGER 2
#define DOUBLE 3

typedef struct {
  PyObject_HEAD

  long chunkSize;
  std::vector<std::string> names;
  std::vector<std::vector<std::string> > paths;
  std::vector<int> types;

  avro::DataFileReader<avro::GenericDatum> *dataFileReader;
  avro::GenericDatum *datum;

  std::vector<std::vector<int> > *fieldIndexes;

} avrostream_InputStream;

static PyObject *avrostream_InputStream_start(avrostream_InputStream *self, PyObject *args);
static PyObject *avrostream_InputStream_schema(avrostream_InputStream *self);
static PyObject *avrostream_InputStream_next(avrostream_InputStream *self);
static PyObject *avrostream_InputStream_close(avrostream_InputStream *self);

/*
 * PyVarObject_HEAD_INIT was added in Python 2.6.  Its use is
 * necessary to handle both Python 2 and 3.  This replacement
 * definition is for Python <= 2.5
 */
#ifndef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(type, size) \
    PyObject_HEAD_INIT(type) size,
#endif

#ifndef Py_TYPE
#define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#if PY_MAJOR_VERSION >= 3
    #define MOD_DEF(ob, name, doc, methods) \
        static struct PyModuleDef moduledef = { \
            PyModuleDef_HEAD_INIT, name, doc, -1, methods, }; \
        ob = PyModule_Create(&moduledef);
#else
    #define MOD_DEF(ob, name, doc, methods) \
        ob = Py_InitModule3(name, methods, doc);
#endif

/*
 * Python 3 only has long.
 */
#if PY_MAJOR_VERSION >= 3
#define PyInt_AsLong PyLong_AsLong
#define PyInt_Check PyLong_Check
#endif

#if PY_MAJOR_VERSION >= 3
#define PyString_Check(x) 1
#define PyString_FromString(x) PyUnicode_FromString(x)
#define PyString_FromFormat(x,y) PyUnicode_FromFormat(x,y)
#define PyString_AsString(x) PyUnicode_AS_DATA(x)
#endif

static PyMethodDef avrostream_InputStream_methods[] = {
  {"start", (PyCFunction)(avrostream_InputStream_start), METH_VARARGS, "Initialize an InputStream."},
  {"schema", (PyCFunction)(avrostream_InputStream_schema), METH_NOARGS, "Get the schema from the current file."},
  {"next", (PyCFunction)(avrostream_InputStream_next), METH_NOARGS, "Get the next record or None."},
  {"close", (PyCFunction)(avrostream_InputStream_close), METH_NOARGS, "Closes the file."},
  {NULL}
};

static PyTypeObject avrostream_InputStreamType = {
   PyVarObject_HEAD_INIT(NULL,0)
   "avrostream.InputStream",                 /* tp_name */
   sizeof(avrostream_InputStream),           /* tp_basicsize */
   0,                                        /* tp_itemsize */
   0,                                        /* tp_dealloc */
   0,                                        /* tp_print */
   0,                                        /* tp_getattr */
   0,                                        /* tp_setattr */
   0,                                        /* tp_compare */
   0,                                        /* tp_repr */
   0,                                        /* tp_as_number */
   0,                                        /* tp_as_sequence */
   0,                                        /* tp_as_mapping */
   0,                                        /* tp_hash */
   0,                                        /* tp_call */
   0,                                        /* tp_str */
   0,                                        /* tp_getattro */
   0,                                        /* tp_setattro */
   0,                                        /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT,                       /* tp_flags */
   "Low-level iterator over an avro file that extracts a desired subset of data fields.", /* tp_doc */
   0,                                        /* tp_traverse */
   0,                                        /* tp_clear */
   0,                                        /* tp_richcompare */
   0,                                        /* tp_weaklistoffset */
   0,                                        /* tp_iter */
   0,                                        /* tp_iternext */
   avrostream_InputStream_methods,           /* tp_methods */
   0,                                        /* tp_members */
   0,                                        /* tp_getset */
   0,                                        /* tp_base */
   0,                                        /* tp_dict */
   0,                                        /* tp_descr_get */
   0,                                        /* tp_descr_set */
   0,                                        /* tp_dictoffset */
   0,                                        /* tp_init */
   0,                                        /* tp_alloc */
   0,                                        /* tp_new */
};

static PyObject *avrostream_InputStream_start(avrostream_InputStream *self, PyObject *args) {
  char *fileName = NULL;
  long chunkSize = 0;
  PyObject *paths = NULL;
  PyObject *types = NULL;

  if (!PyArg_ParseTuple(args, "slOO", &fileName, &chunkSize, &paths, &types)  ||  !PyDict_Check(paths)  ||  !PyDict_Check(types)) {
    PyErr_SetString(PyExc_TypeError, "arguments: fileName [str], chunkSize [int], paths [dict(str -> seq(str))], types [dict(str -> str)]");
    return NULL;
  }

  self->chunkSize = chunkSize;

  PyObject *pathItems = PyDict_Items(paths);
  for (int i = 0;  i < PySequence_Length(pathItems);  i++) {
    PyObject *item = PySequence_GetItem(pathItems, i);
    PyObject *key = PySequence_GetItem(item, 0);
    PyObject *value = PySequence_GetItem(item, 1);
    
    if (!PyString_Check(key)  ||  !PySequence_Check(value)  ||  PyString_Check(value)) {
      PyErr_SetString(PyExc_TypeError, "third argument: paths [dict(str -> seq(str))]");
      return NULL;
    }

    self->names.push_back(std::string(PyString_AsString(key)));
    std::vector<std::string> path;

    for (int j = 0;  j < PySequence_Length(value);  j++) {
      PyObject *itemitem = PySequence_GetItem(value, j);

      if (!PyString_Check(itemitem)) {
        PyErr_SetString(PyExc_TypeError, "third argument: paths [dict(str -> seq(str))]");
        return NULL;
      }

      path.push_back(std::string(PyString_AsString(itemitem)));
    }

    self->paths.push_back(path);
  }

  if ((unsigned int)PyMapping_Length(types) != self->names.size()) {
    PyErr_SetString(PyExc_TypeError, "fourth argument (types) must have the same keys as the third (paths)");
    return NULL;
  }

  const std::string enum_string("string");
  const std::string enum_category("category");
  const std::string enum_integer("integer");
  const std::string enum_double("double");

  for (std::vector<std::string>::iterator name = self->names.begin();  name != self->names.end();  ++name) {
    PyObject *item = PyDict_GetItemString(types, name->c_str());

    if (item == NULL) {
      PyErr_SetString(PyExc_TypeError, "fourth argument (types) must have the same keys as the third (paths)");
      return NULL;
    }
    if (!PyString_Check(item)) {
        PyErr_SetString(PyExc_TypeError, "fourth argument: types [dict(str -> str)]");
        return NULL;
    }

    std::string type(PyString_AsString(item));
    if (type == enum_string) {
      self->types.push_back(STRING);
    }
    else if (type == enum_category) {
      self->types.push_back(CATEGORY);
    }
    else if (type == enum_integer) {
      self->types.push_back(INTEGER);
    }
    else if (type == enum_double) {
      self->types.push_back(DOUBLE);
    }
    else {
        PyErr_SetString(PyExc_TypeError, "fourth argument: types [dict(str -> str)] values can only be \"string\", \"category\", \"integer\", \"double\"");
        return NULL;
    }
  }

  try {
    self->dataFileReader = new avro::DataFileReader<avro::GenericDatum>(fileName);
  }
  catch (avro::Exception err) {
    PyErr_SetString(PyExc_IOError, err.what());
    return NULL;
  }

  try {
    self->datum = new avro::GenericDatum(self->dataFileReader->dataSchema());
  }
  catch (avro::Exception err) {
    PyErr_SetString(PyExc_IOError, err.what());
    self->dataFileReader->close();
    return NULL;
  }

  self->fieldIndexes = new std::vector<std::vector<int> >();
  std::vector<std::string>::const_iterator theName = self->names.begin();
  for (std::vector<std::vector<std::string> >::const_iterator path = self->paths.begin();  path != self->paths.end();  ++path, ++theName) {
    avro::NodePtr node = self->dataFileReader->dataSchema().root();

    std::vector<int> indexList;
    for (std::vector<std::string>::const_iterator pathname = path->begin();  pathname != path->end();  ++pathname) {
      if (node->type() != avro::AVRO_RECORD) {
        PyErr_SetString(PyExc_ValueError, (std::string("invalid path for \"") + *theName + std::string("\"")).c_str());
        self->dataFileReader->close();
        return NULL;
      }

      int index = -1;
      for (unsigned int i = 0;  i < node->names();  i++) {
        if (node->nameAt(i) == *pathname) {
          index = i;
        }
      }

      if (index == -1) {
        PyErr_SetString(PyExc_ValueError, "unrecognized name in schema");
        self->dataFileReader->close();
        return NULL;
      }

      indexList.push_back(index);
      node = node->leafAt(index);
    }

    if (indexList.size() == 0) {
      PyErr_SetString(PyExc_ValueError, "third argument: paths cannot have zero length");
      self->dataFileReader->close();
      return NULL;
    }

    self->fieldIndexes->push_back(indexList);
  }

  return Py_BuildValue("O", Py_None);
}

static PyObject *avrostream_InputStream_schema(avrostream_InputStream *self) {
  std::ostringstream stream;
  avro::NodePtr node = self->dataFileReader->dataSchema().root();
  node->printJson(stream, 0);
  return PyString_FromString(stream.str().c_str());
}

static PyObject *avrostream_InputStream_close(avrostream_InputStream *self) {
  try {
    if (self->dataFileReader != NULL) {
      self->dataFileReader->close();
    }
  }
  catch (avro::Exception err) {
    PyErr_SetString(PyExc_IOError, err.what());
    return NULL;
  }

  return Py_BuildValue("O", Py_None);
}

static PyObject *avrostream_InputStream_next(avrostream_InputStream *self) {
  PyObject *dict = PyDict_New();
  std::vector<PyObject*> arrays;
  std::vector<PyArrayIterObject*> arrayiters;

  for (unsigned long i = 0;  i < self->types.size();  i++) {
    int type = self->types[i];
    npy_intp dims[1] = {self->chunkSize};

    PyObject *array;
    if (type == STRING) {
      array = PyArray_SimpleNew(1, dims, NPY_OBJECT);
    }
    else if (type == CATEGORY) {
      array = PyArray_SimpleNew(1, dims, NPY_INT64);
    }
    else if (type == INTEGER) {
      array = PyArray_SimpleNew(1, dims, NPY_INT64);
    }
    else if (type == DOUBLE) {
      array = PyArray_SimpleNew(1, dims, NPY_DOUBLE);
    }

    PyDict_SetItemString(dict, self->names[i].c_str(), array);
    arrays.push_back(array);

    PyArrayIterObject *arrayiter = (PyArrayIterObject*)PyArray_IterNew(array);
    arrayiters.push_back(arrayiter);
  }

  long recordNumber = 0;
  for (;  recordNumber < self->chunkSize;  recordNumber++) {
    try {
      if (!self->dataFileReader->read(*(self->datum))) {
        break;
      }
    }
    catch (avro::Exception err) {
      PyErr_SetString(PyExc_IOError, (std::string("Avro file reading error: ") + std::string(err.what())).c_str());
      goto fail;
    }

    avro::GenericRecord &record = self->datum->value<avro::GenericRecord>();

    unsigned int nameIndex = 0;
    for (std::vector<std::vector<int> >::const_iterator indexList = self->fieldIndexes->begin();  indexList != self->fieldIndexes->end();  ++indexList, ++nameIndex) {
      avro::GenericRecord *subrecord = &record;
      avro::GenericDatum *field = &(subrecord->fieldAt((*indexList)[0]));

      for (unsigned int i = 1;  i < indexList->size();  i++) {
        if (field->type() != avro::AVRO_RECORD) {
          PyErr_SetString(PyExc_IOError, "Avro file reading error: non-record");
          goto fail;
        }
        subrecord = &(field->value<avro::GenericRecord>());
        field = &(subrecord->fieldAt((*indexList)[i]));
      }

      int type = self->types[nameIndex];
      avro::Type fieldType = field->type();

      if (type == STRING) {
        PyArrayIterObject *arrayiter = arrayiters[nameIndex];
        PyObject **dataptr = (PyObject**)arrayiter->dataptr;

        PyObject *value;
        if (fieldType == avro::AVRO_STRING) {
          std::string string = field->value<std::string>();
          value = PyString_FromString(string.c_str());
        }
        else if (fieldType == avro::AVRO_BYTES) {
          std::vector<uint8_t> bytes = field->value<std::vector<uint8_t> >();
          char *string = new char[bytes.size() + 1];
          int pointer = 0;
          for (std::vector<uint8_t>::const_iterator iter = bytes.begin();  iter != bytes.end();  ++iter, ++pointer) {
            string[pointer] = (char)(*iter);
          }
          value = PyString_FromString(string);
          delete [] string;
        }
        else if (fieldType == avro::AVRO_NULL) {
          std::string string("null");
          value = PyString_FromString(string.c_str());
        }
        else if (fieldType == avro::AVRO_BOOL) {
          std::string string(field->value<bool>() ? "true" : "false");
          value = PyString_FromString(string.c_str());
        }
        else if (fieldType == avro::AVRO_INT) {
          std::string string = boost::lexical_cast<std::string>(field->value<int32_t>());
          value = PyString_FromString(string.c_str());
        }
        else if (fieldType == avro::AVRO_LONG) {
          std::string string = boost::lexical_cast<std::string>(field->value<int64_t>());
          value = PyString_FromString(string.c_str());
        }
        else if (fieldType == avro::AVRO_FLOAT) {
          std::string string = boost::lexical_cast<std::string>(field->value<float>());
          value = PyString_FromString(string.c_str());
        }
        else if (fieldType == avro::AVRO_DOUBLE) {
          std::string string = boost::lexical_cast<std::string>(field->value<double>());
          value = PyString_FromString(string.c_str());
        }
        else {
          PyErr_SetString(PyExc_TypeError, "cannot cast Avro type into string");
          goto fail;
        }
        *dataptr = value;
          
        PyArray_ITER_NEXT(arrayiter);
      }

      else if (type == CATEGORY) {
        PyArrayIterObject *arrayiter = arrayiters[nameIndex];
        long *dataptr = (long*)arrayiter->dataptr;

        long value;
        if (fieldType == avro::AVRO_ENUM) {
          avro::GenericEnum e = field->value<avro::GenericEnum>();
          value = e.value();
        }
        else {
          PyErr_SetString(PyExc_TypeError, "cannot cast Avro type into category");
          goto fail;
        }
        *dataptr = value;

        PyArray_ITER_NEXT(arrayiter);
      }

      else if (type == INTEGER) {
        PyArrayIterObject *arrayiter = arrayiters[nameIndex];
        long *dataptr = (long*)arrayiter->dataptr;

        long value;
        if (fieldType == avro::AVRO_BOOL) {
          value = field->value<bool>() ? 1 : 0;
        }
        else if (fieldType == avro::AVRO_INT) {
          value = field->value<int32_t>();
        }
        else if (fieldType == avro::AVRO_LONG) {
          value = field->value<int64_t>();
        }
        else {
          PyErr_SetString(PyExc_TypeError, "cannot cast Avro type into integer");
          goto fail;
        }
        *dataptr = value;

        PyArray_ITER_NEXT(arrayiter);
      }

      else if (type == DOUBLE) {
        PyArrayIterObject *arrayiter = arrayiters[nameIndex];
        double *dataptr = (double*)arrayiter->dataptr;

        double value;
        if (fieldType == avro::AVRO_BOOL) {
          value = field->value<bool>() ? 1.0 : 0.0;
        }
        else if (fieldType == avro::AVRO_INT) {
          value = field->value<int32_t>();
        }
        else if (fieldType == avro::AVRO_LONG) {
          value = field->value<int64_t>();
        }
        else if (fieldType == avro::AVRO_FLOAT) {
          value = field->value<float>();
        }
        else if (fieldType == avro::AVRO_DOUBLE) {
          value = field->value<double>();
        }
        else {
          PyErr_SetString(PyExc_TypeError, "cannot cast Avro type into double");
          goto fail;
          return NULL;
        }
        *dataptr = value;

        PyArray_ITER_NEXT(arrayiter);
      }

      else {
        PyErr_SetString(PyExc_IOError, "Avro file reading error: unrecognized type");
        goto fail;
      }
    }
  }

  for (std::vector<PyArrayIterObject*>::const_iterator i = arrayiters.begin();  i != arrayiters.end();  ++i) {
    Py_XDECREF((*i));
  }

  if (recordNumber < self->chunkSize) {
    for (std::vector<PyObject*>::const_iterator array = arrays.begin();  array != arrays.end();  ++array) {
      npy_intp dims[1] = {recordNumber};
      PyArray_Dims newshape;
      newshape.ptr = dims;
      newshape.len = 1;
      PyArray_Resize((PyArrayObject*)(*array), &newshape, 1, NPY_CORDER);
    }
  }

  return dict;

 fail:
  for (std::vector<PyObject*>::const_iterator i = arrays.begin();  i != arrays.end();  ++i) {
    Py_XDECREF((*i));
  }
  for (std::vector<PyArrayIterObject*>::const_iterator i = arrayiters.begin();  i != arrayiters.end();  ++i) {
    Py_XDECREF((*i));
  }
  Py_XDECREF(dict);

  return NULL;
}

static PyMethodDef avrostream_methods[] = {
  {NULL}
};

static PyObject *moduleinit(void) {
  PyObject *m;

  avrostream_InputStreamType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&avrostream_InputStreamType) < 0) return NULL;

  MOD_DEF(m, "avrostream", "Module provides a low-level iterator over Avro data", avrostream_methods);
  if (m == NULL) {
    return NULL;
  }
  Py_INCREF(&avrostream_InputStreamType);
  PyModule_AddObject(m, "InputStream", (PyObject*)(&avrostream_InputStreamType));

  import_array1(m);
}

#if PY_MAJOR_VERSION < 3
    PyMODINIT_FUNC initavrostream(void)
    {
        moduleinit();
    }
#else
    PyMODINIT_FUNC PyInit_avrostream(void)
    {
        return moduleinit();
    }
#endif
