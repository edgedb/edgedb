/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
*/

#ifndef __ENCODER_H__
#define __ENCODER_H__

#include <Python.h>

// see http://ecma262-5.com/ELS5_HTML.htm#Section_8.5 for Java number specs
#define JAVASCRIPT_MAXINT 9007199254740992

// type objects for some not-built-in types natively supported by the encoder
static PyTypeObject* PyType_UUID;
static PyTypeObject* PyType_Decimal;
static PyTypeObject* PyType_Col_OrderedDict;
static PyTypeObject* PyType_Col_Set;
static PyTypeObject* PyType_Col_Sequence;
static PyTypeObject* PyType_Col_Mapping;

// base class for the Encoder class we implement in c
static PyTypeObject* PyType_BaseEncoder;

typedef struct {
    PyObject_HEAD
    bool use_hook;
} PyEncoderObject;

#endif
