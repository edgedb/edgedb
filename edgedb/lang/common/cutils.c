/*
 * Copyright (c) 2008-2010 Sprymix Inc.
 * All rights reserved.
 *
 * See LICENSE for details.
 */


#include <Python.h>


static PyObject *
cutils__clear_exc_info(PyObject *self, PyObject *args)
{
    PyThreadState *tstate = PyThreadState_GET();

    PyObject *tmp_exc_type, *tmp_exc_value, *tmp_exc_tb;

    tmp_exc_type = tstate->exc_type;
    tmp_exc_value = tstate->exc_value;
    tmp_exc_tb = tstate->exc_traceback;

    tstate->exc_type = NULL;
    tstate->exc_value = NULL;
    tstate->exc_traceback = NULL;

    Py_XDECREF(tmp_exc_type);
    Py_XDECREF(tmp_exc_value);
    Py_XDECREF(tmp_exc_tb);

    Py_RETURN_NONE;
}


static PyObject *
cutils__set_exc_info(PyObject *self, PyObject *args)
{
    PyThreadState *tstate = PyThreadState_GET();

    PyObject *exc_type, *exc_value, *exc_tb;

    if (!PyArg_UnpackTuple(args, "set_exc_info", 3, 3, &exc_type, &exc_value, &exc_tb))
        return NULL;

    Py_XDECREF(tstate->exc_type);
    Py_XDECREF(tstate->exc_value);
    Py_XDECREF(tstate->exc_traceback);

    Py_XINCREF(exc_type);
    Py_XINCREF(exc_value);
    Py_XINCREF(exc_tb);

    tstate->exc_type = exc_type;
    tstate->exc_value = exc_value;
    tstate->exc_traceback = exc_tb;

    Py_RETURN_NONE;
}


static PyMethodDef cutils_methods[] = {
    {"clear_exc_info", cutils__clear_exc_info, METH_NOARGS, NULL},
    {"set_exc_info", cutils__set_exc_info, METH_VARARGS, NULL},
    {NULL, NULL}
};


static PyModuleDef cutilsmodule = {
    PyModuleDef_HEAD_INIT,
    "semantix.utils.cutils",
    "C Level Utils",
    -1,
    cutils_methods,
    NULL, NULL, NULL, NULL
};


PyMODINIT_FUNC
PyInit_cutils(void)
{
    PyObject *module;

    module = PyModule_Create(&cutilsmodule);
    if (module == NULL)
        return NULL;

    return module;
}
