/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
*/

#ifndef ___ENCODER_BUFFER_H__
#define ___ENCODER_BUFFER_H__

#include <Python.h>
#include <stdbool.h>

#define BUFFERTYPE char

#define DEFAULT_BUFFER_SIZE          65536   // initial size of the buffer
#define MAX_EXTRA_ALLOCATION_SIZE  4194304   // max allocated above requested

/*====================================================================*/

typedef struct
{
    int depth;                                  // current recursion depth
    int max_depth;                              // max alowed recursion depth

    BUFFERTYPE * buffer;                        // output buffer start
    BUFFERTYPE * buffer_free;                   // first empty char in the buffer
    Py_ssize_t   buffer_size;

    BUFFERTYPE * buffer_end;                    // == buffer + buffer_size  - for speed

    PyObject *self;

    bool use_hook;
}
EncodedData;

static void encoder_data_init (EncodedData * data, PyObject *self, int max_depth, bool use_hook);
static void encoder_data_destruct (EncodedData * data);

static bool encoder_data_reserve_space (EncodedData * data, Py_ssize_t size);

static Py_ssize_t encoder_data_get_size (EncodedData * data);

static bool encoder_data_has_error (EncodedData * data);
static void encoder_data_set_error (EncodedData * data);

static void encoder_data_append_cstr (EncodedData * data, const char * cstr, Py_ssize_t max_size);
static void encoder_data_append_char (EncodedData * data, const BUFFERTYPE ch);
static void encoder_data_append_ch_nocheck (EncodedData * data, const BUFFERTYPE ch);
static void encoder_data_place_ch_nocheck (EncodedData * data, const BUFFERTYPE ch, int offset);

// current memory is ignored
static bool _encoder_buffer_allocate (EncodedData * data, Py_ssize_t size);

// current data is saved (if present)
static bool _encoder_buffer_grow     (EncodedData * data, Py_ssize_t new_size);

#endif
