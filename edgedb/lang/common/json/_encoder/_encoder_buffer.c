/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
*/

#include "_encoder_buffer.h"

static void encoder_data_init (EncodedData * data, PyObject *self, int max_depth, bool use_hook)
{
    data->depth     = 0;
    data->max_depth = max_depth;
    data->self      = self;
    data->use_hook  = use_hook;

    _encoder_buffer_allocate(data, DEFAULT_BUFFER_SIZE);

    data->buffer_free = data->buffer;
}

static bool _encoder_buffer_allocate (EncodedData * data, Py_ssize_t size)
// current allocation is ignored
{
    //printf(" [Buffer: %lld chars] ",(long long)size); fflush(stdout);

    data->buffer      = (BUFFERTYPE*) malloc (size);

    if (data->buffer == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Unable to allocate memory for internal buffer");
        encoder_data_set_error(data);
        return false;
    }

    data->buffer_size = size;
    data->buffer_end  = data->buffer + size;

    return true;
}

static bool _encoder_buffer_grow (EncodedData * data, Py_ssize_t new_size)
// current data is saved (if present)
{
    if (new_size <= data->buffer_size) return true;

    BUFFERTYPE * old_buffer      = data->buffer;
    Py_ssize_t   old_buffer_size = data->buffer_size;
    BUFFERTYPE * old_buffer_free = data->buffer_free;

    if (! _encoder_buffer_allocate(data, new_size)) return false;

    if (old_buffer != NULL)
    {
        memcpy(data->buffer, old_buffer, old_buffer_size);
        free(old_buffer);
        data->buffer_free = data->buffer + (old_buffer_free - old_buffer);
    }

    return true;
}

static void encoder_data_destruct (EncodedData * data)
{
    free(data->buffer);
}

static Py_ssize_t encoder_data_get_size (EncodedData * data)
{
    return (data->buffer_free - data->buffer);
}

static bool encoder_data_reserve_space (EncodedData * data, Py_ssize_t size)
{
    if (data->buffer_free + size >= data->buffer_end)
    {
        if (encoder_data_has_error(data)) return false;

        // try to allocate twice the size of currently used space + size
        Py_ssize_t need_size = data->buffer_size > size? data->buffer_size*2 : size*2;

        // limit the size of extra unused buffer to MAX_EXTRA_ALLOCATION_SIZE
        if (need_size > data->buffer_size + MAX_EXTRA_ALLOCATION_SIZE)
            need_size = encoder_data_get_size(data) + size + MAX_EXTRA_ALLOCATION_SIZE;

        // make requested size a multiple of DEFAULT_BUFFER_SIZE
        // (in theory shoudl improve memory fragmentation in the OS)
        Py_ssize_t alloc_size = need_size + (DEFAULT_BUFFER_SIZE - need_size % DEFAULT_BUFFER_SIZE);

        if (! _encoder_buffer_grow(data, alloc_size)) return false;
    }
    // else: do nothing, already have enough space in currently allocated buffer

    return true;
}

static bool encoder_data_has_error (EncodedData * data)
{
    return data->buffer_size == -1;
}

static void encoder_data_set_error (EncodedData * data)
{
    data->buffer_size = -1;
}

static void encoder_data_append (EncodedData * data, const BUFFERTYPE* str, Py_ssize_t str_length)
{
    if (!encoder_data_reserve_space(data, str_length)) return;

    memcpy(data->buffer_free, str, str_length);

    data->buffer_free += str_length;
}

static void encoder_data_append_char (EncodedData * data, const BUFFERTYPE ch)
{
    if(!encoder_data_reserve_space(data, 1)) return;

    *data->buffer_free++ = ch;
}

static void encoder_data_append_ch_nocheck (EncodedData * data, const BUFFERTYPE ch)
{
    *data->buffer_free++ = ch;
}

static void encoder_data_place_ch_nocheck (EncodedData * data, const BUFFERTYPE ch, int offset)
{
    data->buffer_free[offset] = ch;
}

static void encoder_data_append_cstr (EncodedData * data, const char * cstr, Py_ssize_t max_size)
{
    if(!encoder_data_reserve_space(data, max_size)) return;

    char ch;
    Py_ssize_t i = 0;

    while ( (ch = cstr[i++]) )
    {
        encoder_data_append_ch_nocheck(data, ch);
    }
}
