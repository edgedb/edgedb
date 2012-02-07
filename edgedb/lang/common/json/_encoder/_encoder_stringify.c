/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
*/

#include "_encoder_stringify.h"

/*
 * See http://stackoverflow.com/questions/4351371/c-performance-challenge-integer-to-stdstring-conversion
 *
 * This implementation is based on http://ideone.com/0uhhX
 *
 */
static void longlong_to_string (long long n, EncodedData * encodedData)
{
    // longest possible integer is -JAVASCRIPT_MAXINT which is 17 bytes long
    encoder_data_reserve_space(encodedData, 18);

    if (n == 0)
    {
        encoder_data_append_ch_nocheck(encodedData, '0');
        return;
    }

    int sign = -(n<0);

    if (sign) encoder_data_append_ch_nocheck(encodedData, '-');

    unsigned long long val = (n^sign)-sign;

    // JAVASCRIPT_MAXINT == 9007199254740992
    int size;
    if (val >= 10000)
    {
        if (val >= 10000000)
        {
            if (val >= 1000000000)
            {
                if (val >= 1000000000000)
                {
                    if (val >= 1000000000000000)
                        size = 16;
                    else if (val >= 100000000000000)
                        size = 15;
                    else if (val >= 10000000000000)
                        size = 14;
                    else
                        size = 13;
                }
                else
                {
                    if (val >= 100000000000)
                        size = 12;
                    else if (val >= 10000000000)
                        size = 11;
                    else
                        size = 10;
                }
            }
            else if (val >= 100000000)
                size = 9;
            else
                size = 8;
        }
        else
        {
            if (val >= 1000000)
                size = 7;
            else if (val >= 100000)
                size = 6;
            else
                size = 5;
        }
    }
    else
    {
        if (val >= 100)
        {
            if (val >= 1000)
                size = 4;
            else
                size = 3;
        }
        else
        {
            if (val >= 10)
            {
                encoder_data_append_ch_nocheck(encodedData, '0' + val/10);
                encoder_data_append_ch_nocheck(encodedData, '0' + val % 10);
                return;
            }
            else
            {
                encoder_data_append_ch_nocheck(encodedData, '0' + val);
                return;
            }
        }
    }

    int offset = size-1;
    while (val >= 10)
    {
        encoder_data_place_ch_nocheck(encodedData, '0' + (val%10), offset--);
        val /= 10;
    }
    encoder_data_place_ch_nocheck(encodedData, '0' + val, offset);

    encodedData->buffer_free += size;
}


static void datevalue_to_string (unsigned int n, EncodedData * encodedData, int fill_to_size)
{
    // longest supported number is 999999 (to support 100000 microseconds)
    encoder_data_reserve_space(encodedData, 6);

    int size;
    if (n >= 100)
    {
        if (n >= 10000)
        {
            if (n >= 100000)
                size = 6;
            else
                size = 5;
        }
        else
        {
            if (n >= 1000)
                size = 4;
            else
                size = 3;
        }
    }
    else
    {
        if (n >= 10)
            size = 2;
        else
            size = 1;
    }

    int i;
    for (i = 0; i < fill_to_size - size; i++)
        encoder_data_append_ch_nocheck(encodedData, '0');

    if (n == 0)
    {
        encoder_data_append_ch_nocheck(encodedData, '0');
        return;
    }

    int offset = size-1;
    while (n > 0)
    {
        encoder_data_place_ch_nocheck(encodedData, '0' + (n%10), offset--);
        n /= 10;
    }
    encodedData->buffer_free += size;
}
