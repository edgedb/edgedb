##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import MetaJSParserTest_Base, jxfail, flags
from metamagic.utils.lang.javascript.parser.jsparser import UnknownToken, UnexpectedToken, \
    PP_UnexpectedToken, PP_MalformedToken


class TestJSParser_withPP(metaclass=MetaJSParserTest_Base):
# behaves

    @flags(ppsupport=True)
    @jxfail(UnknownToken, attrs={'line' : 1, 'col' : 1})
    def test_utils_lang_js_parser_pp_wrong1(self):
        '''#bogus define MAX 1'''

    @flags(ppsupport=True)
    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 11})
    def test_utils_lang_js_parser_pp_wrong2(self):
        '''bogus = 1 #define MAX 1'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_error1(self):
        '''#error '''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_error2(self):
        '''#error "Testing"'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_error3(self):
        '''#error this is a test message'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_error4(self):
        r'''#error this is a test message\
        print('very long message....');'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_error5(self):
        r'''#error this is a test message
        print('very long message....');'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_error6(self):
        '''
        #error '''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_error7(self):
        '''print('test');
        #error '''

    @jxfail(UnknownToken, attrs={'line' : 1, 'col' : 1})
    def test_utils_lang_js_parser_pp_error8(self):
        '''#error '''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_warning1(self):
        '''#warning this is a test message'''

    @jxfail(UnknownToken, attrs={'line' : 1, 'col' : 1})
    def test_utils_lang_js_parser_pp_warning2(self):
        '''#warning this is a test message'''

    @flags(ppsupport=True)
    @jxfail(PP_UnexpectedToken, attrs={'line' : 1, 'col' : 10})
    def test_utils_lang_js_parser_pp_include1(self):
        '''#include bad include'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_include2(self):
        '''#include "good_include"'''

    @flags(ppsupport=True)
    @jxfail(PP_MalformedToken, attrs={'line' : 1, 'col' : 9})
    def test_utils_lang_js_parser_pp_include3(self):
        '''#include'''

    @jxfail(UnknownToken)
    def test_utils_lang_js_parser_pp_include4(self):
        '''#include "good_include"'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_define1(self):
        '''#define MAX 1'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_define2(self):
        '''#define MAX (a, b) ((a)>(b)?(a):(b))'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_define3(self):
        r'''#define MAX (a, b) \
        ((a)>(b)?(a):(b))'''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_define4(self):
        r'''#define FOO (a) (a(b, a, b##a()))'''

    @jxfail(UnknownToken)
    def test_utils_lang_js_parser_pp_define5(self):
        '#define MAX 1'

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_if1(self):
        '''
        #if 1
        print('hello');
        #elif 2
        print('Hallo');
        #endif
        print('always');
        '''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_if2(self):
        '''
        #if 1
        print('hello');
        #define FOO 1
        #elif 2
        print('Hallo');
        #warning 'test'
        #endif
        print('always');
        '''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_if3(self):
        '''
        #if 1
            print('hello');
            #ifndef OS
                #define OS 'bad'
                #if 1

                #endif
            #endif
        #elif 2
            print('Hallo');
            #if 1
                print('nesting')
            #endif
        #endif

        print('always');
        '''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_if4(self):
        '''
        #if FLAG > 2
        print('hello');
        #elif FLAG == 1
        print('Hallo');
        #endif
        print('always');
        '''

    @jxfail(UnknownToken, attrs={'line' : 2, 'col' : 9})
    def test_utils_lang_js_parser_pp_if5(self):
        '''
        #if 1
        print('hello');
        #elif 2
        print('Hallo');
        #endif
        print('always');
        '''

    @flags(ppsupport=True)
    def test_utils_lang_js_parser_pp_ifdef1(self):
        '''
        #ifdef WINDOWS
        print('hello');
        #warning 'test'
        #endif
        #ifndef MAC
        print('Hallo');
        #define MAC 10
        #endif
        print('always');
        '''

    @jxfail(UnknownToken, attrs={'line' : 2, 'col' : 9})
    def test_utils_lang_js_parser_pp_ifdef2(self):
        '''
        #ifdef WINDOWS
        print('hello');
        #warning 'test'
        #endif
        '''

