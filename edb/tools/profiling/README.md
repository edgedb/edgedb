HOWTO profile EdgeDB code
=========================

This package provides tooling built around cProfile that lets us perform
quick time-based profiling.  Then we can aggregate the results into
regular pstats output as well as a zoomable and searchable SVG flame
graph.


## Running the profiler

The profiler is not sample based and supports multiprocessing so it
should provide a full view into what is going on.  That being said, it's
time-based and done in-process so it's bound to have some jitter.

Running it is as simple as this:

```py3
from edb.tools.profiling import profile

...

@profile()
def some_function_we_want_to_profile() -> None:
    ...
```

This will generate a series of `.prof` files in a temporary directory
(note: on macOS this is *not* /tmp/ by default).  Only invocations of
this function will be profiled.  The profiler reuses the same file name
per process to avoid generating hundreds of thousands of files if the
profiled function is called many times.


## Aggregating data

After the program is profiled, we can aggregate the results by running:

```
$ python -m edb.tools.profiling
```

This should produce a few files in the EdgeDB project directory by
default:

* `profile_analysis.prof` which is an aggregation of possibly hundreds
  of smaller `.prof` files produced by subprocesses of the profiled
  program.  You can use this as input for further aggregation or
  analysis.  This is the marshalled format accepted directly by the
  `pstats` module.

* `profile_analysis.pstats` is a text file with "Top calls" sorted and
  formatted by the `pstats.Stats` class.

* `profile_analysis.svg` is a zoomable and searchable SVG flame graph.

* `profile_analysis.singledispatch` is a single dispatch sidecar file,
  explained further down in this document.

## Customizing the profiler

The `profile()` decorator accepts a number of arguments.  I don't want
to risk this file going out of date, so your best course of action is
looking at the decorator's docstring.

Similarly, `python -m edb.tools.profiling --help` lists all possible
options of the aggregator.

Here let me just show you an example customization:

```py3
@profile(dir='/tmp/', prefix='specific_function_')
def specific_function() -> None:
    ...

@profile(dir='/tmp/', prefix='another_function_')
def another_function() -> None:
    ...
```

To aggregate this data, use for example:

```
$ python -m edb.tools.profiling --prefix=another_function_ --out=/Users/ambv/Desktop/another_function.pstats /tmp/
```

As you see, we filtered by the prefix, put the output on the desktop
with a custom file name, and we're looking for the input files in /tmp/
which is where they were configured to be saved by the decorators.

## SVG flame graphs

Flame graphs are a nice way to visualize call patterns in a program.
In the graphs generated here, the upper one is the traditional call
stack graph where the lower bars are functions calling other functions
in the upper bars.  The width demonstrates relative time spent in each
function.  The lower graph is a reverse flame graph, showing functions
that are called very often and their callers.

The generated flame graph can be big by default if a large part of the
program is analyzed.  If the resulting file is over 10MB big and your
Web browser doesn't like it, adjust the threshold and/or width of the
generated file, like so:

```
$ python -m edb.tools.profiling --threshold=0.001 --width=1280
```

## Singledispatch and the fog of war

Single-dispatch generic functions available in Python with the use of
the `functools.singledispatch` decorator are handy but have one downside
when it comes to profiling.

Let's say we have a decorated single-dispatch generic function S.  That
function has a few implementations registered for different types, let's
say F1, F2, and F3.

Now, if functions A, B, C all call function S, we would expect to see
those calls nicely separated and showing us which concrete
registered implementations were chosen when function A was calling S,
or when function C was calling S.  Sadly, this is not what's happening.

The generic function S is replaced with a `wrapper` by the
`@singledispatch` decorator.  This wrapper dispatches calls to
registered concrete implementations based on the type of the first
argument in the call.  Even though the closures are different for every
decorated single-dispatch generic function, the code for the `wrapper`
is shared between every one.  In consequence, in the SVG flame graph
you'll see functions A, B, C low in the graph, then a thick bar called
`wrapper` and concrete implementations F1, F2, and F3 above.  But we
lost knowledge about whether A is responsible for 90% of F3 calls or
whether B is the biggest user of S.

To fix this problem, do this *very early in your program*:

```py3
from edb.tools.profiling import tracing_singledispatch
tracing_singledispatch.patch_functools()
```

You have to do it very early because single-dispatch generic functions
are configured and registered at import time.  Patch before any
singledispatch import and usage.

Anyway, now that singledispatch is patched, the `@profile` decorator
will discover this and will save sidecar `.singledispatch` files that
store which function called the single-dispatch wrapper and which
implementation ended up being used.  It also stores the call count to
enable proportional aggregation later.

When `.singledispatch` sidecar files were present, aggregating the
profiler data will generate an SVG flame graph that replaces the
`wrapper` fog of war with concrete relationships between A, B, C
and F1, F2, and F3.

Pro tip: to profile tests, make sure to cover at least the following
entry points with the functools patch:

* edb/cli/__init__.py (for the `edgedb` CLI command)

* edb/server/main.py (for the `edgedb-server` CLI command)

* edb/tools/edb.py (for the `edb` CLI command)

* edb/server/procpool/worker.py (for worker subprocesses like the EdgeQL
  compiler)

## Profiling caveats

The decorators are not fully reentrant: if more than one function is
profiled at the same time and they share some call paths, the resulting
data will be corrupt (i.e. each result will be missing parts of the
information).

This is a wall clock time-based profiler, it's not well suited for
benchmarking.

This profiler does not store call history so the flame graph does not
represent a timeline.

This profiler is using `atexit` to ensure all data is written to disk
before the program is done.  However, terminating a process with
a signal may circumvent `atexit` from executing.  Similarly, an
exception in an `atexit` handler called sooner may prevent our handler
from running.  If data is visibly incomplete, use `save_every_n_calls=1`
(which is the default).

This profiler will not be able to trace calls that don't trigger
`sys.settrace()` callbacks.
