# B+Tree Library "Beet"


The library is tested on Linux and should work
on other systems as well. The tests use features
only available on Linux (e.g. high resolution timers)
and won't work on CygWin (and similar).

The code is licensed under the LGPL V3 with the exception
that static linking is explicitly allowed 
("static linking exception").

The library comes with a GNU Makefile.

- `make`
  builds the library and tests

- `make run`
  runs the tests

- `make all`
   builds the library, tests and tools

- `make debug`
  like `make all` but in debug mode

- `make tools`
   builds the library and tools

- `make lib`
  builds the library only

- `make clean`
  removes all binaries and object files

- `. setenv.sh`
  adds `./lib` to the `LD_LIBRARY_PATH`
