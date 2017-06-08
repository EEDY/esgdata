# Makefile for the microjson project:

VERSION=1.3

CFLAGS = -O

# Add DEBUG_ENABLE for the tracing code
CFLAGS += -DDEBUG_ENABLE -g
# Add TIME_ENABLE to support RFC3339 time literals
CFLAGS += -DTIME_ENABLE

all: mjson.o test_microjson example1 example2 example3
	@test_microjson

mjson.o: mjson.c mjson.h

test_microjson: test_microjson.o mjson.o
	$(CC) $(CFLAGS) -o test_microjson test_microjson.o mjson.o

.SUFFIXES: .html .asc .3

# Requires asciidoc and xsltproc/docbook stylesheets.
.asc.html:
	asciidoc $*.asc
.asc.3:
	a2x --doctype manpage --format manpage $*.asc

# Regression test
check: test_microjson
	test_microjson

# Worked examples.  These are essentially subsets of the regresion test.
example1: example1.c mjson.c mjson.h
example2: example2.c mjson.c mjson.h
example3: example3.c mjson.c mjson.h

clean:
	rm -f mjson.o test_microjson.o test_microjson example[123]
	rm -f microjson.html mjson.html

SUPPRESSIONS = --suppress=unusedStructMember
SUPPRESSIONS += -U__UNUSED__
cppcheck:
	cppcheck -I. --template gcc --enable=all $() $(SUPPRESSIONS) *.[ch]

SOURCES = Makefile *.[ch]
DOCS = README COPYING NEWS control microjson.asc mjson.asc
ALL =  $(SOURCES) $(DOCS)
microjson-$(VERSION).tar.gz: $(ALL)
	tar --transform='s:^:microjson-$(VERSION)/:' --show-transformed-names -cvzf microjson-$(VERSION).tar.gz $(ALL)

dist: microjson-$(VERSION).tar.gz

release: microjson-$(VERSION).tar.gz microjson.html mjson.html
	shipper version=$(VERSION) | sh -e -x
