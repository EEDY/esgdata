
CFLAGS = -O2
#CFLAGS = -m32 -g
#CFLAGS = -m64 -g


LIB = libexcelformat.so


HEADERS = basicexcel.hpp excelformat.h
SRC = basicexcel.cpp excelformat.cpp
EXAMPLE = examples.cpp
LIB_SRC = excelformatlib.h excelformatlib.cpp
LIB_TEST = test_lib.cpp


all: excelformat $(LIB) lib_test

clean:
	rm -f excelformat test_lib $(LIB) *.xls

excelformat: $(SRC) $(HEADERS) $(EXAMPLE)
	g++ $(CFLAGS) -o $@ $(SRC) $(EXAMPLE)

$(LIB): $(SRC) $(HEADERS) $(LIB_SRC)
	g++ $(CFLAGS) $(SRC) $(HEADERS) $(LIB_SRC)  -o $(LIB) -fPIC -shared 

lib_test: $(LIB)
	g++ $(CFLAGS) $(LIB_TEST) -o test_lib -L . -l excelformat
