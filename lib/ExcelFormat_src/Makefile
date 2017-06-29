
CFLAGS = -O2
#CFLAGS = -m32 -g
#CFLAGS = -m64 -g


LIB = libexcelformat.so
LIB_S = libexcelformat.a


HEADERS = basicexcel.hpp excelformat.h
SRC = basicexcel.cpp excelformat.cpp
EXAMPLE = examples.cpp
LIB_SRC = excelformatlib.h excelformatlib.cpp
LIB_TEST = test_lib.cpp
LIB_S_OBJECT = basicexcel.o excelformat.o excelformatlib.o


all: excelformat $(LIB) $(LIB_S) lib_test

clean:
	rm -f excelformat test_lib $(LIB) $(LIB_S) *.xls

excelformat: $(SRC) $(HEADERS) $(EXAMPLE)
	g++ $(CFLAGS) -o $@ $(SRC) $(EXAMPLE)

$(LIB): $(SRC) $(HEADERS) $(LIB_SRC)
	g++ $(CFLAGS) $(SRC) $(HEADERS) $(LIB_SRC)  -o $(LIB) -fPIC -shared 
	
$(LIB_S): $(LIB_S_OBJECT)
	ar -rsv $(LIB_S) $(LIB_S_OBJECT)

lib_test: $(LIB)
	g++ $(CFLAGS) $(LIB_TEST) -o test_lib -L . -l excelformat
