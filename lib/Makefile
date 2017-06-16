
all: excelformat

excelformat:
	@echo "Building libexcelformat.so"
	cd ExcelFormat_src && make
	cd ..
	cp ExcelFormat_src/excelformatlib.h ExcelFormat_src/libexcelformat.so ./


clean:
	rm -f ./excelformatlib.h ./libexcelformat.so
	cd ExcelFormat_src && make clean
