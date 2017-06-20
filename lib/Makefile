
all: excelformat

excelformat:
	@echo "Building libexcelformat.a"
	cd ExcelFormat_src && make
	cd ..
	cp ExcelFormat_src/excelformatlib.h ExcelFormat_src/libexcelformat.a ./


clean:
	rm -f ./excelformatlib.h ./libexcelformat.a
	cd ExcelFormat_src && make clean
