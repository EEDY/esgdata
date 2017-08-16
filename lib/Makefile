
all: excelformat

excelformat:
	@echo "Building libexcelformat.a"
	cd ExcelFormat_src && make -j 8
	cd ..
	cp ExcelFormat_src/excelformatlib.h ExcelFormat_src/libexcelformat.a ./


clean:
	rm -f ./excelformatlib.h ./libexcelformat.a
	cd ExcelFormat_src && make clean
