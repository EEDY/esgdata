

all:
	cd lib && make -f Makefile.lib
	cd ..
	@echo "Building esgen"
	cd src && make -f Makefile.suite
	cd ..
	cp src/esgen src/tpcds.idx ./


clean:
	rm -f ./esgen ./tpcds.idx
	cd src && make -f Makefile.suite clean
	cd ..
	cd lib && make -f Makefile.lib clean
	cd ..
