

all:
	cd lib && make
	cd ..
	@echo "Building esgen"
	cd src && make -f Makefile.suite
	cd ..
	cp src/esgen src/tpcds.idx ./
	cp python/esgen.py ./


clean:
	rm -f ./esgen ./tpcds.idx ./esgen.py
	cd src && make -f Makefile.suite clean
	cd ..
	cd lib && make clean
	cd ..
