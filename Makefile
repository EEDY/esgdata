


all:
	@echo "Building esgen"
	cd src && make -f Makefile.suite
	cd ..
	cp src/esgen src/tpcds.idx ./


clean:
	rm ./esgen ./tpcds.idx
	cd src && make -f Makefile.suite clean
