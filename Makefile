

all:
	cd lib && make -j 8
	cd ..
	@echo "Building esgdata"
	cd src && make -j 8 -f Makefile.suite
	cd ..
	cp src/esgdata src/tpcds.idx ./
	cp python/esgdata.py ./


clean:
	rm -f ./esgdata ./tpcds.idx ./esgdata.py
	cd src && make -f Makefile.suite clean
	cd ..
	cd lib && make clean
	cd ..
