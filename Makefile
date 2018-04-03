

all:
	cd lib && make -j 8
	cd ..
	@echo "Building esgdata"
	cd src && make -j 8 -f Makefile.suite
	cd ..
	cp src/esgdata src/tpcds.idx ./
	cp python/esgdata.py ./
	mkdir esgdata-kit  esgdata-kit/python
	cp -r esgdata esgdata.py tpcds.idx nodes.conf dirs.conf convert_to_utf8.sh README.md example3.xls files  esgdata-kit/
	cp -r python/lib python/conf  esgdata-kit/python/
	tar czf esgdata-kit.rel.tgz esgdata-kit
	rm -rf esgdata-kit


clean:
	rm -f ./esgdata ./tpcds.idx ./esgdata.py ./esgdata-kit.rel.tgz
	cd src && make -f Makefile.suite clean
	cd ..
	cd lib && make clean
	cd ..
