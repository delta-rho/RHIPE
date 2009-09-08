VER=0.5

.PHONY : doc code 

all: doc updateweb

updateweb: doc
	/Applications/Aquamacs.app/Contents/MacOS/Aquamacs  -l index2html.el
	mv index.html website/

code:
	sed  -i ""  "s/Version: [0-9]*\.*[0-9]*/Version: ${VER}/" code/R/DESCRIPTION 
	sed  -i ""  "s/version=\"[0-9]*\.*[0-9]*\"/version=\"${VER}\"/" code/R/R/zzz.R

	if test -d build;then rm -rf build;else	mkdir build; fi
	rsync -a code/ build/
	ant -f build/build.xml clean
	ant -f build/build.xml
	ant -f build/build.xml clean	

	mv build/R build/rhipe.${VER}
	tar -czf rhipe.${VER}.tar.gz  -C build/  rhipe.${VER}
	rsync  rhipe.${VER}.tar.gz website/dn/
	rm -rf rhipe.${VER}.tar.gz
	cp website/dn/rhipe.${VER}.tar.gz website/dn/rhipe.tar.gz
	rm -rf build
doc: 
	sed -i "" "/^[(version)|(release)]/ s/\"[0-9]*\.*[0-9]*\"/\"${VER}\"/" doc/conf.py
	make -C doc -f Makefile html latex
	make -C doc/build/latex -f Makefile all-pdf
	rsync -av doc/build/html/ website/doc/html/
	rsync -av doc/build/latex/rhipe.pdf website/doc/

clean:
	rm -rf doc/build



