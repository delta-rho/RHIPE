VER=0.5

.PHONY : doc code 

all: doc updateweb

updateweb: doc
	/Applications/Aquamacs.app/Contents/MacOS/Aquamacs  -l index2html.el
	mv index.html website/

code:
	sed  -i ""  "s/Version: [0-9]*\.*[0-9]*/Version: ${VER}/" code/R/DESCRIPTION 
	sed  -i ""  "s/version=\"[0-9]*\.*[0-9]*\"/version=\"${VER}\"/" code/R/R/zzz.R
	ant -f code/build.xml clean
	rm -rf code/R/a.out.dSYM/ code/R/config.log code/R/config.status code/R/src/*.o
	cd ..
	mkdir rhipe.${VER} 
	rsync -a code/R/ rhipe.${VER}
	tar czf rhipe.${VER}.tgz  rhipe.${VER}
	rm -rf rhipe.${VER}
	rsync rhipe.${VER}.tgz website/dn/
	cp rhipe.${VER}.tgz website/dn/rhipe.tgz
	rm -rf rhipe.${VER}.tgz

doc: 
	sed -i "" "/^[(version)|(release)]/ s/\"[0-9]*\.*[0-9]*\"/\"${VER}\"/" doc/conf.py
	make -C doc -f Makefile html latex
	make -C doc/build/latex -f Makefile all-pdf
	rsync -av doc/build/html/ website/doc/html/
	rsync -av doc/build/latex/rhipe.pdf website/doc/

clean:
	rm -rf doc/build



