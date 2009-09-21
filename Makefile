VER=0.5

.PHONY : doc code  sync

all: code doc index

sync:
	rsync -av website/ sguha@altair.stat.purdue.edu:/home/www/rhipe/

index: doc
	/Applications/Aquamacs\ Emacs.app/Contents/MacOS/Aquamacs\ Emacs  -l index2html.el
	# mv index.html website/


ec2:
	tar  cvfz website/dn/rhipeec2.tar.gz --exclude hadoop-ec2-env.sh   -C code  ec2

code:
	sed  -i ""  "s/Version: [0-9]*\.*[0-9]*/Version: ${VER}/" code/R/DESCRIPTION 
	sed  -i ""  "s/version=\"[0-9]*\.*[0-9]*\"/version=\"${VER}\"/" code/R/R/zzz.R

	if test -d build;then rm -rf build;else	mkdir build; fi
	rsync -a code/ build/
	ant -f build/build.xml clean
	ant -f build/build.xml
	ant -f build/build.xml clean	

	mv build/R build/Rhipe
	tar -czf Rhipe_${VER}.tar.gz  -C build/  Rhipe
	rsync  Rhipe_${VER}.tar.gz website/dn/
	rm -rf Rhipe_${VER}.tar.gz
	cp website/dn/Rhipe_${VER}.tar.gz website/dn/rhipe.tar.gz
	rm -rf build
doc: 
	sed -i "" "/^[(version)|(release)]/ s/\"[0-9]*\.*[0-9]*\"/\"${VER}\"/" doc/conf.py
	make -C doc -f Makefile html latex
	make -C doc/build/latex -f Makefile all-pdf
	rsync -av doc/build/html/ website/doc/html/
	rsync -av doc/build/latex/rhipe.pdf website/doc/

clean:
	rm -rf doc/build



