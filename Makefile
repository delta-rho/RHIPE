VER=0.65.4
#
.PHONY : doc code  sync

all: code doc index

sync: 
	cp a.css website/
	rsync -av website/ sguha@odds.stat.purdue.edu:~/.www/rhipe/
	ssh sguha@odds.stat.purdue.edu 'fixwww'
index: doc
	/Applications/Aquamacs.app/Contents/MacOS/Aquamacs  -nw -l index2html.el
	cp  index.html website/


ec2:
	rsync -av code/hadoop-ec2-init-remote.sh website/dn/

code:
	# sed  -i ""  "s/Version: [0-9]*\.*[0-9]*/Version: ${VER}/" code/R/DESCRIPTION 
	# sed  -i ""  "s/vvvv <- \"[0-9]*\.*[0-9]*\"/vvvv <- \"${VER}\"/" code/R/R/zzz.R

	if test -d build;then rm -rf build;else	mkdir build; fi
	rsync -a code/ build/
	ant -f build/build.xml clean
	ant -f build/build.xml
	ant -f build/build.xml clean	

	cd
# 	mv build/R build/Rhipe
	# tar -czf Rhipe_${VER}.tar.gz  -C build/  Rhipe
	R CMD BUILD  build/R
	mv  Rhipe_${VER}.tar.gz website/dn/
	cp website/dn/Rhipe_${VER}.tar.gz website/dn/rhipe.tar.gz
	rm -rf build
doc: 
	sed -i "" "/^[(version)|(release)]/ s/\"[0-9]*\.*[0-9]*\"/\"${VER}\"/" doc/conf.py
	make -C doc -f Makefile html latex
	make -C doc/_build/latex -f Makefile all-pdf
	rsync -av doc/_build/html/ website/doc/html/
	rsync -av doc/_build/latex/RHIPE.pdf website/doc/
clean:
	rm -rf doc/build



