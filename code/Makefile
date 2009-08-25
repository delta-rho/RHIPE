all: R java
VER=0.44
.PHONY: build R java

R: build transform
	@echo "Installing R package"
	R CMD INSTALL build/rhipe

java: build 
	@echo "Building rhipe.jar"
	ant -f build/build.xml clean
	ant -f build/build.xml 
	cp build/java/rhipe.jar .

build: 
	@echo "Making build folder"
	mkdir -p build
	cp build.xml build/
	rsync -a rhipe build/
	rsync -a java build/


clean:
	@echo "Cleaning"
	if test -d build; then rm -rf build ; fi
	if test -e rhipe.jar ; then rm rhipe.jar ; fi

transform:
	@echo "Transforming Files"
	sed   's/_VER_/${VER}/' build.xml > build/build.xml ; 
	sed   's/_VER_/${VER}/' rhipe/R/zzz.R > build/rhipe/R/zzz.R ; 
	sed   's/_VER_/${VER}/' rhipe/DESCRIPTION> build/rhipe/DESCRIPTION ; 

