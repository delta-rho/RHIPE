#!/bin/bash

echo "deb http://cran.rstudio.com/bin/linux/ubuntu lucid/" | sudo tee -a /etc/apt/sources.list

sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys E084DAB9

sudo -E apt-get --yes --force-yes update
sudo -E apt-get --yes install vim
#### install R
sudo -E apt-get --yes --force-yes install r-base-dev

#### install rstudio-server
## http://www.rstudio.com/ide/download/server
sudo -E apt-get --yes --force-yes install gdebi-core
sudo -E apt-get --yes --force-yes install libapparmor1
wget http://download2.rstudio.org/rstudio-server-0.98.501-amd64.deb
sudo gdebi --n rstudio-server-0.98.501-amd64.deb
## only want to start manually (as it will be installed in each node)
echo "manual" | sudo tee /etc/init/rstudio-server.override

sudo useradd rstudio
echo "rstudio:rstudio" | sudo chpasswd
sudo mkdir /home/rstudio
sudo chmod -R 0777 /home/rstudio

#### git
sudo -E apt-get --yes --force-yes install git-core

#### install shiny-server
sudo apt-get --yes install cmake
sudo su - -c "R -e \"install.packages('shiny', repos='http://cran.rstudio.com/')\""

# Clone the repository from GitHub
git clone https://github.com/rstudio/shiny-server.git

# Get into a temporary directory in which we'll build the project
cd shiny-server
mkdir tmp
cd tmp

# Add the bin directory to the path so we can reference node
DIR=`pwd`
PATH=$PATH:$DIR/../bin/

# See the "Python" section below if your default python version is not 2.6 or 2.7. 
PYTHON=`which python`

# Check the version of Python. If it's not 2.6.x or 2.7.x, see the Python section below.
$PYTHON --version

# Use cmake to prepare the make step. Modify the "--DCMAKE_INSTALL_PREFIX"
# if you wish the install the software at a different location.
cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DPYTHON="$PYTHON" ../
# Get an error here? Check the "How do I set the cmake Python version?" question below

# Recompile the npm modules included in the project
make
mkdir ../build
(cd .. && bin/npm --python="$PYTHON" rebuild)
# Need to rebuild our gyp bindings since 'npm rebuild' won't run gyp for us.
(cd .. && ext/node/lib/node_modules/npm/node_modules/node-gyp/bin/node-gyp.js --python="$PYTHON" rebuild)

# Install the software at the predefined location
sudo make install
##############
##############
##############

## http://www.rstudio.com/shiny/server/install-opensource
sudo su - -c "R -e \"install.packages('shiny', repos='http://cran.rstudio.com/')\""
wget http://download3.rstudio.org/ubuntu-12.04/x86_64/shiny-server-1.0.0.42-amd64.deb
sudo gdebi --n shiny-server-1.0.0.42-amd64.deb
## only want to start manually (as it will be installed in each node)
echo "manual" | sudo tee /etc/init/shiny-server.override

## move examples over to server directory
sudo mkdir /srv/shiny-server/examples
sudo cp -R /usr/local/lib/R/site-library/shiny/examples/* /srv/shiny-server/examples
sudo chown -R shiny:shiny /srv/shiny-server/examples

#### install datadr / trelliscope
## system dependencies
sudo -E apt-get --yes --force-yes install libcurl4-openssl-dev
sudo -E apt-get --yes --force-yes install libxml2-dev
sudo -E apt-get --yes --force-yes install mongodb
sudo -E apt-get --yes --force-yes install openjdk-6-jdk
#wget --no-check-certificate https://github.com/aglover/ubuntu-equip/raw/master/equip_java7_64.sh && bash equip_java7_64.sh
sudo su - -c "R -e \"install.packages('rJava', repos='http://cran.rstudio.com/')\""

#### install Rhipe components
wget http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz
tar -xzf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0
./configure
sudo make -j4
sudo make install

sudo ldconfig
cd ..

## devtools
sudo su - -c "R -e \"install.packages('devtools', repos='http://cran.rstudio.com/')\""
## datadr
sudo su - -c "R -e \"options(repos = 'http://cran.rstudio.com/'); library(devtools); install_github('datadr', 'hafen')\""
## trelliscope
sudo su - -c "R -e \"options(repos = 'http://cran.rstudio.com/'); library(devtools); install_github('trelliscope', 'hafen')\""


#sudo rm -rf ~/*


#### java environment variables from javareconf
export JAVA_CPPFLAGS=-I/usr/lib/jvm/java-6-openjdk-amd64/jre/../include
export JAVAC=/usr/bin/javac
export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64/jre
export JAVAH=/usr/bin/javah
export JAVA_LD_LIBRARY_PATH=/usr/lib/jvm/java-6-openjdk-amd64/jre/lib/amd64/server
export JAVA_LIBS=-L/usr/lib/jvm/java-6-openjdk-amd64/jre/lib/amd64/server
export JAVA=/usr/bin/java

### these are wrong for java 1.7
sudo echo 'export JAVA_CPPFLAGS=-I/usr/lib/jvm/java-6-openjdk-amd64/jre/../include' | sudo tee -a /etc/bash.bashrc
sudo echo 'export JAVAC=/usr/bin/javac' | sudo tee -a /etc/bash.bashrc
sudo echo 'export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64/jre' | sudo tee -a /etc/bash.bashrc
sudo echo 'export JAVAH=/usr/bin/javah' | sudo tee -a /etc/bash.bashrc
sudo echo 'export JAVA_LD_LIBRARY_PATH=/usr/lib/jvm/java-6-openjdk-amd64/jre/lib/amd64/server' | sudo tee -a /etc/bash.bashrc
sudo echo 'export JAVA_LIBS=-L/usr/lib/jvm/java-6-openjdk-amd64/jre/lib/amd64/server' | sudo tee -a /etc/bash.bashrc
sudo echo 'export JAVA=/usr/bin/java' | sudo tee -a /etc/bash.bashrc

sudo R CMD INSTALL Rhipe_0.75.0_cdh3.tar.gz
