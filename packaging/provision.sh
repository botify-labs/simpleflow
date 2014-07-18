#!/usr/bin/bash

# Provisioning in addition to `saas_cdf` puppet module

# install ElasticSearch
# FIXME puppet module `saas_elasticsearch` is broken for the moment
sudo apt-get update
sudo apt-get install openjdk-7-jre-headless -y;
#install elasticsearch only if required
if ! sudo service elasticsearch status > /dev/null
then
	wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.0.1.deb;
	sudo dpkg -i elasticsearch-1.0.1.deb;
	rm elasticsearch-*.deb
else
	echo "Elastic search already installed"
fi
sudo service elasticsearch start;

# install levelDB related system packages
sudo apt-get install libsnappy1
sudo apt-get install libleveldb1
sudo apt-get install libleveldb-dev


# install xml libs
# FIXME this should be done by puppet module `saas_cdf`
sudo apt-get -y install libboost-python-dev;
sudo apt-get -y install libxml2-dev;
sudo apt-get -y install libxslt1-dev;


# install test utils
sudo apt-get -y install python-virtualenv
sudo apt-get -y install sloccount
