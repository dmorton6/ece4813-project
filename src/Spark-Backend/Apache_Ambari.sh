#! /bin/sh 
sudo apt-get -q -y update 
sudo ufw disable
sudo apt-get -q -y install ntp
sudo service ntp start
sudo wget http://public-repo-1.hortonworks.com/ambari/ubuntu12/1.x/updates/1.7.0/ambari.list
sudo cp ambari.list /etc/apt/sources.list.d/
sudo apt-key adv --recv-keys --keyserver keyserver.ubuntu.com B9733A7A07513CAD
sudo apt-get -q -y update 
sudo apt-get -q -y install ambari-server
sudo apt-get -q -y install ant gcc g++ libkrb5-dev libmysqlclient-dev libssl-dev libsasl2-dev libsasl2-modules-gssapi-mit libsqlite3-dev libtidy-0.99-0 libxml2-dev libxslt-dev python-dev python-simplejson python-setuptools maven libldap2-dev python2.7-dev make python-pip
