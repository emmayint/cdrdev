# CDR

##Software installation
### Install python dependencies

	yum -y install python-pip

	pip install MySQL-python
	pip install sqlalchemy
	pip install alembic
	pip install inotify


## Create the database

	alembic upgrade head
	alembic upgrade head --sql
	alembic downgrade -1


## Files checksum
Fts use the xrdadler32 algorithm to checksum the files:

	[mboretto@na62merger1 cdr]$ xrdadler32  /merger/cdr/na62raw_1524108472-01-008511-0891.dat
	fbbf5cd5 /merger/cdr/na62raw_1524108472-01-008511-0891.dat


## Useful links
http://www.dangtrinh.com/2013/06/sqlalchemy-python-module-with-mysql.html  

http://docs.pylonsproject.org/projects/pyramid-blogr/en/latest/basic_models.html#what-are-the-implications-of-this
