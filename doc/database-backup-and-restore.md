# Database backup and restore


## Backup

	mysqldump --single-transaction --routines --triggers -u -p  na62_bk > na62_bk_2018-05-15.sql


## Restore 

	mysql -uroot -p na62_bk < na62_bk_2017-10-24.sql




## Move to Castor

	xrdcp na62_bk_2017-10-24.sql root://castorpublic//castor/cern.ch/na62/data/2017/raw/run/na62_bk_2017-10-24.sql

