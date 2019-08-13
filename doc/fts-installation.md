# FTS installation and configuration

## Generate user certificate
Request a user certificate at https://ca.cern.ch

    openssl pkcs12 -in usercertificate.p12 -nocerts -nodes > userkey.pem
    openssl pkcs12 -in usercertificate.p12 -nokeys -nodes > usercert.pem
    chmod 0600 host*


## Generate host certificate
Request a host certificate for your server at https://ca.cern.ch

    scp root@pcna62daqdev:~/yourhostcertificate.p12 /etc/grid-security/
    cd /etc/grid-security/
    openssl pkcs12 -in yourhostcertificate.p12 -nocerts -nodes > hostkey.pem
    openssl pkcs12 -in yourhostcertificate.p12 -nokeys -nodes > hostcert.pem
    chmod 0600 host*


## Install the Storage element

    yum install -y m2crypto python-m2ext voms-client globus-gridftp-server edg-mkgridmap globus-gridftp-server-progs

### Enable ports
Open the following ports:
    - 2811/tcp port for the grid
    - 20000-21000/tcp Open ports for GLOBUS_TCP_PORT_RANGE

### Configure gridmap
Put those files:

    vim  /etc/edg-mkgridmap.conf
    vim  /etc/localgridmap.conf

Pull the certificate known by the VO and push loca mapping with the following command

    edg-mkgridmap


## Set up automatic gridmap polling with a cron
Set up a cron:

    crontab -e
    17 */2 * * * /usr/sbin/edg-mkgridmap --conf=/etc/edg-mkgridmap.conf --output=/etc/grid-security/grid-mapfile --safe --cache —quiet

### Connection issues
Please notice that edg-mkgridmap will fail if there are network restictions and the gridmap has to be copied over statically

    [how to set up a static gridmap: TODO]

## Start the storage element

    service globus-gridftp-server start

## Init the proxy
Generates the temporary proxy:

    voms-proxy-init --cert usercert.pem --key userkey.pem

Generates the temporary proxy with voms and hours of lifetime:

    voms-proxy-init --cert ./na62cdrdmcert.pem --key ./na62cdrdmkey.pem --voms na62.vo.gridpp.ac.uk -hours 36

## Set up automatic proxy renewal with cron

    crontab -e
    0 */4 * * * voms-proxy-init --cert /etc/robot-certificate/na62cdrcert.pem --key /etc/robot-certificate/na62cdrkey.pem -hours 24 -q

## Install FTS python client

    pip install fts3-rest-API gfal2-python gfal2-all

### FTS CLI commands examples
Get your certificate without the proxy

    fts-rest-whoami -s https://fts3-daq.cern.ch:8446 --cert=./usercert.pem --key=./userkey.pem -vv


Get your certificate with the proxy

    fts-rest-whoami -s https://fts3-daq.cern.ch:8446

### Transfer a file
Submit a transfer from castor to dpm

    fts-rest-transfer-submit -s https://fts3-daq.cern.ch:8446 srm://srm-public.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/na62/data/2017/pippo.txt https://dpmhead-rc.cern.ch/dpm/cern.ch/home/dteam/aalvarez/public/pippo.txt

Transfer from my host to Castor

    fts-rest-transfer-submit -s https://fts3-daq.cern.ch:8446 -o  gsiftp://na62merger5.cern.ch/storage/1.data  srm://srm-public.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/na62/data/2017/pippo.txt

Check the status

    fts-rest-transfer-status -s https://fts3-pilot.cern.ch:8446 04ba6736-1f87-11e7-8d47-02163e007a99A

### Monitoring

https://fts3-pilot.cern.ch:8449

https://monit.cern.ch


## Code init

    import fts3.rest.client.easy as fts
