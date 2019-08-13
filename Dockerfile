FROM  gitlab-registry.cern.ch/linuxsupport/cc7-base
LABEL maintainer="marco.boretto@cern.ch"


# Install prerequisites.
RUN yum install -y gcc python-devel mysql-devel python-pip pylint
RUN pip install mysql-python sqlalchemy alembic

