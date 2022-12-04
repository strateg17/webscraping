# Build from Azure Container Base image with preinstalled SQL Server ODBC Driver 17 and Ubuntu 18.04
FROM mcr.microsoft.com/azureml/openmpi3.1.2-ubuntu18.04

# Update apt-get and install necessary packages
RUN apt-get update && apt-get install -y unixodbc \
	unixodbc-dev \
	freetds-dev \ 
	tdsodbc software-properties-common && \
    rm -rf /var/lib/apt/lists/*


# Update conda install repository and install allnecessary packages
RUN conda install -c r -y pip=20.1.1 openssl=1.1.1c && conda clean -ay && \
	pip install --no-cache-dir azureml-defaults==1.33.0 azureml-pipeline==1.33.0 azure-storage-blob==2.1.0


# Upgrade pip and install requirements.
COPY /requirements.txt /requirements.txt
RUN pip install --upgrade pip && pip install -r /requirements.txt


# Tor configuring
RUN add-apt-repository ppa:micahflee/ppa && apt update && apt install -y torbrowser-launcher
EXPOSE 9050 9051
RUN echo "ControlPort 9051" >> /etc/tor/torrc
RUN echo "SocksPort 0.0.0.0:9050" >> /etc/tor/torrc
RUN echo "HashedControlPassword $(tor --quiet --hash-password mypassword)" >> /etc/tor/torrc


# clean the install.
RUN apt-get -y clean


# Set `tor` as the entrypoint for the image
ENTRYPOINT ["tor"]


# Set the default container command
# This can be overridden later when running a container
CMD ["-f", "/etc/tor/torrc"]
