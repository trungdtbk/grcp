FROM faucet/python3

RUN apk --no-cache add gcc python3-dev musl-dev
COPY . /grcp-src
RUN pip3 install pip --upgrade
RUN cd /grcp-src && pip3 install -r requirements.txt
RUN cd /grcp-src && python3 setup.py install

VOLUME ["/etc/grcp", "/var/log/grcp"]

CMD ["grcp"]
