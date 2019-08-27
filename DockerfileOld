FROM maven:3.6.1-jdk-11
MAINTAINER BUI HUY HOANG <hoang.bui3@tiki.vn>

ENV APP_HOME /src

WORKDIR $APP_HOME

RUN wget https://bootstrap.pypa.io/get-pip.py && \
	python get-pip.py && \
	rm -rf get-pip.py

RUN pip install --upgrade --no-cache-dir pip \
    && pip install supervisor


COPY ./docker/supervisord/supervisord.conf /etc/supervisord.conf
ADD ./docker/supervisord/supervisor.d/ /etc/supervisor.d/

ADD . $APP_HOME

RUN mvn install

CMD ["supervisord", "-c", "/etc/supervisord.conf", "-n"]
