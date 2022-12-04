FROM python:3.10.4-alpine
USER root

ARG BOT_NAME="kithub-sys"

ENV LC_CTYPE='C.UTF-8'
ENV TZ='Asia/Tokyo'
ENV DEBIAN_FRONTEND=noninteractive

RUN set -x && \
    apk add --no-cache build-base nano git tzdata ncdu && \
    cp /usr/share/zoneinfo/Asia/Tokyo /etc/localtime

ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8
ENV TZ JST-9
ENV TERM xterm

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN python -m pip install --upgrade py-cord
RUN python -m pip install --upgrade mysql-connector-python==8.0.29

RUN echo "Hello, ${BOT_NAME} ready!"

CMD ["/opt/bot.sh"]