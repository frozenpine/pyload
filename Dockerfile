FROM python:3.7.4-alpine3.10
COPY . /pyload
ENV TZ=Asia/Shanghai PYTHONIOENCODING=utf8
RUN apk --no-cache add --virtual=.build-dep build-base && apk --no-cache add tzdata bash gawk sed grep libzmq openssl-dev libffi-dev&& \
    cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    cd /tmp && pip install --no-cache-dir -r /pyload/requirements.txt
WORKDIR /pyload
ENTRYPOINT ["python"]
CMD ["-V"]