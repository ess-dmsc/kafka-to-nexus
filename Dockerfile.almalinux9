FROM almalinux:9

RUN groupadd -r app && useradd -r -g app app

WORKDIR /opt/kafka-to-nexus

COPY dist/kafka-to-nexus .

RUN chown -R app:app /opt/kafka-to-nexus \
 && chmod -R a+rX /opt/kafka-to-nexus

ENV PATH="/opt/kafka-to-nexus/bin:${PATH}"

USER app
