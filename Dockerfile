ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

WORKDIR /app

COPY --from=py3 / /
COPY . /app

RUN pip install -r requirements.txt

#ENTRYPOINT ["python"]