FROM python:3.13

WORKDIR /pythonfile

COPY . /pythonfile/


CMD [ "python", "addition.py" ]