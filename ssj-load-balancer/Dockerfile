FROM python:3.8.10-slim-buster

# Install python deps
COPY ./requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

# Copy over stuff
COPY ./ /monitor

WORKDIR /monitor

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/monitor/algorithms:${PYTHONPATH}"
CMD ["python", "./src/main.py", "-algo", "aaa", "-p", "99%"]