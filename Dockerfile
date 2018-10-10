FROM python:3.7

EXPOSE 8000
WORKDIR /user/src/app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /user/src/app

CMD ["python", "wranging.py"]