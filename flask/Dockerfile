FROM python:3.9

WORKDIR /app/pyfiles

COPY flask_app.py .

RUN pip install flask requests

ENV FLASK_APP=flask_app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000

EXPOSE 5000

CMD ["python", "flask_app.py"]

