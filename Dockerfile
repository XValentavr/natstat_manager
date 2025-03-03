FROM python:3.12

RUN apt update
RUN apt install -y sudo
RUN apt-get install -y unixodbc-dev
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN sudo apt-get update
RUN sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Copy the rest of the project and set the command to launch the service.
COPY . .
ENV ENV='PRODUCTION'
ENV PYTHONPATH=.
CMD python3 main.py
