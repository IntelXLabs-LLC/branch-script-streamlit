# Use an official Python runtime as a parent image
FROM python:3.10‑slim

# set working directory
WORKDIR /usr/src/app

# copy & install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# copy the rest of your app
COPY . .

# expose the port Streamlit uses
EXPOSE 8501

# run the app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]