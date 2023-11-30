# Use the official Python image as the base image
FROM python:3.9


# Set the working directory in the container
WORKDIR /app

# Copy the local code to the container
COPY . /app

# Install project dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Specify the command to run on container start
CMD ["python", "app.py"]

