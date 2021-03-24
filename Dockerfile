# Using official python runtime base image
FROM python:3.7-slim

# Set the working directory
WORKDIR /src

# Install our requirements.txt
COPY requirements.txt /src/requirements.txt
RUN pip install -r requirements.txt

# Copy our code from the current folder to /src inside the container
COPY . .

# Define our command to be run when launching the container
CMD ["python", "-u", "src/run.py"]