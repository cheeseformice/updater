# Using official python runtime base image
FROM pypy:3.7

# Set the working directory
WORKDIR /usr/src/app

# Install our requirements.txt
COPY requirements.txt ./
RUN pypy3 -m pip install --no-cache-dir -r requirements.txt

# Copy our code from the current folder inside the container
COPY . .

# Define our command to be run when launching the container
CMD ["pypy3", "-u", "./src/start.py"]