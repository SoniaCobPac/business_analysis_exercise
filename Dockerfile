# Download the specified Python version
FROM python:3.8

# Copy all project (dockerfile required to be at this level for it) to a folder/app inside the container
COPY . /app

# Move working place to the folder/app
WORKDIR /app

# Execute commands inside of the Docker image
RUN cat requirements.txt | xargs -n 1 pip install --no-cache-dir

# Port
# EXPOSE 6060:6060

# Run the command on the container
CMD ["python3","./src/main.ipynb"]