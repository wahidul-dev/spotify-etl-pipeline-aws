# Use the official Python 3.9 image as the base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the Jupyter Notebook file from your local directory to the container's working directory
COPY spotify-etl-pipeline-aws-pandas.ipynb .

# Install Jupyter Notebook (assuming you want to run the notebook)
RUN pip install jupyter

# Expose the port for Jupyter Notebook (if you plan to use it)
EXPOSE 8888

# Command to run Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "spotify-etl-pipeline-aws-pandas.ipynb"]
