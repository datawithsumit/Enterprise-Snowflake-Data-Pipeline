# 1. Use a lightweight version of Python as the base
FROM python:3.9-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy the requirements file to the container
COPY requirements.txt .

# 4. Install the required libraries (Pandas, Snowflake Connector)
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of your code (main.py)
COPY main.py .

# 6. The command to run when the container starts
CMD ["python", "main.py"]