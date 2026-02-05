# Kylas CRM MCP Server - Lead Only
# MCP Registry: set io.modelcontextprotocol.server.name to your server name (e.g. io.github.USERNAME/kylas-crm)
FROM python:3.11-slim
LABEL io.modelcontextprotocol.server.name="io.github.akshaykylas94/kylas-crm"

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application
COPY main.py .

# MCP servers typically run in stdio mode
ENV PYTHONUNBUFFERED=1

# Run the MCP server (stdio transport)
CMD ["python", "-u", "main.py"]
