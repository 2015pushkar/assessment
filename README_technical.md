# Clinical Data ETL Pipeline

## Overview

A microservices-based data pipeline for processing clinical trial data. The system consists of three Docker services: TypeScript API, Python ETL processor, and PostgreSQL database.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   API Service   │    │   ETL Service   │    │   PostgreSQL    │
│  (TypeScript)   │◄──►│    (Python)     │◄──►│   Database      │
│   Port: 3000    │    │   Port: 8000    │    │   Port: 5432    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Services

### API Service (TypeScript/Node.js)
- ETL job management endpoints
- Data querying capabilities
- Status tracking functionality
- Port: 3000

### ETL Service (Python/FastAPI)
- CSV data processing
- Data validation and transformation
- Database integration
- Port: 8000

### PostgreSQL Database
- Clinical data storage
- ETL job tracking
- Port: 5432

## Prerequisites

- Docker and Docker Compose
- Git (for cloning the repository)
- curl (for testing API endpoints)

## Quick Start

```bash
# Start all services
docker compose up --build

# Verify services are running
curl http://localhost:3000/health  # API Service
curl http://localhost:8000/health  # ETL Service
```

## Development Setup

### API Service Development
```bash
cd api-service
npm install
npm run dev  # Runs on port 3000
```

### ETL Service Development
```bash
cd etl-service
pip install -r requirements.txt
uvicorn src.main:app --reload  # Runs on port 8000
```

## API Documentation

### Submit ETL Job
```bash
curl -X POST http://localhost:3000/api/etl/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "sample_study001.csv",
    "studyId": "STUDY001"
  }'
```

### Query Data
```bash
curl "http://localhost:3000/api/data?studyId=STUDY001&measurementType=glucose"
```

### Get Job Status
```bash
curl http://localhost:3000/api/etl/jobs/{job-id}/status
```

## Sample Data

Sample CSV files are provided in the `data/` directory:
- `sample_study001.csv` - Glucose, cholesterol, weight, height measurements
- `sample_study002.csv` - Blood pressure, heart rate measurements

### Data Format
```csv
study_id,participant_id,measurement_type,value,unit,timestamp,site_id,quality_score
STUDY001,P001,glucose,95.5,mg/dL,2024-01-15T09:30:00Z,SITE_A,0.98
```

## Testing

```bash
# API Service
cd api-service
npm test

# ETL Service
cd etl-service
python -m pytest
```

## Database Access

```bash
# Connect to PostgreSQL
docker exec -it clinical-api-etl-postgres-1 psql -U user -d clinical_data

# View schema
\i /docker-entrypoint-initdb.d/schema.sql
\dt  # List tables
```

## Project Structure

```
clinical-api-etl/
├── api-service/           # TypeScript/Node.js API
│   ├── src/
│   │   ├── controllers/   # Request handlers
│   │   ├── routes/        # API routes
│   │   ├── services/      # Business logic
│   │   └── middleware/    # Express middleware
│   ├── package.json
│   └── Dockerfile
├── etl-service/           # Python/FastAPI ETL
│   ├── src/
│   │   └── main.py       # FastAPI application
│   ├── requirements.txt
│   └── Dockerfile
├── database/
│   └── schema.sql        # Database schema
├── data/                 # Sample CSV files
└── docker-compose.yml    # Service orchestration
```

## Troubleshooting

### Common Issues

#### Services Won't Start
```bash
# Check if ports are already in use
netstat -tulpn | grep -E ':(3000|8000|5432)'

# Kill processes using required ports
sudo lsof -ti:3000 | xargs kill -9
sudo lsof -ti:8000 | xargs kill -9
sudo lsof -ti:5432 | xargs kill -9
```

#### Docker Issues
```bash
# Clean up Docker resources
docker compose down -v
docker system prune -f

# Rebuild with no cache
docker compose build --no-cache
docker compose up
```

#### Database Connection Issues
```bash
# Check if PostgreSQL is running
docker compose ps

# View database logs
docker compose logs postgres

# Reset database
docker compose down -v
docker compose up postgres
```

#### Permission Issues (Linux/macOS)
```bash
# Fix file permissions
sudo chown -R $USER:$USER .
chmod -R 755 .
```

### Platform-Specific Setup

#### Windows (WSL2 Recommended)
```powershell
# Install WSL2 and Docker Desktop
wsl --install
# Then follow Linux instructions in WSL2
```

#### macOS
```bash
# Install Docker Desktop for Mac
brew install --cask docker
# Start Docker Desktop and follow Linux instructions
```

#### Linux
```bash
# Install Docker and Docker Compose
sudo apt update
sudo apt install docker.io docker-compose
sudo usermod -aG docker $USER
# Logout and login again
```

### Service Health Checks

#### API Service (Port 3000)
```bash
# Health check
curl http://localhost:3000/health

# Expected response
{"status": "ok", "timestamp": "..."}
```

#### ETL Service (Port 8000)
```bash
# Health check
curl http://localhost:8000/health

# API documentation
curl http://localhost:8000/docs
```

#### Database (Port 5432)
```bash
# Connection test
docker exec -it clinical-api-etl-postgres-1 psql -U user -d clinical_data -c "SELECT version();"
```

### Environment Variables

Default configuration (can be overridden):
```env
# Database
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=clinical_data

# API Service
NODE_ENV=development
PORT=3000

# ETL Service
PYTHONPATH=/app/src
```

### Development Tips

1. **Hot Reload**: Both services support hot reload in development mode
2. **Logs**: Use `docker compose logs -f [service-name]` to follow logs
3. **Database Reset**: Use `docker compose down -v` to reset database
4. **Port Conflicts**: Change ports in `docker-compose.yml` if needed
5. **Memory Issues**: Increase Docker memory limit in Docker Desktop settings

### Getting Help

If you encounter issues:
1. Check service logs: `docker compose logs [service-name]`
2. Verify all services are running: `docker compose ps`
3. Ensure ports 3000, 8000, and 5432 are available
4. Try rebuilding: `docker compose build --no-cache`
