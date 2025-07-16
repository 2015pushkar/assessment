# Assessment - Clinical Data ETL Pipeline

## Project Overview
Complete a partially-built microservices data pipeline for processing clinical trial data.

## Time Expectation
**4-6 hours** to complete all three tasks.

**We encourage you to use AI assistants** such as ChatGPT, Claude, GitHub Copilot, etc. You are welcome to leverage AI tools throughout your development process to help solve problems, implement features, and optimize your workflow. Demonstrating effective use of AI can be a valuable part of this assessment. We're evaluating your problem-solving approach and implementation quality, not your ability to memorize syntax.

## Assessment Delivery

This assessment is provided as a **zip file** containing a partially-implemented microservices project. You will:

1. **Extract** the project files
2. **Push** to your public GitHub repository  
3. **Implement** the required features with regular commits
4. **Share** your public repository URL for review with [george.mitra@regeneron.com](mailto:george.mitra@regeneron.com)

## What We'll Review
- **Problem-solving approach**: How you break down and tackle complex challenges
- **Commit history**: Regular commits showing development progress and thought process
- **Code quality**: Architecture decisions, clean implementation, and best practices
- **Feature completion**: All three required tasks implemented and working correctly
- **Error handling**: Edge cases and graceful error management
- **Database design**: Schema optimization for analytical queries
- **Docker integration**: Proper service setup and connectivity
- **Tool utilization and use of AI**: Effective use of available resources, tooling and modern development practices

## Getting Started

### What You'll Start With
Bootstrap application with three Docker services:

#### 1. API Service (TypeScript/Node.js) - 95% Complete ✅
- ETL job triggering (`POST /api/etl/jobs`) and data querying (`GET /api/data`) endpoints complete
- **Your task**: Add status tracking endpoint (`GET /api/etl/jobs/{id}/status`)

#### 2. ETL Service (Python/FastAPI) - 50% Complete ⚠️
- Job submission and status endpoints functional
- Sample data processing framework
- **Your task**: Complete the data processing pipeline and add quality validation

#### 3. PostgreSQL Database - Basic Schema ⚠️
- Tables created automatically on startup
- **Your task**: Design optimal schema and indexes for analytical queries

Technical details are available in [README_technical.md](./README_technical.md).

### 1. Setup Your Repository

```bash
# Extract the provided zip file
unzip clinical-api-etl.zip
cd clinical-api-etl

# Initialize git repository
git init
git add .
git commit -m "Initial project setup"

# Create a public GitHub repository and push
git remote add origin https://github.com/your-username/clinical-api-etl.git
git branch -M main
git push -u origin main
```

### 2. Start the Services

```bash
# Start all services
docker compose up --build

# Verify services are running
curl http://localhost:3000/health  # API Service
curl http://localhost:8000/health  # ETL Service
```

## Your Tasks

### Task 1: Add Status Endpoint (TypeScript)

Implement the `GET /api/etl/jobs/{id}/status` endpoint in the API service.

### Files to Modify:
1. `api-service/src/routes/etl.routes.ts` - Add route
2. `api-service/src/controllers/etl.controller.ts` - Add controller method  
3. `api-service/src/services/etl.service.ts` - Add service method

### Requirements:
- Connect to ETL service (`http://etl:8000/jobs/{id}/status`)
- Handle invalid job IDs (404 error)
- Handle connection errors gracefully
- Return consistent JSON response format

### Expected Response:
```json
{
  "success": true,
  "message": "Status retrieved successfully",
  "data": {
    "jobId": "uuid",
    "status": "running|completed|failed",
    "progress": 75,
    "message": "Processing data..."
  }
}
```

### Task 2: Complete ETL Service (Python)

Complete the ETL data processing pipeline and add quality validation in the Python service.

**Missing implementations:**
- Data transformation pipeline
- Quality validation rules
- Database loading
- Error handling

### Task 3: Database Schema Design (PostgresSQL)

Design and implement an optimized schema and indexes for analytical queries in PostgreSQL.

- Normalized tables for clinical data
- Proper indexes for queries
- Foreign key relationships
- Performance optimization

**Example Business Questions to Optimize For:**
- Which studies have the highest data quality scores?
- What are the glucose trends for a specific participant over time?
- How do measurement counts compare across different research sites?
- Which measurements have quality scores below our threshold?
- What clinical data was collected in the last 30 days?
- How many participants are enrolled in each study?
- What's the average BMI for participants in a specific study?

**Your schema should efficiently support these types of analytical queries.**

#### Sample Data

The following sample data is included:

- `data/sample_study001.csv` - Glucose, cholesterol, weight, height
- `data/sample_study002.csv` - Blood pressure, heart rate

The system processes clinical trial data in this standardized format:

```csv
study_id,participant_id,measurement_type,value,unit,timestamp,site_id,quality_score
STUDY001,P001,glucose,95.5,mg/dL,2024-01-15T09:30:00Z,SITE_A,0.98
STUDY001,P001,cholesterol,180,mg/dL,2024-01-15T09:30:00Z,SITE_A,0.95
STUDY001,P002,glucose,102.1,mg/dL,2024-01-15T10:15:00Z,SITE_A,0.97
STUDY002,P003,blood_pressure,120/80,mmHg,2024-01-15T11:00:00Z,SITE_B,0.92
```

## Testing Your Implementation

```bash
# 1. Submit a job
curl -X POST http://localhost:3000/api/etl/jobs \
  -H "Content-Type: application/json" \
  -d '{"filename": "sample_study001.csv", "studyId": "STUDY001"}'

# 2. Get job status (your implementation)
curl http://localhost:3000/api/etl/jobs/{job-id}/status

# 3. Query processed data
curl "http://localhost:3000/api/data?studyId=STUDY001"
```

## Submission Instructions
Submit your project by emailing your Git repository URL and any notes to [george.mitra@regeneron.com](mailto:george.mitra@regeneron.com).

### Final Submission Checklist
- [ ] Ensure all services start successfully with `docker compose up --build`
- [ ] Repository is public and URL is shared
- [ ] Brief explanation of AI tool usage is included in the repository
- [ ] Additional comments or notes about your approach are included (optional)