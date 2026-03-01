# Data Engineering Event Processing Pipeline

## What Is This Project?

This project is a complete automated data processing system that collects, cleans, and analyzes event data from users. Think of it like an assembly line for data:

1. Raw data comes in continuously
2. The system cleans and organizes it
3. The analysis layer enriches it with business context
4. Results are ready for reporting and dashboards

The entire process runs automatically on a schedule without any manual intervention.

---

## How Does It Work? (The Complete Flow)

### Overview: Three-Layer Data Processing

The system uses a three-layer architecture where data gets progressively more refined:

```
Step 1: Raw Data (Bronze)
   |
   v
Step 2: Clean Data (Silver)
   |
   v
Step 3: Analysis-Ready Data (Gold)
```

### Stage 1: Bronze Layer - Raw Data Collection

The system continuously listens for new events from a message queue (RabbitMQ). It stores these raw, unprocessed events in object storage (MinIO).

What happens:
- Events arrive from various user activities
- Each event is stored exactly as received
- No data processing or cleaning yet
- Storage location: S3-compatible bucket called "datalake"
- Update frequency: Continuous (real-time)

### Stage 2: Silver Layer - Data Cleaning

Once every hour, the system automatically processes the raw data and cleans it:

What happens:
- Raw events are read from Bronze layer
- Invalid or incomplete records are removed
- Data formatting is standardized
- Duplicate entries are removed
- Clean data is stored in a structured format (Parquet)
- Database: PostgreSQL stores user and product information
- Storage location: datalake/silver/ in MinIO
- Schedule: Every hour at the top of the hour (1:00, 2:00, 3:00, etc.)

### Stage 3: Gold Layer - Business Analysis

Once per day, the system creates analysis-ready datasets by combining clean data with business context:

What happens:
- Clean event data is combined with user profiles
- Product information is joined with purchases
- Summary statistics are calculated (totals, counts, averages)
- Results are organized by user, product, and time period
- Storage location: datalake/gold/ in MinIO
- Schedule: Every day at 2:00 AM
- This data powers dashboards and reports

### Data Flow Diagram

```
User Activity Events (RabbitMQ)
            |
            v
    Bronze Layer (Raw)
    S3 Storage
            |
            v
   [Runs Every Hour]
            |
            v
    Silver Layer (Cleaned)
    + PostgreSQL Dimensions
            |
            v
   [Runs Every Day]
            |
            v
    Gold Layer (Analytics)
    Ready for Reports
```

---

## System Architecture

### What You Need On Your Computer

The system runs inside containers, which means everything is isolated and consistent. You need:

1. **Docker and Docker Compose** - Container management tool
2. **At least 8GB RAM** - All services need memory
3. **50GB free disk space** - For data and databases
4. **Windows, Mac, or Linux** - Any modern operating system

No need to install Python, PostgreSQL, Spark, or any other tool individually - everything comes packaged.

### Services That Run

When you start the system, 9 different services start automatically:

| Service | Purpose | What It Does |
|---------|---------|--------------|
| PostgreSQL | Database | Stores user and product information |
| Airflow | Scheduler | Runs data jobs on schedule |
| MinIO | Storage | Stores data files (like S3 cloud storage) |
| RabbitMQ | Message Queue | Receives events from applications |
| Spark Master | Processing | Orchestrates data processing jobs |
| Spark Worker | Processing | Processes (cleans and analyzes) data |
| Event Ingestion | Application | Reads from RabbitMQ, writes to MinIO |
| Airflow Webserver | Interface | Web interface to monitor jobs (http://localhost:8082) |
| Airflow Scheduler | Automation | Triggers jobs at scheduled times |

### Ports and Access Points

Once running, you can access:

| Service | Address | Purpose |
|---------|---------|---------|
| Airflow | http://localhost:8082 | Monitor and manage data jobs |
| MinIO Console | http://localhost:9001 | Browse stored data files |
| RabbitMQ Admin | http://localhost:15675 | Monitor message queue |
| PostgreSQL | localhost:5433 | Direct database access |
| Spark | http://localhost:8080 | Monitor data processing |

---

## Getting Started: Step-by-Step Instructions

### Step 1: Prerequisites

Before you begin, install these on your computer:

**Windows:**
1. Install Docker Desktop for Windows
2. Enable Windows Subsystem for Linux 2 (WSL2)
3. Allocate at least 4GB RAM to Docker in settings

**Mac:**
1. Install Docker Desktop for Mac
2. Allocate at least 4GB RAM to Docker in settings

**Linux:**
1. Install Docker and Docker Compose using your package manager

To check if Docker is installed, open command line/terminal and type:
```bash
docker --version
docker-compose --version
```

Both commands should show version numbers.

### Step 2: Obtain the Code

**Option A: Clone from GitHub (Recommended)**

```bash
git clone https://github.com/YOUR-USERNAME/YOUR-REPO.git
cd 1_eng_project
```

Replace `YOUR-USERNAME` and `YOUR-REPO` with the actual repository details.

**Option B: Manual Download**

If you don't have Git installed:
1. Visit the GitHub repository
2. Click the green "Code" button
3. Click "Download ZIP"
4. Extract the ZIP file
5. Open terminal/command prompt in the extracted folder

### Step 3: Set Up Environment Variables

Environment variables are settings that configure how the system works. The system needs database passwords and access credentials.

**First time setup:**

```bash
cd docker
cp .env.example .env
```

This creates a `.env` file with default values.

**Important:** Your system administrator or team lead needs to provide you with the actual passwords. Ask them for:
- PostgreSQL password
- MinIO credentials
- Airflow admin password

Once you have these, open the `.env` file and replace the placeholder values:

```bash
nano docker/.env
# or
# Right-click and open with Notepad on Windows
```

Look for lines like:
```
POSTGRES_PASSWORD=your_secure_postgres_password
```

Replace them with the actual password provided:
```
POSTGRES_PASSWORD=actualpasswordhere
```

Save the file and close it.

### Step 4: Start the System

From the `docker` folder:

```bash
docker-compose up -d
```

This command pulls all service images and starts them in the background. It takes 30-60 seconds.

To watch the startup process (optional):

```bash
docker-compose logs -f
```

Press Ctrl+C to stop viewing logs.

### Step 5: Verify Everything Started

Check that all services are running:

```bash
docker-compose ps
```

You should see 9 containers with status "Up". If any show "Exited", there's an issue.

### Step 6: Access the Web Interfaces

Open your web browser and go to:

**Main Interface (Airflow):**
- Address: http://localhost:8082
- Username: airflow
- Password: (from your .env file)
- What you see: List of data jobs, execution history, logs

**Data Storage (MinIO):**
- Address: http://localhost:9001
- Username: minio
- Password: (from your .env file)
- What you see: Folders containing all processed data files

---

## Using the System: Common Tasks

### Viewing Data Jobs

1. Open http://localhost:8082 in your browser
2. You'll see "Dags" (Data Jobs) on the left menu
3. Current jobs:
   - **silver_cleaning**: Runs every hour to clean raw data
   - **gold_aggregation**: Runs daily to create analysis datasets
   - **bronze_ingestion**: Continuously reads new events

### Checking Data in Storage

1. Open http://localhost:9001
2. Click on "datalake" bucket
3. You'll see folders:
   - `bronze/` - Raw, unprocessed data
   - `silver/` - Cleaned data
   - `gold/` - Analysis-ready data

### Viewing Raw Data Files

Data is stored in Parquet format (efficient columnar storage):

```bash
# List files in silver layer
docker exec spark-master bash -c \
  "hadoop fs -s3a://datalake/silver/events_clean/"
```

### Accessing the Database

If you're technical and want to query the database directly:

```bash
docker exec postgres psql -U shaheer -d data_storage_db
```

Then you can run SQL queries to explore the data.

### Viewing System Logs

To see what the system is doing:

```bash
# Airflow webserver logs
docker logs airflow-webserver

# Spark processing logs
docker logs spark-master

# Data ingestion logs
docker logs bronze-events-ingestion
```

---

## Stopping and Starting

### Stop All Services (Pause the System)

```bash
cd docker
docker-compose stop
```

This stops all running containers without deleting data.

### Start Again (Resume After Stopping)

```bash
cd docker
docker-compose up -d
```

### Completely Remove Everything (Nuclear Option)

Only do this if you want to start fresh:

```bash
cd docker
docker-compose down -v
```

Warning: This deletes all data! Only use if you want a clean slate.

---

## Troubleshooting: Common Issues

### Issue: Services Won't Start

**Check if Docker is running:**
```bash
docker ps
```

If this fails, Docker isn't running. Start Docker Desktop.

**Check service logs:**
```bash
cd docker
docker-compose logs
```

### Issue: "Connection refused" errors

Wait 60 seconds. Services take time to start up.

Then check status:
```bash
docker-compose ps
```

All should show "Up". If any show "Exited", look at logs.

### Issue: Cannot access http://localhost:8082

1. Check if Airflow container is running: `docker ps | grep airflow`
2. Wait 2 more minutes for Airflow to fully initialize
3. Try clearing your browser cache (Ctrl+Shift+Delete)
4. Try a different browser

### Issue: Wrong Credentials Error

The `.env` file passwords don't match what's actually running.

Solution:
1. Stop services: `docker-compose down`
2. Edit `.env` with correct passwords
3. Start again: `docker-compose up -d`
4. Wait 60 seconds

### Issue: Out of Disk Space

The system stores all processed data on disk. If it fills up:

1. Check current disk usage: `docker system df`
2. Delete old data in MinIO if not needed
3. Or increase disk space on your computer

### Issue: Running Out of Memory

If system becomes slow:

1. Reduce concurrent data processing in `docker-compose.yml`
2. Or allocate more RAM to Docker (Docker Desktop settings)
3. Or stop other applications temporarily

---

## Project Structure: File Locations

Understanding where files are located:

```
1_eng_project/
├── docker/
│   ├── docker-compose.yml          Configuration to start all services
│   ├── .env                        Your passwords (do not share)
│   └── .env.example                Template for .env
│
├── airflow/
│   └── dags/                       Data job definitions
│       ├── silver_cleaning_dag.py
│       ├── gold_aggregation_dag.py
│       └── bronze_ingestion_dag.py
│
├── transformations/                Actual data processing code
│   ├── bronze_to_silver_clean.py    Cleans raw data
│   └── silver_to_gold.py            Creates analysis datasets
│
├── ingestion/                       Code to read from RabbitMQ
│   └── rabbitmq_to_bronze.py
│
├── config/
│   ├── app_config.yaml              Configuration settings
│   └── logging_config.yaml
│
├── sql/                             Database initialization
│   └── 01_create_oltp_tables.sql
│
└── README.md                        This file
```

---

## For Developers: Modifying the System

If you want to change how data is processed:

### To Change Processing Logic

1. Edit files in `transformations/`
2. Example: `transformations/bronze_to_silver_clean.py`
3. Changes take effect next scheduled run
4. Or manually trigger in Airflow UI

### To Add a New Job

1. Create a new DAG file in `airflow/dags/`
2. Define schedule (every hour, daily, etc.)
3. Restart Airflow: `docker-compose restart airflow-webserver`
4. New job appears in Airflow UI

### To Change Job Schedule

1. Edit the DAG file in `airflow/dags/`
2. Find the `schedule_interval` parameter
3. Use cron syntax or preset values
4. Restart Airflow to apply changes

---

## Security and Best Practices

### Protecting Your Credentials

The `.env` file contains passwords. Never:
- Commit it to Git
- Share it via email or Slack
- Include it in screenshots or logs

It's already protected by `.gitignore` (excluded from Git).

### Rotating Passwords

To change passwords periodically:

1. Get new passwords from your team
2. Update `docker/.env`
3. Also update in actual systems:
   - PostgreSQL: `docker exec postgres psql -U postgres -c "ALTER USER shaheer WITH PASSWORD 'newpassword';"`
   - MinIO: Through MinIO console
4. Stop and restart services: `docker-compose restart`

### Regular Backups

Important data is stored in MinIO and PostgreSQL. Backup regularly:

```bash
# Backup PostgreSQL
docker exec postgres pg_dump -U shaheer data_storage_db > backup.sql

# Backup MinIO data
# (Download important files through MinIO console)
```

---

## Running on Different Environments

### Development Machine
- Just follow the Getting Started section above
- All data is local and temporary

### Production Server
- Use environment-specific `.env` files
- Mount persistent volumes for data
- Set resource limits in docker-compose.yml
- Monitor logs and set up alerts
- Regular backup strategy

### Cloud Deployment (AWS, Google Cloud, Azure)
- Container orchestration (Kubernetes) recommended
- Use managed services (RDS for PostgreSQL, S3 for MinIO)
- Implement auto-scaling
- Enhanced security and monitoring

---

## Understanding Data Quality

The system maintains data quality through several mechanisms:

### Validation
- Invalid records are flagged and excluded
- Duplicates are removed
- Missing required fields cause records to be rejected

### Monitoring
- Failed jobs are logged in Airflow
- Processing metrics are tracked
- Failures trigger notifications (if configured)

### Recovery
- If a job fails, it can be re-run
- Partial data is handled gracefully
- No data loss on failures

---

## Performance and Scaling

### Current Configuration

The system is configured to process:
- Continuous event streaming (real-time)
- Hourly cleaning of collected events
- Daily aggregation of patterns

Processing time:
- Cleaning (Silver): 5-10 minutes for 24 hours of events
- Aggregation (Gold): 10-15 minutes for all users and products

### Scaling Up

If you need to process more data:

1. Increase Spark worker resources in `docker-compose.yml`
2. Allocate more RAM to Docker container
3. Increase executor memory in DAG files
4. Consider adding more Spark workers

### Optimization

For faster processing:

1. Partition data by date
2. Add caching for frequently accessed dimensions
3. Use columnar format (Parquet) for storage
4. Optimize SQL queries on PostgreSQL

---

## Frequently Asked Questions

### Q: Can I run this on a laptop?

A: Yes, but it needs:
- At least 8GB RAM
- 50GB free disk space
- Fast internet for initial download
- Newer operating system (Windows 10+, macOS 10.15+, recent Linux)

### Q: What happens to my data if I stop the system?

A: Data in MinIO and PostgreSQL persists. When you restart, all data is still there.

### Q: How can I monitor if things are working?

A: Check Airflow UI (http://localhost:8082) to see:
- Job execution history
- Success/failure status
- Processing logs
- Next scheduled runs

### Q: Can I use this with my own data?

A: Yes! You need to:
1. Connect your data source to RabbitMQ
2. Or modify the ingestion layer to read from your source
3. Adjust the data model as needed

### Q: What if I need to modify the processing logic?

A: Edit the files in `transformations/` folder. Your changes take effect on the next run.

### Q: Is my data secure?

A: Data is stored locally or in MinIO (your own storage). Credentials are protected in `.env` file (not committed to Git). Consider additional security measures for production use.

### Q: Can I run this on AWS, Google Cloud, or Azure?

A: Yes! You can:
1. Run on a cloud instance (EC2, Compute Engine, VM)
2. OR use managed services (Lambda, Cloud Functions) for processing
3. OR use Kubernetes for orchestration

---

## Support and Documentation

### Additional Resources

- **Airflow Documentation:** https://airflow.apache.org/docs/
- **Spark Documentation:** https://spark.apache.org/docs/latest/
- **PostgreSQL Documentation:** https://www.postgresql.org/docs/
- **MinIO Documentation:** https://min.io/docs/

### Project Documentation Files

In the project folder, you'll find:
- `SETUP.md` - Environment setup guide
- `GITHUB_PUSH_CHECKLIST.md` - Before pushing to GitHub
- `MIGRATION_SUMMARY.md` - Technical migration details
- `SYSTEM_INTEGRITY_REPORT.md` - System health check report

### Getting Help

If something isn't working:

1. Check the troubleshooting section above
2. Look at service logs: `docker-compose logs SERVICE_NAME`
3. Check Airflow UI for job failures
4. Review error messages in the logs
5. Ask your team members or administrator

---

## Next Steps

Once you have everything running:

1. Log in to Airflow (http://localhost:8082)
2. Verify both DAGs are enabled (toggle is blue)
3. Monitor the scheduled job runs
4. Check MinIO to see processed data accumulating
5. Query PostgreSQL to analyze user and product data
6. Set up dashboards or reports using the cleaned data from Gold layer

---

## Version and Updates

Current Version: 1.0
Last Updated: February 26, 2026

To check for updates:
```bash
git pull origin main
```

If there are configuration changes:
1. Review the changes
2. Update your `.env` file if needed
3. Rebuild containers: `docker-compose down && docker-compose up -d`

---

## License and Attribution

This project processes event data through an automated three-layer pipeline. It uses Apache Airflow, Apache Spark, PostgreSQL, and MinIO for enterprise-grade data processing.

For questions or clarifications, contact your data engineering team.

Happy data processing!
