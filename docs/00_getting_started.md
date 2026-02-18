# Getting Started with Data Pipelines

This guide will help you set up your environment and start learning about data pipelines.

## Prerequisites

- **Python 3.8+**: Check your version with `python --version`
- **pip**: Python package manager
- **Git**: Version control (optional but recommended)
- **Text Editor/IDE**: VS Code, PyCharm, or your preferred editor

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/roger-lloret/dm4bi-2026.git
cd dm4bi-2026
```

### 2. Create a Virtual Environment

Creating a virtual environment isolates your project dependencies:

**On Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**On Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

You should see `(venv)` in your terminal prompt.

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

This will install:
- pandas: Data manipulation
- numpy: Numerical computing
- sqlalchemy: Database connectivity
- apache-airflow: Workflow orchestration
- And other necessary packages

### 4. Verify Installation

```bash
python -c "import pandas; print('pandas version:', pandas.__version__)"
python -c "import airflow; print('airflow version:', airflow.__version__)"
```

## Learning Path

### Week 1: Fundamentals
1. **Read Documentation** (`docs/01_introduction.md`)
   - Understand what data pipelines are
   - Learn about ETL concepts
   - Explore pipeline components

2. **Run Simple Example** (`examples/basic_etl/simple_etl.py`)
   ```bash
   cd examples/basic_etl
   python simple_etl.py
   ```

3. **Complete Exercise 1** (`exercises/exercise_1/`)
   - Build your first ETL pipeline
   - Practice data cleaning and transformation

### Week 2: Data Transformation
1. **Read ETL Documentation** (`docs/02_etl_processes.md`)
   - Deep dive into ETL processes
   - Learn transformation patterns
   - Understand error handling

2. **Explore Transformations** (`examples/data_transformation/transformation_examples.py`)
   ```bash
   cd examples/data_transformation
   python transformation_examples.py
   ```

3. **Complete Exercise 2** (`exercises/exercise_2/`)
   - Work with aggregations
   - Generate reports
   - Practice advanced transformations

### Week 3: Workflow Orchestration
1. **Read Airflow Guide** (`docs/03_airflow_orchestration.md`)
   - Learn about Apache Airflow
   - Understand DAGs and operators
   - Explore scheduling

2. **Study Airflow Example** (`examples/airflow_dags/daily_sales_etl_dag.py`)
   - Review the DAG structure
   - Understand task dependencies
   - Learn XCom usage

3. **Complete Exercise 3** (`exercises/exercise_3/`)
   - Create your own Airflow DAG
   - Orchestrate a multi-step pipeline

## Running Examples

### Basic ETL Example

```bash
cd examples/basic_etl
python simple_etl.py
```

This will:
- Create sample sales data
- Run the ETL pipeline
- Generate processed output

### Data Transformation Examples

```bash
cd examples/data_transformation
python transformation_examples.py
```

This demonstrates:
- Data cleaning techniques
- Type conversions
- Feature engineering
- Aggregations
- Pivoting

## Working with Airflow

### Initialize Airflow (First Time Only)

```bash
# Set Airflow home directory
export AIRFLOW_HOME=~/airflow

# Initialize the database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### Start Airflow

**Terminal 1 - Start the webserver:**
```bash
airflow webserver --port 8080
```

**Terminal 2 - Start the scheduler:**
```bash
airflow scheduler
```

### Access Airflow UI

Open your browser and go to: `http://localhost:8080`

Login with:
- Username: admin
- Password: (the one you set during user creation)

### Add Your DAGs

Copy your DAG files to the Airflow DAGs folder:
```bash
cp examples/airflow_dags/*.py ~/airflow/dags/
```

Airflow will automatically detect and load your DAGs.

## Tips for Success

### 1. Start Small
- Begin with simple examples
- Understand each concept before moving on
- Run code frequently to see results

### 2. Practice Regularly
- Complete all exercises
- Try modifying examples
- Experiment with different datasets

### 3. Debug Effectively
- Read error messages carefully
- Use print statements for debugging
- Check data at each pipeline stage

### 4. Use Documentation
- Refer to official docs for pandas, airflow, etc.
- Read code comments
- Ask questions when stuck

### 5. Version Control
- Use git to track your progress
- Commit frequently
- Write meaningful commit messages

## Common Issues and Solutions

### Issue: Import Errors
```
ModuleNotFoundError: No module named 'pandas'
```
**Solution:** Make sure your virtual environment is activated and dependencies are installed:
```bash
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### Issue: Airflow Not Starting
```
ERROR: Airflow command error
```
**Solution:** Initialize the database first:
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
```

### Issue: File Not Found
```
FileNotFoundError: [Errno 2] No such file or directory
```
**Solution:** Check your current working directory and use absolute paths or navigate to the correct directory.

## Additional Resources

### Documentation
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

### Tutorials
- [Real Python - Data Science Tutorials](https://realpython.com/tutorials/data-science/)
- [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)

### Books
- "Data Pipelines Pocket Reference" by James Densmore
- "Designing Data-Intensive Applications" by Martin Kleppmann

## Next Steps

1. ✅ Complete the installation
2. ✅ Read the introduction documentation
3. ✅ Run your first example
4. ✅ Complete Exercise 1
5. ⬜ Progress through Week 2 and 3
6. ⬜ Build your own data pipeline project

## Getting Help

- Review the documentation in `/docs`
- Check the examples in `/examples`
- Read through exercise solutions
- Experiment and learn by doing!

Happy Learning! 🚀
