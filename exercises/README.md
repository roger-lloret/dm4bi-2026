# Exercises

Practice what you've learned with these hands-on exercises!

## Available Exercises

### Exercise 1: Build Your First ETL Pipeline
**Level**: Beginner  
**Topics**: Basic ETL, Data Cleaning, CSV Processing  
**Time**: 1-2 hours

Build a simple ETL pipeline that processes e-commerce transaction data.

📁 Location: `exercise_1/`  
📖 Instructions: See `exercise_1/README.md`

**What you'll learn:**
- Extract data from CSV files
- Clean and validate data
- Transform data with calculations
- Load processed data to output

### Exercise 2: Data Aggregation and Reporting
**Level**: Intermediate  
**Topics**: Aggregations, Grouping, Multiple Outputs  
**Time**: 2-3 hours

Create a reporting pipeline that generates multiple analytical views from sales data.

📁 Location: `exercise_2/`  
📖 Instructions: See `exercise_2/README.md`

**What you'll learn:**
- Complex data transformations
- Group by operations
- Multiple output generation
- Creating insights from data

### Exercise 3: Build an Airflow DAG
**Level**: Advanced  
**Topics**: Airflow, Orchestration, Task Dependencies  
**Time**: 3-4 hours

Create a production-ready Airflow DAG to orchestrate a multi-step data pipeline.

📁 Location: `exercise_3/`  
📖 Instructions: See `exercise_3/README.md`

**What you'll learn:**
- Airflow DAG structure
- Task dependencies
- XCom for data passing
- Data quality checks
- Error handling in workflows

## How to Approach Exercises

### 1. Read the Requirements
- Carefully read the exercise README
- Understand the business context
- Note all requirements

### 2. Plan Your Solution
- Sketch out the pipeline steps
- Identify input and output
- List transformations needed

### 3. Implement Incrementally
- Start with extraction
- Add transformations one by one
- Test frequently
- Add error handling

### 4. Test Thoroughly
- Use provided sample data
- Verify output matches requirements
- Test edge cases
- Check data quality

### 5. Refine and Optimize
- Review your code
- Add comments
- Improve error handling
- Consider performance

## Tips for Success

### Do's ✅
- Start with the examples in `/examples`
- Read the documentation in `/docs`
- Test each component independently
- Use print statements for debugging
- Commit your work regularly

### Don'ts ❌
- Don't skip reading the documentation
- Don't copy-paste without understanding
- Don't ignore errors or warnings
- Don't try to do everything at once

## Getting Help

If you're stuck:

1. **Review Documentation**: Check `/docs` for concepts
2. **Study Examples**: Look at similar code in `/examples`
3. **Debug Step by Step**: Use print statements to see what's happening
4. **Break It Down**: Solve one small part at a time
5. **Read Error Messages**: They often tell you exactly what's wrong

## Solutions

Solutions are provided for reference, but try to solve exercises yourself first!

The best way to learn is by:
- Attempting the exercise
- Getting stuck and figuring it out
- Comparing your solution with provided one
- Understanding different approaches

## Progression Path

```
Exercise 1 → Exercise 2 → Exercise 3
   (Basics)    (Intermediate)  (Advanced)
```

Complete exercises in order as they build on each other.

## Beyond the Exercises

Once you complete all exercises:

1. **Create Your Own**: Build pipelines for datasets you're interested in
2. **Combine Concepts**: Mix ideas from multiple exercises
3. **Add Features**: Extend exercises with bonus challenges
4. **Share Knowledge**: Help others who are learning

## Common Challenges

### Challenge: "I don't know where to start"
**Solution**: Start with the extract function. Read data and print it. Then move to transform.

### Challenge: "My code has errors"
**Solution**: Read the error message carefully. It tells you the line number and type of error.

### Challenge: "Output doesn't match requirements"
**Solution**: Print intermediate results at each transformation step to see where it diverges.

### Challenge: "Airflow DAG not appearing"
**Solution**: Check for syntax errors. Run `python your_dag.py` to test import.

## Additional Practice

Looking for more practice?

1. **Modify Exercises**: Change requirements to make them harder
2. **Real Datasets**: Find datasets on Kaggle or data.gov
3. **Build Projects**: Create end-to-end pipelines for real problems
4. **Contribute**: Add your own exercises to help others learn

Happy Learning! 🎓
