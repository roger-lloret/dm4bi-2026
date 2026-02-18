# Contributing to DM4BI-2026

Thank you for your interest in contributing to this data pipelines learning repository! This document provides guidelines for contributing.

## Ways to Contribute

### 1. Improve Documentation
- Fix typos or unclear explanations
- Add more examples or clarifications
- Translate documentation
- Add links to helpful resources

### 2. Add Examples
- Create new pipeline examples
- Show different use cases
- Demonstrate advanced techniques
- Add real-world scenarios

### 3. Create Exercises
- Design new learning exercises
- Add solution variations
- Create challenge problems
- Develop project ideas

### 4. Fix Bugs
- Report issues you find
- Fix code bugs
- Improve error handling
- Add missing validations

### 5. Enhance Code Quality
- Improve code readability
- Add type hints
- Optimize performance
- Add unit tests

## Getting Started

### 1. Fork the Repository

Click the "Fork" button at the top right of the repository page.

### 2. Clone Your Fork

```bash
git clone https://github.com/YOUR_USERNAME/dm4bi-2026.git
cd dm4bi-2026
```

### 3. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### 4. Set Up Your Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Contribution Guidelines

### Code Style

- Follow PEP 8 for Python code
- Use meaningful variable names
- Add docstrings to functions
- Keep functions focused and small
- Add comments for complex logic

#### Example:

```python
def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform sales data by cleaning and adding calculated fields.
    
    Args:
        df: Raw sales DataFrame
    
    Returns:
        Transformed DataFrame with cleaned data and new columns
    """
    # Remove duplicates
    df = df.drop_duplicates(subset=['transaction_id'])
    
    # Calculate total
    df['total_amount'] = df['quantity'] * df['unit_price']
    
    return df
```

### Documentation Style

- Use clear, simple language
- Provide examples for concepts
- Include both code and explanation
- Add prerequisites when needed
- Use proper markdown formatting

### Commit Messages

Write clear, descriptive commit messages:

**Good:**
```
Add example for real-time data pipeline
Fix bug in data validation logic
Update ETL documentation with error handling
```

**Not so good:**
```
Update
Fix stuff
Changes
```

### Pull Request Process

1. **Update Documentation**: If your changes affect usage, update relevant docs

2. **Test Your Changes**: Ensure your code works:
   ```bash
   python your_script.py
   ```

3. **Check Code Quality**:
   ```bash
   # Format code (if you have black installed)
   black your_file.py
   
   # Check for issues (if you have flake8 installed)
   flake8 your_file.py
   ```

4. **Create Pull Request**:
   - Go to your fork on GitHub
   - Click "New Pull Request"
   - Provide a clear description of changes
   - Reference any related issues

5. **Respond to Feedback**: Address review comments promptly

## What to Contribute

### High Priority
- Real-world pipeline examples
- More exercises (especially intermediate level)
- Data quality validation examples
- Performance optimization tips
- Common error solutions

### Welcome Additions
- Integration examples (APIs, databases, cloud services)
- Visualization of pipeline results
- CI/CD for data pipelines
- Testing strategies
- Monitoring and alerting examples

### Not Needed
- Duplicate examples without added value
- Overly complex examples for beginners
- Framework-specific code (keep it general)
- Large binary files or datasets

## Code Review Criteria

Contributions will be reviewed for:

1. **Correctness**: Code runs without errors
2. **Clarity**: Easy to understand for learners
3. **Documentation**: Well-documented and explained
4. **Relevance**: Fits the learning objectives
5. **Quality**: Follows best practices
6. **Completeness**: Includes necessary files and docs

## Adding New Examples

When adding a new example:

1. Create a new directory under `examples/`
2. Add a descriptive Python file with comments
3. Include sample data if needed (small files only)
4. Update `examples/README.md` with description
5. Test thoroughly

Structure:
```
examples/
├── your_new_example/
│   ├── example_pipeline.py
│   ├── sample_data.csv (if needed)
│   └── README.md (optional)
```

## Adding New Exercises

When adding a new exercise:

1. Create directory: `exercises/exercise_N/`
2. Add `README.md` with:
   - Clear objective
   - Requirements
   - Starter code
   - Sample data
   - Expected output
3. Include sample input data
4. Optionally add solution (in separate file)
5. Update `exercises/README.md`

## Questions?

If you have questions:
- Open an issue for discussion
- Check existing issues and pull requests
- Review documentation first

## Code of Conduct

### Our Pledge

- Be respectful and inclusive
- Welcome newcomers
- Accept constructive criticism
- Focus on what's best for learners

### Unacceptable Behavior

- Harassment or discrimination
- Trolling or insulting comments
- Publishing others' private information
- Other unprofessional conduct

## Recognition

Contributors will be recognized in:
- GitHub contributors list
- Release notes (for significant contributions)
- Special thanks section (for major contributions)

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

## Thank You!

Your contributions help others learn about data pipelines and make this resource better for everyone. We appreciate your time and effort! 🙏

---

**Happy Contributing!** 🚀

For questions or discussions, please open an issue on GitHub.
