# Week 3 Lab: Git Workflows, Testing, and CI/CD

This week focuses on software engineering practices for data engineering:
- Git branching and pull requests
- Writing tests with pytest
- Automated testing with GitHub Actions

## Lab Structure

### SQL Testing with pytest

**File:** `week3_lab.ipynb`
SQL notebook with tagged INSERT statements demonstrating basic testing concepts:
- Insert employees with a valid email address
- Insert employees within a salary range
- Insert employees hired after a specific date
- Insert employees from a specific department

**Domain:** Employee/HR data (different from the bookstore domain used in weeks 4-6)

Each INSERT statement is tagged with `-- @test:tag_name` so tests can extract and run them.

**Setup:** `create_week3_tables.ipynb`
Creates the `week3_testing` schema, populates the `employees` source table, and creates the empty `filtered_employees` target table.

**Tests:** `tests/test_week3_sql.py`
4 tests that validate each INSERT statement works correctly:
- Run the tagged INSERT using `_run_cell()`
- Query `filtered_employees` with `spark.sql()` to get results
- Assert the correct rows were inserted

**How to run:**
```bash
pytest tests/test_week3_sql.py -v
```

## Test Coverage

4 tests demonstrating the INSERT + verify pattern students will use in weeks 4-6.

**Focus areas:**
- Email format filtering
- Numeric range filtering (salary)
- Date filtering (recent hires)
- String filtering (department)

**Note:** Week 3 uses Employee/HR domain to learn testing fundamentals. The bookstore domain is introduced starting in Week 4.

## Learning Objectives

By the end of this lab, students can:
1. Create feature branches and submit pull requests
2. Write pytest tests with assertions
3. Test SQL INSERT statements using the tagging framework
4. Read test failures and debug issues
5. Set up GitHub Actions for automated testing
6. Understand the value of CI/CD in preventing bugs

## Next Steps

In weeks 4-6, students will:
- Use the same INSERT + verify pattern for bronze/silver/gold transformations
- Write more complex data validation tests
- Practice the Git workflow for all lab submissions
