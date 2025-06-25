# Azure Integrator Project - Claude Code Configuration

## Project Overview
This is a custom Python library for Azure Blob Storage operations, designed specifically for data engineers who need a simple, educational interface to Azure cloud storage services.

### Key Features
- **Educational Comments**: Extensive inline comments explaining what each piece of code does
- **Data Format Support**: Convert between DataFrames and multiple formats (CSV, Excel, Parquet)
- **Complete CRUD Operations**: Create, Read, Update, Delete for both containers and blobs
- **Error Handling**: Comprehensive error handling with clear, informative messages
- **Environment Configuration**: Secure credential management using .env files
- **Type Safety**: Full type hints for better code reliability

### Project Structure
- `azure_integrator.py` - Main library with AzureBlobStorage class
- `original_code_backup.md` - Backup of original code before refactoring
- `CHANGES.md` - Documentation of all improvements made
- `test.ipynb` - Jupyter notebook for testing functionality

## Coding Style Preferences

### Comment Style
I prefer **extensive educational comments** that help new data engineers understand:
- **What** each piece of code does
- **Why** we're doing it this way
- **How** it relates to the bigger picture
- **Real-world analogies** (like "opening doors", "copying files", "creating folders")

Example of preferred commenting style:
```python
# Get connection details
# Can provide data in parameters or use .env file  
# Priority: parameter value > environment variable > None
self.connection_string = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')

# This is a security mechanism that:
# Catches all possible errors during connection
# Informs user in a readable way what went wrong
# Allows for appropriate error response
```

### Code Organization
- **Section separators**: Use long equals lines (`======`) to visually separate method groups
- **Method grouping**: Group related methods together with descriptive headers
- **Clear docstrings**: Every method should have comprehensive docstrings
- **Type hints**: Always include type hints for parameters and return values

### Error Handling Philosophy
- **Never fail silently** - always provide clear error messages
- **Educational errors** - explain what went wrong and how to fix it
- **Consistent exceptions** - use appropriate exception types (ValueError, RuntimeError, ConnectionError)
- **User-friendly messages** - avoid technical jargon when possible

### Dependencies Management
Current project dependencies:
- `azure-storage-blob` - Azure SDK for blob operations
- `pandas` - Data manipulation and analysis
- `python-dotenv` - Environment variable management
- `typing` - Type hints support

### Environment Variables
Always use .env files for sensitive configuration:
- `AZURE_STORAGE_CONNECTION_STRING` - Azure storage account connection string
- `CONTAINER_NAME` - Default container name for operations

## Development Guidelines

### When Adding New Methods
1. Add extensive educational comments explaining the purpose
2. Include real-world analogies where helpful
3. Provide comprehensive error handling
4. Add type hints for all parameters and return values
5. Write clear docstrings with examples if needed
6. Group related methods together with section separators

### Code Review Focus Areas
- **Comment quality**: Are the comments educational and helpful for beginners?
- **Error handling**: Does it provide clear, actionable error messages?
- **Type safety**: Are all parameters and returns properly typed?
- **Consistency**: Does it follow the established patterns in the codebase?

### Testing Approach  
- Test all methods with valid inputs
- Test error conditions and edge cases
- Verify error messages are clear and helpful
- Test with different data formats (CSV, Excel, Parquet)
- Validate environment variable handling