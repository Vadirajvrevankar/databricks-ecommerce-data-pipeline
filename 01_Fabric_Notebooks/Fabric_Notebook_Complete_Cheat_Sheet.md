# Microsoft Fabric Notebook --- Complete Interview Cheat Sheet

## 1. Notebook Basics

-   **Notebook** → Interactive environment for PySpark / Python / SQL
-   **Cell** → Individual unit of code
-   **Run Cell** → Execute current cell
-   **Run All** → Execute complete notebook
-   **Cancel / Stop** → Stop execution
-   **Restart Session** → Restart Spark/Python session

## 2. Magic Commands

-   **`%run`** → Reuse another notebook's code
-   **`%pip`** → Install Python libraries
-   **`%sql`** → Execute SQL
-   **`%%configure`** → Configure Spark session
-   **`%time`** → Measure execution time
-   **`%timeit`** → Benchmark execution
-   **`%ls`** → List files
-   **`%pwd`** → Current working directory

## 3. `%run`

-   Reuse another notebook
-   Functions become available
-   Variables become available
-   Common code / shared configuration

**Memory:** `%run` → REUSE CODE

## 4. `notebookutils.notebook.run()`

-   Execute another child notebook
-   Notebook orchestration
-   Pass parameters
-   Receive child result

**Memory:** `notebook.run()` → EXECUTE CHILD NOTEBOOK

## 5. `%run` vs `notebook.run()`

  `%run`                `notebookutils.notebook.run()`
  --------------------- --------------------------------
  Reuse code            Execute child notebook
  Functions available   Programmatic execution
  Variables available   Pass parameters
  Shared code           Receive exit value

**Easy memory:** `%run` = REUSE \| `notebook.run()` = EXECUTE

## 6. Notebook Parameters

-   Input values given to notebook
-   Make notebook reusable
-   Pipeline → Parameters → Notebook
-   Common: date, path, environment, file name, table name

**Memory:** Parameters → SEND DATA IN

## 7. Child Notebook Communication

**Parent → Child** - Parameters / arguments

**Child → Parent** - `notebookutils.notebook.exit()` - Return value /
message / status

**Flow:** `Parent → run() → Child → exit() → Parent`

## 8. `notebookutils.notebook.exit()`

-   Return a value/message from child notebook
-   End child notebook execution
-   Return status / result

**Memory:** `exit()` → SEND RESULT OUT

## 9. `runMultiple()`

-   Execute multiple notebooks
-   Parallel execution
-   Dependencies
-   DAG workflow

**Memory:** `run()` = one \| `runMultiple()` = multiple

## 10. `notebookutils`

-   **`notebookutils.fs`** → File system
-   **`notebookutils.notebook`** → Notebook execution
-   **`notebookutils.runtime.context`** → Runtime/execution context
-   **`notebookutils.session`** → Session operations
-   **`notebookutils.credentials`** → Credential-related utilities
-   **`notebookutils.lakehouse`** → Lakehouse utilities
-   **`notebookutils.variableLibrary`** → Variable Library operations

**Memory:** `notebookutils.fs` = FILES \| `notebookutils.notebook` =
NOTEBOOKS

## 11. File System

-   **Lakehouse → Files** → File-based data
-   **Lakehouse → Tables** → Managed analytical data
-   **Default Lakehouse** → Primary Lakehouse
-   **Attached Lakehouse** → Additional Lakehouse
-   **Path** → Location of resource
-   Relative path
-   Absolute path

## 12. `notebookutils.fs`

-   **`ls()`** → List
-   **`cp()`** → Copy
-   **`mv()`** → Move
-   **`rm()`** → Delete
-   **`mkdirs()`** → Create directory
-   **`exists()`** → Check existence

## 13. `%%configure`

-   Configure Spark session
-   Spark properties
-   Session configuration
-   Compute/resource configuration
-   Default Lakehouse configuration

## 14. Spark Session

-   **`spark`** → Spark entry point
-   Read
-   Write
-   SQL
-   DataFrame operations
-   Spark configuration

## 15. Exception Handling

-   **`try`** → Try code
-   **`except`** → Catch exception
-   **`raise`** → Propagate exception
-   **`finally`** → Cleanup

**Memory:** `try` = TRY \| `except` = CATCH \| `raise` = INFORM PIPELINE

## 16. Retry

-   Execute failed activity again
-   Useful for transient failures
-   Not a solution for permanent code/data errors

**Memory:** Retry → TRY AGAIN

## 17. Timeout

-   Maximum allowed execution time
-   Prevent long-running/stuck execution

**Memory:** Timeout → STOP AFTER LIMIT

## 18. Logging

Track: - Start - End - Parameters - Input - Record count - Processing
status - Error - Duration - Output

**Memory:** Logging → WHAT HAPPENED?

## 19. Debugging

-   **`display()`** → Visual DataFrame inspection
-   **`show()`** → Display rows
-   **`printSchema()`** → Check schema
-   **`count()`** → Record count
-   **`print()`** → Values/messages
-   **Stack trace** → Error location
-   **Logs** → Execution investigation

**Memory:** Debugging → WHY DID IT FAIL?

## 20. Data Validation

Check: - File exists - Data available - Schema - Required columns - Data
types - Nulls - Duplicates - Record count - Business rules

## 21. Notebook Resources

-   Supporting files/resources used by notebook
-   Small supporting files
-   Reusable code/resources
-   Notebook dependencies

## 22. Variable Library

-   Centralized reusable variables/configuration
-   Environment values
-   Reusable settings

## 23. Runtime Context

-   **`notebookutils.runtime.context`**
-   Notebook information
-   Workspace information
-   Execution information

## 24. Session Utilities

-   **`notebookutils.session`**
-   Session management
-   Stop/restart session-related operations

## 25. Secrets / Credentials

Never hardcode: - Password - API key - Secret - Access token

Use: - Secure credential mechanisms - Connections - Managed identity
where applicable

**Memory:** Code ≠ Credentials

## 26. Idempotency

-   Safe to rerun without incorrect duplicate results
-   Important for retry, rerun, failure recovery, and backfill

## 27. Notebook Chaining

`Notebook 1 → Notebook 2 → Notebook 3`

Used for: - Modular processing - Separate responsibilities -
Maintainability

## 28. Pipeline vs Notebook

### Pipeline → ORCHESTRATE

-   Schedule
-   Dependencies
-   Parameters
-   Retry
-   Timeout
-   Monitoring
-   Activity execution

### Notebook → PROCESS

-   PySpark
-   Python
-   SQL
-   Transformation
-   Validation
-   Business logic

**Memory:** Pipeline = CONTROL \| Notebook = PROCESS

## 29. Monitoring

Monitor: - Running - Success - Failed - Duration - Output - Error -
Logs - Activity status

## 30. Performance

Avoid unnecessary: - `collect()` - Repeated `count()` - Unnecessary
`display()` - Large driver-side operations

Remember: - Spark is distributed - Avoid moving large data to driver

## 31. Databricks `dbutils`

-   **`dbutils`** → Databricks utility
-   **`dbutils.fs`** → File system
-   **`dbutils.notebook.run()`** → Run notebook
-   **`dbutils.notebook.exit()`** → Return result
-   **`dbutils.widgets`** → Notebook input parameters

## 32. Databricks Widgets

-   **`dbutils.widgets`** → Create notebook input controls
-   **`text()`** → Text input
-   **`dropdown()`** → Dropdown
-   **`combobox()`** → Combo box
-   **`multiselect()`** → Multiple selections
-   **`get()`** → Get widget value
-   **`remove()`** → Remove widget
-   **`removeAll()`** → Remove all widgets

**Memory:** Databricks → Widgets for notebook inputs

## 33. Fabric vs Databricks

  Fabric                            Databricks
  --------------------------------- ---------------------------
  `notebookutils`                   `dbutils`
  `notebookutils.fs`                `dbutils.fs`
  `notebookutils.notebook.run()`    `dbutils.notebook.run()`
  `notebookutils.notebook.exit()`   `dbutils.notebook.exit()`
  `%run`                            `%run`
  Notebook Parameters               `dbutils.widgets`

**Memory:** Fabric → `notebookutils` \| Databricks → `dbutils`

## 34. Old Fabric API

-   **`mssparkutils`** → Older Fabric utility API
-   **`notebookutils`** → Current/recommended Fabric utility API

**Memory:** Old Fabric → `mssparkutils` \| Current Fabric →
`notebookutils`

## 35. Most Important Flow

`PIPELINE` ↓ `PARAMETERS` ↓ `PARENT NOTEBOOK` ↓
`notebookutils.notebook.run()` ↓ `CHILD NOTEBOOK` ↓
`notebookutils.notebook.exit()` ↓ `PARENT / PIPELINE` ↓ `RESULT`

## 36. Final Memory Map

-   **`%run`** → Reuse code / functions / variables
-   **`notebook.run()`** → Execute child notebook
-   **Parameters** → Send data in
-   **`exit()`** → Send result out
-   **`runMultiple()`** → Multiple notebooks
-   **`notebookutils.fs`** → File system
-   **`%%configure`** → Spark configuration
-   **Exception** → Handle error
-   **`raise`** → Propagate error
-   **Retry** → Try again
-   **Timeout** → Maximum time
-   **Logging** → What happened?
-   **Debugging** → Why did it fail?
-   **Validation** → Is data correct?
-   **Idempotency** → Safe to rerun?
-   **Pipeline** → Orchestrate
-   **Notebook** → Process
-   **Fabric** → `notebookutils`
-   **Databricks** → `dbutils`
-   **Databricks inputs** → Widgets

## 37. Top Interview Priority

`%run` → Parameters → `notebookutils.notebook.run()` →
`notebookutils.notebook.exit()` → `runMultiple()` → `notebookutils.fs` →
Files / Tables / Lakehouse → `%%configure` → Exception / `raise` → Retry
→ Timeout → Logging → Debugging → Validation → Idempotency → Pipeline vs
Notebook → Fabric `notebookutils` vs Databricks `dbutils` → Databricks
Widgets → `mssparkutils` legacy API
