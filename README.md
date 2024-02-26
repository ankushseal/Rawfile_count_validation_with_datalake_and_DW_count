## Healthcare Data Analysis with AWS Services

This repository contains a Python script for analyzing healthcare data using various AWS services including Amazon S3, Amazon Athena, and PostgreSQL. The script retrieves data from different sources, performs analysis, and sends a summary report via email.

### Prerequisites

Before running the script, ensure you have the following dependencies installed:

- Python 3.x
- `boto3`
- `pandas`
- `psycopg2`
- `environ`
- `pandasql`
- `pretty_html_table`

You can install the dependencies via pip:

```bash
pip install boto3 pandas psycopg2 environ pandasql pretty_html_table
```

### Configuration

Ensure you have configured your AWS credentials properly. You can set up your AWS credentials using AWS CLI or directly in the script.

### Usage

1. Clone the repository:

```bash
git clone https://github.com/your-username/healthcare-aws-analysis.git
cd healthcare-aws-analysis
```

2. Update the `config.json` file with your AWS credentials and other necessary configurations.

3. Run the Python script `healthcare_analysis.py`:

```bash
python healthcare_analysis.py
```

### Description

This script performs healthcare data analysis using the following steps:

1. **Amazon S3 Interaction**: Connects to Amazon S3 using the `boto3` library to retrieve data files.

2. **File Count**: Counts the number of rows in each data file and prints the results.

3. **Athena Interaction**: Utilizes Amazon Athena to query data snapshots and counts the records in each snapshot.

4. **PostgreSQL Interaction**: Connects to a PostgreSQL database to count records in landing tables.

5. **Email Notification**: Generates a summary report containing file counts, Athena counts, and PostgreSQL counts. Sends the report via email using SMTP.

### Input Data

The input data consists of CSV files stored in an Amazon S3 bucket and snapshots in Amazon Athena.

### Output

The output is a summary report sent via email, containing the counts of records in data files, Athena snapshots, and PostgreSQL landing tables.

### Contributing

Contributions are welcome! If you have suggestions, feature requests, or bug fixes, please feel free to open an issue or create a pull request.


### Acknowledgements

- [boto3](https://github.com/boto/boto3) - AWS SDK for Python.
- [pandas](https://github.com/pandas-dev/pandas) - Python data analysis library.
- [psycopg2](https://github.com/psycopg/psycopg2) - PostgreSQL adapter for Python.
- [pretty-html-table](https://github.com/dexplo/pretty-html-table) - Python library for generating HTML tables.


### workflow :
1st we download all the daily files from s3 à Then we count the rows of all the files and mention those counts in an Excel spreadsheet à Then we take Athena count for that day’s snapshot and mention those counts in that spreadsheet à And after Integration job completion we take the landing table count and mention those in that spreadsheet à At the end we send a mail with those details in tabular format.

 This python framework which can help us to achieve automation of that upper manual workflow. By that script :-

Count the records of daily coming files in S3 buckets without downloading the files.
Taking the Athena count of that file’s snapshots without querying Athena manually.
Collecting the landing table count without querying manually.
Sending a mail with all the count in tabular format automatically.
 

For testing purpose, I have used dummy customer.txt and order.txt file.
PFB some screenshots for better understanding. Please let me know if I need to provide a demo for better understanding.
S3 path details :

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/dea31b06-a6c1-4c25-a713-665ecb5834df)

Customer folder :

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/f2c02a32-800d-479b-8fb3-80bb03fdee3d)

Order folder:

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/37d9cec1-003d-4c1b-a88e-41e17d0b3296)

File Description:

Customer File:

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/b272869d-2f08-4eda-8f1f-6e183035f8f7)

Order File:

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/f77a85aa-de26-49e5-a488-37f1fffbf4fc)

In customer file 654 rows and order file we have 60918 rows.

 Athena Count :

Customer :

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/f387baa7-f8ae-4beb-a7ed-390d74ee0281)

Orders:

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/b8d0162d-de82-4d7a-8b92-f9b370d57363)

Landing Table :

Customer:

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/b4f0c95a-851b-472c-9f7f-2df9a3227e30)

Orders:

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/0e45b6c5-3565-4c24-a0b9-b7bb3ebb2967)


Script’s output:

![image](https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/39130e8e-6635-4f67-9413-673ee791ce1f)

Mail :

<img width="466" alt="image" src="https://github.com/ankushseal/Rawfile_count_validation_with_datalake_and_DW_count/assets/65338558/f67c07eb-dc53-414e-a320-1ee81545114b">

