workflow :
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

