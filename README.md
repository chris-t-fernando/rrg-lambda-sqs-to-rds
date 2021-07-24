# rrg-lambda-sqs-to-rds

This lambda function is added as a trigger on an SQS queue.

The SQS queue contains stock quote objects published by the stock_scraper project in my rrg repo.

The lambda function validates that the quote does not already exist, and then inserts it into the RDS database used by the project.

To do:
1. Handle sector quotes
1. Better error handling
1. Automatically turn on RDS if its turned off (cost saving)
1. Better handling of the DLQ
1. Rename stuff and general cleanup - was originally created from boilerplate helloworld code
