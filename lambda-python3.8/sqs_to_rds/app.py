from __future__ import print_function
import json
import boto3
import pymysql.cursors
import logging, sys

# handler for pulling config from SSM
def getSSMParameter(ssm, parameterPath, encryptionOption=False):
    return (
        ssm.get_parameter(Name=parameterPath, WithDecryption=encryptionOption)
        .get("Parameter")
        .get("Value")
    )


def lambda_handler(event, context):
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    # set up boto SSM
    ssmClient = boto3.client("ssm")

    connection = pymysql.connect(
        host=getSSMParameter(ssmClient, "/rrg-creator/rds-endpoint"),
        user=getSSMParameter(ssmClient, "/rrg-creator/rds-user"),
        password=getSSMParameter(ssmClient, "/rrg-creator/rds-password", True),
        database=getSSMParameter(ssmClient, "/rrg-creator/rds-database"),
        cursorclass=pymysql.cursors.DictCursor,
    )

    sqlSelect = "select count(*) from weekly_stock_quotes where stock_code = %s and quote_date = %s"

    insertQuotes = []

    if not "Records" in event.keys():
        logging.error("Weird payload, failing")
        raise Exception("Weird payload, failing")

    with connection:
        with connection.cursor() as cursor:
            for record in event["Records"]:
                listOfQuotes = json.loads(record["body"])
                if not "quoteObject" in listOfQuotes.keys():
                    logging.error("Unable to find quoteObject in payload")
                    raise Exception("Unable to find quoteObject in payload")

                for x in listOfQuotes["quoteObject"]:
                    sqlParameters = (
                        x["stock_code"],
                        x["quote_date"],
                    )
                    cursor.execute(sqlSelect, sqlParameters)
                    result = cursor.fetchone()
                    # print(result["count(*)"])
                    if result["count(*)"] == 0:
                        logging.warning(
                            f"Queuing for insert: stock_code: {x['stock_code']} quote for date: {x['quote_date']}"
                        )
                        insertQuotes.append(
                            (
                                x["quote_date"],
                                x["stock_code"],
                                x["open"],
                                x["high"],
                                x["low"],
                                x["close"],
                                x["volume"],
                            )
                        )

                    else:
                        logging.warning(
                            f"Skipping stock_code: {x['stock_code']} quote for date: {x['quote_date']}"
                        )

            sqlInsert = "insert into weekly_stock_quotes (quote_date, stock_code, open_price, high_price, low_price, close_price, volume) values (%s, %s, %s, %s, %s, %s, %s)"

            try:
                for y in insertQuotes:
                    cursor.execute(sqlInsert, y)

                connection.commit()

                logging.warning("Successfully inserted quotes")
                return {"statusCode": 200, "body": json.dumps("Success")}
            except Exception as e:
                logging.error(f"Failed to insert: {str(e)}")
                raise
