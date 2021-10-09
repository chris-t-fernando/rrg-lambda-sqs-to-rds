import json
import boto3
import logging, sys
from stockobjects.sector import Sector
from stockobjects.company import Company
from stockobjects.sectorcollection import SectorCollection


if __name__ == "__main__":
    import pymysql as MySQLdb
    import pymysql.cursors as cursors
else:
    import MySQLdb.cursors


logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# handler for pulling config from SSM
def getSSMParameter(ssm, path, encrypted=False):
    return (
        ssm.get_parameter(Name=path, WithDecryption=encrypted)
        .get("Parameter")
        .get("Value")
    )


def processStockQuotes(listOfQuotes, cursor):
    insertQuotes = []
    sqlSelect = "select count(*) from weekly_stock_quotes where stock_code = %s and quote_date = %s"

    for x in listOfQuotes["quoteObject"]:
        sqlParameters = (
            x["stock_code"],
            x["quote_date"],
        )
        cursor.execute(sqlSelect, sqlParameters)
        result = cursor.fetchone()

        if result["count(*)"] == 0:
            logging.info(
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
                f"Quote already exists! Skipping - stock_code: {x['stock_code']} quote for date: {x['quote_date']}"
            )

    return insertQuotes


def processSectorQuotes(listOfQuotes, cursor):
    insertQuotes = []
    sqlSelect = "select count(*) from weekly_sector_quotes where sector_code = %s and quote_date = %s"

    for x in listOfQuotes["quoteObject"]:
        sqlParameters = (
            x["sector_code"],
            x["quote_date"],
        )
        cursor.execute(sqlSelect, sqlParameters)
        result = cursor.fetchone()

        if result["count(*)"] == 0:
            logging.info(
                f"Queuing for insert: sector_code: {x['sector_code']} quote for date: {x['quote_date']}"
            )
            insertQuotes.append(
                (
                    x["quote_date"],
                    x["sector_code"],
                    x["open"],
                    x["high"],
                    x["low"],
                    x["close"],
                    x["volume"],
                )
            )

        else:
            logging.warning(
                f"Quote already exists! Skipping - sector_code: {x['sector_code']} quote for date: {x['quote_date']}"
            )

    return insertQuotes


def doInsert(insertQuotes, cursor, messageType):
    sqlInsert = (
        "insert into weekly_"
        + messageType
        + "_quotes (quote_date, "
        + messageType
        + "_code, open_price, high_price, low_price, close_price, volume) values (%s, %s, %s, %s, %s, %s, %s)"
    )

    for y in insertQuotes:
        try:
            cursor.execute(sqlInsert, y)
            logging.info(
                f"Successfully inserted {messageType} quote for {y[1]} on date {y[0]}"
            )

        except Exception as e:
            logging.error(
                f"Failed to insert {messageType} quote for {y[1]} on date {y[0]}: {str(e)}"
            )
            raise


def lambda_handler(event, context):
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    # set up boto SSM
    # ssmClient = boto3.client("ssm", verify=False)
    ssmClient = boto3.client("ssm")
    print(json.dumps(event))

    connection = MySQLdb.connect(
        host=getSSMParameter(ssm=ssmClient, path="/rrg-creator/rds-endpoint"),
        user=getSSMParameter(ssm=ssmClient, path="/rrg-creator/rds-user"),
        password=getSSMParameter(
            ssm=ssmClient, path="/rrg-creator/rds-password", encrypted=True
        ),
        database=getSSMParameter(ssm=ssmClient, path="/rrg-creator/rds-database"),
        cursorclass=MySQLdb.cursors.DictCursor,
    )

    if not "Records" in event.keys():
        logging.error("No Records key in event.  Failing")
        raise Exception("No Records key in event.  Failing")

    with connection:
        with connection.cursor() as cursor:
            for record in event["Records"]:
                try:
                    messageType = record["messageAttributes"]["QuoteType"][
                        "stringValue"
                    ]

                    if messageType != "stock" and messageType != "sector":
                        # malformed messageAttribute
                        logging.error(
                            f"quoteType is invalid.  Expected either 'sector' or 'stock', instead found {messageType}.  Failing"
                        )
                        raise Exception(
                            f"quoteType is invalid.  Expected either 'sector' or 'stock', instead found {messageType}.  Failing"
                        )

                except Exception as e:
                    logging.error("Unable to find quoteType in message.  Failing")
                    logging.error(str(e))
                    raise Exception("Unable to find quoteType in message.  Failing")

                listOfQuotes = json.loads(record["body"])
                if not "quoteObject" in listOfQuotes.keys():
                    logging.error("Unable to find quoteObject in payload")
                    raise Exception("Unable to find quoteObject in payload")

                if messageType == "stock":
                    insertQuotes = processStockQuotes(
                        listOfQuotes=listOfQuotes, cursor=cursor
                    )

                elif messageType == "sector":
                    insertQuotes = processSectorQuotes(
                        listOfQuotes=listOfQuotes, cursor=cursor
                    )

                # todo: something clever with failures
                doInsert(
                    insertQuotes=insertQuotes, cursor=cursor, messageType=messageType
                )

                connection.commit()

    return {"statusCode": 200, "body": json.dumps("Success")}


if __name__ == "__main__":
    # stock
    lambda_handler(
        {
            "Records": [
                {
                    "messageId": "7722ee10-38da-4366-870f-181c75d66209",
                    "receiptHandle": "AQEB3NdXCSeIT6lMwx4IMpn/2FwXZSlbioifULcR217lcYz5rHw7uJqV/0DPxIkrqOmhaC412x3WIhcn5XyMwGv6ozOTpCvXEx21rMjv5TXudMmxZiDtlVQd89uEJKLGgfwnNBv2i3fCEk+GT3ik5J1yab42UTj6C5JOlN/SosnuoQOb5LPZ/TY/W296ZQjDGIS3XyBkrFAVbBNS+cCVE+L9j1n3Bx3ITIKYvGr9PMYbd6xmVSisPkj66kFGEKbSYOU+JUycjWBhUfu2pDvILnfuLCJgJvNqr18QWzbx0kbhUOdFsY4LsQGqJpSm8wA6JfamKHmcAStatZa48yYo5Uvrqu7gWKKu5SkCx9s7m4QyR/MJ2QZ/RWKFHsao4LdSKQct/eKxUZtxZCQIVBSXmAGOXA==",
                    "body": '{"quoteObject": [{"quote_date": "2021-07-12", "stock_code": "29m", "open": 2.0999999046325684, "high": 2.380000114440918, "low": 2.069999933242798, "close": 2.319999933242798, "volume": 10737768}, {"quote_date": "2021-07-19", "stock_code": "29m", "open": 2.3299999237060547, "high": 2.3299999237060547, "low": 2.140000104904175, "close": 2.299999952316284, "volume": 8542782}, {"quote_date": "2021-07-26", "stock_code": "29m", "open": 2.3399999141693115, "high": 2.380000114440918, "low": 2.240000009536743, "close": 2.2799999713897705, "volume": 6614856}, {"quote_date": "2021-08-02", "stock_code": "29m", "open": 2.319999933242798, "high": 2.5799999237060547, "low": 2.2799999713897705, "close": 2.5199999809265137, "volume": 4730473}, {"quote_date": "2021-08-09", "stock_code": "29m", "open": 2.549999952316284, "high": 2.569999933242798, "low": 2.369999885559082, "close": 2.509999990463257, "volume": 8268355}, {"quote_date": "2021-08-16", "stock_code": "29m", "open": 2.5199999809265137, "high": 2.559999942779541, "low": 2.2100000381469727, "close": 2.259999990463257, "volume": 9058240}, {"quote_date": "2021-08-23", "stock_code": "29m", "open": 2.2699999809265137, "high": 2.5199999809265137, "low": 2.259999990463257, "close": 2.359999895095825, "volume": 3568918}, {"quote_date": "2021-08-30", "stock_code": "29m", "open": 2.430000066757202, "high": 2.450000047683716, "low": 2.3399999141693115, "close": 2.4200000762939453, "volume": 2003744}, {"quote_date": "2021-09-06", "stock_code": "29m", "open": 2.4200000762939453, "high": 2.7200000286102295, "low": 2.3499999046325684, "close": 2.4600000381469727, "volume": 2466997}, {"quote_date": "2021-09-13", "stock_code": "29m", "open": 2.4800000190734863, "high": 2.640000104904175, "low": 2.4100000858306885, "close": 2.4600000381469727, "volume": 5061766}, {"quote_date": "2021-09-20", "stock_code": "29m", "open": 2.4800000190734863, "high": 2.5, "low": 2.2699999809265137, "close": 2.4000000953674316, "volume": 2213895}, {"quote_date": "2021-09-24", "stock_code": "29m", "open": 2.490000009536743, "high": 2.5, "low": 2.380000114440918, "close": 2.4000000953674316, "volume": 202455}]}',
                    "attributes": {
                        "ApproximateReceiveCount": "4",
                        "SentTimestamp": "1627288425551",
                        "SenderId": "AIDAJBRAAZQ4REFJVXYLQ",
                        "ApproximateFirstReceiveTimestamp": "1627288425551",
                    },
                    "messageAttributes": {
                        "QuoteType": {
                            "stringValue": "stock",
                            "stringListValues": [],
                            "binaryListValues": [],
                            "dataType": "String",
                        }
                    },
                    "md5OfMessageAttributes": "fd9611cd8619b84e072c6770857bf92c",
                    "md5OfBody": "4e243d74ebe4f67ef8622b579d1cc105",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-west-2:036372598227:quote-updates",
                    "awsRegion": "us-west-2",
                },
            ]
        },
        "",
    )

    # sector
    lambda_handler(
        {
            "Records": [
                {
                    "messageId": "7722ee10-38da-4366-870f-181c75d66209",
                    "receiptHandle": "AQEB3NdXCSeIT6lMwx4IMpn/2FwXZSlbioifULcR217lcYz5rHw7uJqV/0DPxIkrqOmhaC412x3WIhcn5XyMwGv6ozOTpCvXEx21rMjv5TXudMmxZiDtlVQd89uEJKLGgfwnNBv2i3fCEk+GT3ik5J1yab42UTj6C5JOlN/SosnuoQOb5LPZ/TY/W296ZQjDGIS3XyBkrFAVbBNS+cCVE+L9j1n3Bx3ITIKYvGr9PMYbd6xmVSisPkj66kFGEKbSYOU+JUycjWBhUfu2pDvILnfuLCJgJvNqr18QWzbx0kbhUOdFsY4LsQGqJpSm8wA6JfamKHmcAStatZa48yYo5Uvrqu7gWKKu5SkCx9s7m4QyR/MJ2QZ/RWKFHsao4LdSKQct/eKxUZtxZCQIVBSXmAGOXA==",
                    "body": '{"quoteObject": [{"quote_date": "2021-03-29", "sector_code": "xij", "open": 0.032999999821186066, "high": 0.032999999821186066, "low": 0.032999999821186066, "close": 0.032999999821186066, "volume": 0}]}',
                    "attributes": {
                        "ApproximateReceiveCount": "4",
                        "SentTimestamp": "1627288425551",
                        "SenderId": "AIDAJBRAAZQ4REFJVXYLQ",
                        "ApproximateFirstReceiveTimestamp": "1627288425551",
                    },
                    "messageAttributes": {
                        "QuoteType": {
                            "stringValue": "sector",
                            "stringListValues": [],
                            "binaryListValues": [],
                            "dataType": "String",
                        }
                    },
                    "md5OfMessageAttributes": "fd9611cd8619b84e072c6770857bf92c",
                    "md5OfBody": "4e243d74ebe4f67ef8622b579d1cc105",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-west-2:036372598227:quote-updates",
                    "awsRegion": "us-west-2",
                }
            ]
        },
        "",
    )
