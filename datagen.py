import logging, gzip, datetime, shutil, os

from s3upload import upload_to_s3


def generateData(filename, count):
    logging.info("creating file:" + str(filename))
    myfile = open(filename, 'w')
    line1 = '{"isSeedMember":false,"isABTest":false,"campaignId":40231,"isQuarantine":false,"ip":"109.103.105.150","isNegated":true,"eventCount":-1,"eventType":"DELIVERY","deliveryId":56768,"instanceId":"instance_id_T2DLw0I5cx_40","messageType":0,"isBlacklisted":null,"IMSOrgId":"XzLajk7IMQ1Sxrmdg48VKlv40@AdobeOrg","domain":"domain_1768255","failureReason":0,"variant":"F","failureType":0,"recipientId":92704,"broadlogId":'
    line2 = ',"senderDomain":"WUPfkSevaj142@mKBvAjkH.Xe","timestamp":"1478059226000","pushPlatform":null,"mobileApp":null,"offerId":null,"offerPlacementId":null,"offerActivityId":null,"offerType":0,"status":1,"deviceIP":"","trackingUrlType":"","trackingUrl":"","platform":"","browser":"","device":""}'
    limit = count + 10737419
    while count < limit:
        event_str = line1 + str(count) + line2
        myfile.write("%s\n" % event_str)
        count += 1
    logging.info("Data generation completed")
    return count


def compress(path, filename):
    with open(path + filename, 'rb') as f_in:
        with gzip.open(path + filename + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


logging.basicConfig(filename='dataGen.log', level=logging.DEBUG)
path = '/home/suryansh/PycharmProjects/IMSOrgIdPartition-datagen/'
AWS_ACCESS_KEY = 'AKIAJYJVVJLX6D2SA'
AWS_ACCESS_SECRET_KEY = 'nCR8odDaNl3YKGowQxJsA19k7UNm/cZxTJ3'
bucket = 'acs-hadoop-ingestion-poc'
i = 0
count = 0
while i < 200:
    start = datetime.datetime.now()
    logging.debug("Old count:" + str(count))
    filename = "m" + str(i) + ".json"
    count = generateData(filename, count)
    logging.debug("New count:" + str(count))
    compress(path, filename)
    end = datetime.datetime.now()
    # upload to aws
    file = open(filename + '.gz', 'r+')
    key = 'CAMP-29129-1/' + file.name
    if upload_to_s3(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY, file, bucket, key):
        print
        logging.debug("uploaded file" + str(key))
    else:
        print
        logging.debug('The upload failed for' + str(key))
    os.remove(path + filename)
    os.remove(path + filename + '.gz')
    logging.debug("[Iteration " + str(i) + "]time taken:" + str(end - start) + "seconds")
    i = i + 1
