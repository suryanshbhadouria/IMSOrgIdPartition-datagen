import logging, gzip, datetime, shutil


def generateData(filename):
    logging.info("creating file:" + str(filename))
    myfile = open(filename, 'w')
    line1 = '{"isSeedMember":false,"isABTest":false,"campaignId":40231,"isQuarantine":false,"ip":"109.103.105.150","isNegated":true,"eventCount":-1,"eventType":"DELIVERY","deliveryId":56768,"instanceId":"instance_id_T2DLw0I5cx_40","messageType":0,"isBlacklisted":null,"IMSOrgId":"XzLajk7IMQ1Sxrmdg48VKlv40@AdobeOrg","domain":"domain_1768255","failureReason":0,"variant":"F","failureType":0,"recipientId":92704,"broadlogId":'
    line2 = ',"senderDomain":"WUPfkSevaj142@mKBvAjkH.Xe","timestamp":"1478059226000","pushPlatform":null,"mobileApp":null,"offerId":null,"offerPlacementId":null,"offerActivityId":null,"offerType":0,"status":1,"deviceIP":"","trackingUrlType":"","trackingUrl":"","platform":"","browser":"","device":""}'
    count = 0
    while count < 3221225471:
        event_str = line1 + str(count) + line2
        myfile.write("%s\n" % event_str)
        count += 1
    logging.info("Data generation completed")


def compress(path, filename):
    with open(path + filename, 'rb') as f_in:
        with gzip.open(path + filename + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


logging.basicConfig(filename='dataGen.log', level=logging.DEBUG)
path = '/home/suryansh/PyCharmProjects/IMSOrgIdPartition-datagen/'
filename = "m1.json"
start = datetime.datetime.now().timestamp()
generateData(filename)
compress(path, filename)
end = datetime.datetime.now().timestamp()
logging.debug("time taken:" + str(end - start) + "seconds")
