import sys,os
import os
#import pytz #requires pytz to be installed
import random
from datetime import datetime
from datetime import timedelta

dir = os.path.dirname(__file__)


time_add = 0

service_type_tp = ("GET_INTERVIEW", "GET_RIDE", "GET_PULSA")
service_area_name_tp = ("JARKARTA", "BANGKOK", "HCMC")
payment_type_tp = ("GET_CASH", "CREDITCARD", "PAYPAL")
status_tp = ("COMPLETED", "CANCELLED", "ACTIVE")


for x in range(0, 180):

    r1 = random.randint(0, 2)
    r2 = random.randint(0, 2)
    r3 = random.randint(0, 2)
    r4 = random.randint(0, 2)

    service_type = service_type_tp[r1]
    service_area_name = service_area_name_tp[r2]
    payment_type = payment_type_tp[r3]
    status = status_tp[r4]

    filename = os.path.join(dir, 'input', 'input_' + str(x) + '.json')
    # add some seconds to make sure event time is after processing time of beam pipeline
    currentTime = datetime.utcnow() + timedelta(minutes=5) + timedelta(minutes=time_add)
    currentTimeString = str(currentTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]) + "Z"

    f= open(filename, "a")
    f.write("{\"order_number\": \"AP-1\", \"service_type\": \"" + service_type + "\", \"driver_id\": \"driver-123\", \"customer_id\": \"customer-123\", \"service_area_name\": \"" + service_area_name + "\", \"payment_type\": \"" + payment_type + "\", \"status\": \"" + status + "\", \"event_timestamp\": \"" + currentTimeString + "\"}")
    f.close()

    time_add+=1
