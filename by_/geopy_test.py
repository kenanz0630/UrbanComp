
from geopy.distance import great_circle

coordi1=(40.4446,-79.9527)
coordi2=(40.4511,-79.9348)
coordi3=(40.4457,-79.9538)

print great_circle(coordi1,coordi3).meters
'''
-79.9527	40.4446
-79.9538	40.4457
-79.9566	40.4419
-79.9603	40.4309
'''
