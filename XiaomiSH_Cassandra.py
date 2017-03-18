#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 02:28:25 2016

@author: psilva
"""

import socket
import binascii
import struct
import json
import datetime
from cassandra.cluster import Cluster

#Connecting to cluster
cluster = Cluster(['192.168.1.81']) #Cluster ip, can be separeted by comma.
session = cluster.connect()
session.set_keyspace('xsh') #Keyspace

#Function to write cassandra cluster.
def wcassandra(sid,action,sensor):
    session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
    VALUES (%s, %s, %s, %s)""", 
    (sid, datetime.datetime.now(), action, sensor))

#Function to read gateway data.
def gateway(x) :
   y = json.loads(x) #Load the file as json.
   #Insert data on keyspace.
   wcassandra(y['sid'],y['cmd'],y['model'])
   print (y," - gateway") 

# Function to read magnet sensor, in my home I'm using  in the door.
def door(x) :
   y = json.loads(x) #Load the file as json.
   print (y," - door")
   w = json.loads(y['data'])
   if ('no_close') in x:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), "still open","magnet"))
   else:
       wcassandra(y['sid'],w['status'],y['model'])

# Function to read button or switch sensor.  
def button(x) :
   y = json.loads(x)
   print (y)
   w = json.loads(y['data'])
   if ('heartbeat') not in x:
       wcassandra(y['sid'],w['status'],y['model'])    
   else:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), 'heartbeat',"switch"))   

# Function to read presence or motion sensor.     
def motion(x) :
   y = json.loads(x)
   print (y)
   if ('no_motion') in x:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), "no_motion","motion"))
   else:
       w = json.loads(y['data'])
       wcassandra(y['sid'],w['status'],y['model'])
   
   
#Starting UDP Multicast 
UDP_IP = "192.168.1.72" #Gateway IP
UDP_PORT_FROM = 54322 
UDP_PORT = 54321
 
MULTICAST_PORT = 9898
SERVER_PORT = 4321
 
MULTICAST_ADDRESS = '224.0.0.50' #Multicast address
SOCKET_BUFSIZE = 1024
MESSAGE = binascii.unhexlify('21310020ffffffffffffffffffffffffffffffffffffffffffffffffffffffff') #Bin message
 
sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
 
sock.bind(("0.0.0.0", MULTICAST_PORT))
#starting socket
mreq = struct.pack("=4sl", socket.inet_aton(MULTICAST_ADDRESS), socket.INADDR_ANY)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFSIZE)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
 
while True: # Starting loop
    data, addr = sock.recvfrom(SOCKET_BUFSIZE) # buffer size is 1024 bytes
    package=data.decode("utf-8") #by deafault data is read as bin , converting bin to string
    
    if 'f0b429b3ce83' in (package): #Gateway SID
        gateway(package)
    elif '158d00010ef090' in (package): #Motion Sensor 
        motion(package)
    elif '158d000111a5a2' in (package): #Door/Windows Sensor
        door(package)
    elif '158d0001157a0b' in (package): #Button sensor
        button(package)
    
