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

def gateway(x) :
   y = json.loads(x) #Load the file as json.
   #Insert data on keyspace.
   session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
    VALUES (%s, %s, %s, %s)""", 
   (y['sid'], datetime.datetime.now(), y['cmd'],"Gateway"))
   
   print (y,"gateway") 


def door(x) :
   y = json.loads(x) #Load the file as json.
   w = json.loads(y['data'])
   print (y,"door")
   session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
    VALUES (%s, %s, %s, %s)""", 
   (y['sid'], datetime.datetime.now(), w['status'],"Door"))
   
   
   
def button(x) :
   y = json.loads(x)
   print (y)
   #there is a trick where heartbeat button has no 'status' information.
   if ('heartbeat') not in x:
       w = json.loads(y['data'])
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), w['status'],"Button"))       
   else:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), 'heartbeat',"Button"))   

   print (y,"button") 

def motion(x) :
   y = json.loads(x)
   w = json.loads(y['data'])
   #there is a trick where data = 'no_motion', no status information
   if ('no_motion') in w:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), "no_motion","Motion"))
   else:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), w['status'],"Motion"))
       print (y)
   print (y,"motion")
   
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
    data1=data.decode("utf-8") #converting bin to string
    if 'f0b429b3ce83' in (data1): #Gateway SID
        gateway(data1)
    elif '158d00010ef090' in (data1): #Motion Sensor 
        motion(data1)
    elif '158d000111a5a2' in (data1): #Door/Windows Sensor
        door(data1)
    elif '158d0001157a0b' in (data1): #Button sensor
        button(data1)
    
    
                
