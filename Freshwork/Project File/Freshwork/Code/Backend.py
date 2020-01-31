
from threading import*
import time
import json
import os


data={}
data_json={}
dj={}

#Read data from the File
def file_read():
    fr = open("test.txt", 'r')
    if os.stat('test.txt').st_size == 0:
        pass
    else:
        temp = fr.read()
        dj = json.loads(temp)
        return dj
    fr.close()
#Write data to the File
def file_write(dj):
    fw = open("test.txt", 'w')
    temp = json.dumps(dj)
    fw.write(temp)
    fw.close()


#Key_value pair validation and insert to the FIle
def create_data(key,value,timeout=0):
    fw = open("test.txt", 'w')

    if (key in data):
        print("Error:"+key+" already exists")
    else:
        if(key.isalpha()):
            if len(data)<(1024*1024*1020) and value<=(16*1024*1024): #Check the data size and value size
                if timeout==0:
                    v=[value,timeout]
                else:
                    v=[value,time.time()+timeout]
                if len(key)<=32:
                    data[key]=v
                    data_json=json.dumps(data)
                    fw.write(data_json)
                    fw.close()

            else:
                print("Error: Key value more than 32 chars or value greater than 16KB! ")
        else:
            print("Error: Invalind key! key_name must contain only alphabets and no special characters or numbers")


#Read data
def read_data(key):
    file_read()
    dictr={}
    if key not in data and key not in dj:
        print("Error:"+ key +" does not exist in File. Please enter a valid key")
    else:
        a=data[key]
        if a[1]!=0:
            if time.time()<a[1]:
                dictr[key] = a[0]
                js = json.dumps(dictr)
                print(js)
            else:
                print("Error: Time-to-Live of ",key," has expired")
        else:

            dictr[key]=a[0]
            js = json.dumps(dictr)
            print(js)

#Delete Data from file

def delete_data(key):
    file_read()
    if key not in data and key not in dj:
        print("Error: "+key+" does not exist in File. Please enter a valid key")
    else:
        a=data[key]
        if a[1]!=0:
            if time.time()<a[1]:
                del data[key]
                file_write(data)
                print(key+" deleted successfully ")
            else:
                print("Error: Time-to-Live of ",key," has expired")
        else:
            del data[key]
            file_write(data)
            print(key + " deleted successfully ")


#Modify the data
def modify_data(key,value):
    a=data[key]
    if a[1]!=0:
        if time.time()<a[1]:
            file_read()
            if key not in data and key not in dj:
                print("Error: "+key+" does not exist in File. Please enter a valid key")
            else:
                v=[]
                v.append(value)
                v.append(a[1])
                data[key]=v
                file_write(data)
        else:
            print("Error: Time-to-Live of ",key," has expired")
    else:
        file_read()
        if key not in data and key not in dj:
            print("Error: "+key+" does not exist in File. Please enter a valid key")
        else:
            v=[]
            v.append(value)
            v.append(a[1])
            data[key]=v
            file_write(data)


#Threading
def create(key,value,timeout=0):
    t1=Thread(target=create_data,args=(key,value,timeout), daemon=True)
    t1.start()

def modify(key,value):
    t2=Thread(target=modify_data,args=(key,value), daemon=True)
    t2.start()
def read(key):
    t3=Thread(target=read_data,args=(key,))
    t3.start()
    t3.join()
def delete(key):
    t4=Thread(target=delete_data,args=(key,),daemon=True)
    t4.start()