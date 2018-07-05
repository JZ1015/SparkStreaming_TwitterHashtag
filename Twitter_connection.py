import sys

import json
import socket

import tweepy
from tweepy import  OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


class TweetsListener(StreamListener):
    
    def __init__(self,socket):
        print 'Tweet listener initialized'
        self.client_socket=socket
        
    
    def on_data(self,data):
        try:
            jsonMessage=json.loads(data)
            message=jsonMessage['text'].encode('utf-8')
            print message
            self.client_socket.send(message)
        except BaseException as e:
            print('Error on_data: %s' % str(e))
        return True
    
    def on_error(self,status):
        print status
        return True 
    
def connect_to_twitter(connection):
    

    api_key='jRfKXtfRVdSvat2jI4qXLxv7B'
    api_secret='aunJtVdaislHj8BTpWrmUSJIQP1kTCFiF5deZCztOiqI7tM2EV'
    
    access_token='3071663652-v5xNv6RphahVIiWlP4DpuVbJj94efpf2cyiuU2n'
    access_token_secret='gHi1eo6WiGnURw5Q1jZZ0BKHQQv15YBhWdlDyscCoRHBN'
    
    auth=OAuthHandler(api_key,api_secret)
    auth.set_access_token(access_token,access_token_secret)
    
    twitter_stream=Stream(auth,TweetsListener(connection))
    twitter_stream.filter(track=['#'])
    

if __name__=='__main__':
    s=socket.socket()
    host='localhost'
    port=7777
    s.bind((host,port))
    
    print('Listening on port: %s' % str(port))
    
    s.listen(5)
    
    connection,client_address=s.accept()
    
    print ('Received request from : '+str(client_address))
    
    connect_to_twitter(connection)
    
    
    