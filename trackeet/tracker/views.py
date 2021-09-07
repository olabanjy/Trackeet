from django.shortcuts import render, reverse, get_object_or_404, redirect
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.core.exceptions import ObjectDoesNotExist
from django.contrib import messages
from django.views.generic import ListView, DetailView, View
from django.core.paginator import Paginator
import pandas as pd
from django.core.mail import EmailMultiAlternatives, send_mail, EmailMessage
from django.urls import reverse
import requests
from isodate import parse_duration
from .forms import *
from .models import *
from  core.models import * 
import xlwt
import csv
from datetime import datetime, date
from django.db.models import Q
import os
from googleapiclient.discovery import build
from django.conf import settings
import uuid 
import requests
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
import random, string
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags

import os

import google_auth_oauthlib.flow
import googleapiclient.errors
from django.core.files import File




api_key = settings.YT_API_KEY

# youtube = build('youtube', 'v3', developerKey=api_key)



def reload_data(request):
    videos = []
    template = 'tracker/search.html'
    if request.method == 'POST':
        search_url = 'https://www.googleapis.com/youtube/v3/search'
        video_url = 'https://www.googleapis.com/youtube/v3/videos'

        search_params = {
            
            'part' : 'snippet,id',
            'q' : request.POST['search'],
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music'
        }

        r = requests.get(search_url, params=search_params)

        results = r.json()['items']

        video_ids = []
        for result in results:
          
            video_ids.append(result['id']['videoId'])

        if request.POST['submit'] == 'lucky':
            return redirect(f'https://www.youtube.com/watch?v={ video_ids[0] }')

        video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 9,
            
        }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']
     
        

        
        for result in results:
            if 'tags' in result['snippet']:
                if "afro pop" in result['snippet']['tags'] or result['snippet']['description']:
                    print("found one")
                    print(result['snippet']['title'])
            
            video_data = {
                'title' : result['snippet']['title'],
                'id' : result['id'],
                'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                'thumbnail' : result['snippet']['thumbnails']['high']['url']
           
            }

            videos.append(video_data)
                
               
    context = {
        'videos' : videos
    }
    return render(request, template, context)




today = date.today()

my_date = datetime.now().strftime('%m_%d_%Y')
today_date = str(my_date)


def custom_search(request):
    videos = []
    template = "tracker/custom_search.html"
    if request.method == 'POST':
        search_url = 'https://www.googleapis.com/youtube/v3/search'
        video_url = 'https://www.googleapis.com/youtube/v3/videos'

        #most popular
        if request.POST['filter'] == 'most_popular':
            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics, topicDetails',
            'chart': 'mostPopular',
            'regionCode' : request.POST['region'],
            'videoCategoryId': 10,
            'maxResults' : 20,
             }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']
           
          
            for result in results:
                
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
                }

                videos.append(video_data)

        #top afropop
        elif request.POST['filter'] == 'top_afropop':
            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics, topicDetails',
            'chart': 'mostPopular',
            'regionCode' : request.POST['region'],
            'videoCategoryId': 10,
            'maxResults' : 20
             }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']
           
 
            for result in results:

                if 'afro' or 'afro pop' or 'afrobeats' in result['snippet']['description']:
                    video_data = {
                        'title' : result['snippet']['title'],
                        'id' : result['id'],
                        'description': result['snippet']['description'],
                        'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                        'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                        'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                        'viewCount': result['statistics']['viewCount'],
                        'likeCount': result['statistics']['likeCount'],
                        'commentCount': result['statistics']['commentCount']
                    }

                    videos.append(video_data)

        #top gospel
        elif request.POST['filter'] == 'top_gospel':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': 'm/02mscn',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)
        # top reggea
        elif request.POST['filter'] == 'reggae':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': '/m/06cqb',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)


        #top popmusic 
        elif request.POST['filter'] == 'pop_music':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': '/m/064t9',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)
        #top jazz
        elif request.POST['filter'] == 'jazz':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': '/m/03_d0',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)
        #top rock 
        elif request.POST['filter'] == 'rock':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': '/m/06by7',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)
        
        #top R&B and Soul
        elif request.POST['filter'] == 'soul':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': '/m/0gywn',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)

        #country music 
        elif request.POST['filter'] == 'country':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': '/m/01lyv',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)
        #top highlife 
        elif request.POST['filter'] == 'top_highlife':
            search_params = {
            'part' : 'snippet,id',
            'q' : ' ',
            'key' : api_key,
            'maxResults' : 20,
            'type' : 'video,music',
            'regionCode' : request.POST['region'],
            'topicId': '/m/03_d0',
            'videoCategoryId': 10,
            }

            r = requests.get(search_url, params=search_params)
            print(r.json())

            results = r.json()['items']

            video_ids = []
            for result in results:
            
                video_ids.append(result['id']['videoId'])

            video_params = {
            'key' : api_key,
            'part' : 'snippet,contentDetails,statistics',
            'id' : ','.join(video_ids),
            'videoCategoryId' : 10,
            'maxResults' : 20,
            
                }

            r = requests.get(video_url, params=video_params)

            results = r.json()['items']

            
           
 
            for result in results:
                video_data = {
                    'title' : result['snippet']['title'],
                    'id' : result['id'],
                    'description': result['snippet']['description'],
                    'url' : f'https://www.youtube.com/watch?v={ result["id"] }',
                    'duration' : int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60),
                    'thumbnail' : result['snippet']['thumbnails']['high']['url'],
                    'viewCount': result['statistics']['viewCount'],
                    'likeCount': result['statistics']['likeCount'],
                    'commentCount': result['statistics']['commentCount']
            
                }

                videos.append(video_data)
       
            
    
    context = {
        'videos' : videos
            }
    return render(request, template, context)


class Dashboard(View):

    def get(self, *args, **kwargs):

        entries = TrackEntries.objects.all()

        template = 'tracker/entries.html'
        
        context = {
            'entries':entries,
         }

        return render(self.request,template)


def export_query(request):
    data = {}
    region = request.GET.get('region', None)
    query = request.GET.get('query', None)
    print(region)
    print(query)

    search_url = 'https://www.googleapis.com/youtube/v3/search'
    video_url = 'https://www.googleapis.com/youtube/v3/videos'
    
    if query == 'most_popular':

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics, topicDetails',
        'chart': 'mostPopular',
        'regionCode' : region,
        'videoCategoryId': 10,
        'maxResults' : 20,
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }
        for result in results:
            
            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
          
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 
            
        

        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com', 'digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

            

    #top afropop
    elif query == 'top_afropop':
        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics, topicDetails',
        'chart': 'mostPopular',
        'regionCode' : region,
        'videoCategoryId': 10,
        'maxResults' : 20
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }
        for result in results:

            if 'afro' or 'afro pop' or 'afrobeats' in result['snippet']['description']:
                video_data['title'].append(result['snippet']['title']) 
                video_data['id'].append(result['id']) 
            
                video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
                video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
                video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
                video_data['viewCount'].append(result['statistics']['viewCount']) 
                video_data['likeCount'].append(result['statistics']['likeCount']) 
                video_data['commentCount'].append(result['statistics']['commentCount']) 
        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com', 'digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

                

    #top gospel
    elif query == 'top_gospel':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 500,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': 'm/02mscn',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }
        for result in results:
            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 
            
        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

            
    # top reggea
    elif query == 'reggae':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 20,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': '/m/06cqb',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }
        for result in results:

            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 
        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com', 'digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

            

            


    #top popmusic 
    elif query == 'pop_music':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 20,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': '/m/064t9',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }


        for result in results:

            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 
        
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com', 'digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

    #top jazz
    elif query == 'jazz':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 20,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': '/m/03_d0',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }

        for result in results:
            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 

        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com', 'digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

    #top rock 
    elif query == 'rock':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 20,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': '/m/06by7',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }

        for result in results:

            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 

        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com', 'digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})
    
    #top R&B and Soul
    elif query == 'soul':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 20,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': '/m/0gywn',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }

        for result in results:
            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 

        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com', 'digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

    #country music 
    elif query == 'country':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 20,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': '/m/01lyv',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }

        for result in results:
            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 

        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com','digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})

    #top highlife 
    elif query == 'top_highlife':
        search_params = {
        'part' : 'snippet,id',
        'q' : ' ',
        'key' : api_key,
        'maxResults' : 20,
        'type' : 'video,music',
        'regionCode' : region,
        'topicId': '/m/03_d0',
        'videoCategoryId': 10,
        }

        r = requests.get(search_url, params=search_params)
        print(r.json())

        results = r.json()['items']

        video_ids = []
        for result in results:
        
            video_ids.append(result['id']['videoId'])

        video_params = {
        'key' : api_key,
        'part' : 'snippet,contentDetails,statistics',
        'id' : ','.join(video_ids),
        'videoCategoryId' : 10,
        'maxResults' : 20,
        
            }

        r = requests.get(video_url, params=video_params)

        results = r.json()['items']

        
        
        video_col = ['title', 'id',  'url', 'duration', 'thumbnail', 'viewCount', 'likeCount','commentCount' ]
        video_data = {
            'title':[], 'id':[], 'url':[], 'duration':[], 'thumbnail':[], 'viewCount':[], 'likeCount':[],'commentCount':[]
        }

        for result in results:
            video_data['title'].append(result['snippet']['title']) 
            video_data['id'].append(result['id']) 
        
            video_data['url'].append(f"https://www.youtube.com/watch?v={result['id'] }") 
            video_data['duration'].append(f"{int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60)} mins") 
            video_data['thumbnail'].append(result['snippet']['thumbnails']['high']['url']) 
            video_data['viewCount'].append(result['statistics']['viewCount']) 
            video_data['likeCount'].append(result['statistics']['likeCount']) 
            video_data['commentCount'].append(result['statistics']['commentCount']) 


        print(video_data)
        try:
            new_entry_df = pd.DataFrame({key:pd.Series(value) for key, value in video_data.items() }, columns=video_col)
            entry_file_name  = f"export_{today_date}_{uuid.uuid1()}.csv"
            entry = new_entry_df.to_csv(entry_file_name, index = False, header=True)
            new_entry_record = TrackEntries(title=result['snippet']['title'], vid_id=result['id'], url=f"https://www.youtube.com/watch?v={ result['id'] }", duration=int(parse_duration(result['contentDetails']['duration']).total_seconds() // 60), thumbnail=result['snippet']['thumbnails']['high']['url'], total_views=result['statistics']['viewCount'], likes=result['statistics']['likeCount'], commentCount=result['statistics']['commentCount'])
            new_entry_record.save()

            with open(entry_file_name, 'rb') as csv:
                new_entry_record.export_file.save( entry_file_name, File(csv))

            subject, from_email, to = 'YT Tracker Export', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com','digger@wyldpytch.com']
            html_content = render_to_string('mail/export.html', {})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            response = requests.get(new_entry_record.export_file.url)
            msg.attach(entry_file_name, response.content, mimetype="text/csv")
            msg.send()
            data.update({'status':True,'msg': 'export file sent to email'})
        except (ValueError, NameError, TypeError) as error:
            err_msg = str(error)
            print(err_msg)
            data.update({'status':False,'msg': 'Error sending file'})

            

        except:
            print("Unexpected error!")
            raise
            data.update({'status':False,'msg': 'Error sending file'})



    
    return JsonResponse(data)

