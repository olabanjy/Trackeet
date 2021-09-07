from .models import *
import pandas as pd
from django.core.mail import send_mail
from datetime import date
from celery import shared_task
from django.core.files import File
import uuid 
import random, string
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from datetime import datetime
import numpy as np 


today = date.today()



my_date = datetime.now().strftime('%m_%d_%Y')
today_date = str(my_date)



pd.set_option('precision',15)

def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

############################
#     ALBUM                 ######
############################
@shared_task
def process_album(pk):
    album_file = ProcessedDocument.objects.get(pk=pk)
    try:
        #col_list = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album = pd.read_excel(album_file.file_doc, Index=None).fillna('')
        album.columns = album.columns.str.strip().str.lower()
        if 'product upc' not in album:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {album_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['shola.albert@gmail.com', 'digger@wyldpytch.com'],
                            fail_silently=False,
                            )
        else:
            pass
        for index, row in album.iterrows():
            print(row['product upc'])
            check_album = Album.objects.filter(product_upc = row['product upc']).first()
            if check_album:
                check_album.product_title = row['product title']
                check_album.genre = row['genre']
                check_album.release_date = row['release date']
                check_album.total_volume = row['total volumes']
                check_album.imprint_label = row['imprint label']
                check_album.product_artist = row['product artist']
                check_album.artist_url = row['artist url']
                check_album.catalog_number = row['catalog number']
                check_album.manufacture_upc = row['manufacturer upc']
                check_album.deleted = row['deleted?']
                check_album.total_tracks = row['total tracks']
                check_album.cline = row['cline']
                check_album.territories_carveouts = row['territories carveouts']
                check_album.master_carveouts = row['master carveouts']
                check_album.save()
            else:
                new_album = Album(product_title = row['product title'], product_upc = row['product upc'], genre = row['genre'], release_date = row['release date'], total_volume = row['total volumes'], imprint_label = row['imprint label'], product_artist = row['product artist'], artist_url = row['artist url'], catalog_number = row['catalog number'], manufacture_upc = row['manufacturer upc'], deleted = row['deleted?'], total_tracks = row['total tracks'], cline = row['cline'], territories_carveouts = row['territories carveouts'], master_carveouts =row['master carveouts'], processed_day=today)
                new_album.save()
                new_master = Master_DRMSYS(title = new_album.product_title, display_upc = new_album.product_upc, genre = new_album.genre, release_date = new_album.release_date, total_volumes = new_album.total_volume, label_name = f"{new_album.imprint_label}", artist = new_album.product_artist, artist_url = new_album.artist_url, label_catalogno = new_album.catalog_number, manufacturer_upc = new_album.manufacture_upc, deleted = new_album.deleted, total_tracks = new_album.total_tracks, c_line = new_album.cline, territories_carveouts = new_album.territories_carveouts, master_carveouts= new_album.master_carveouts, album = new_album, data_type = 'Album', zIDKey_UPCISRC = f"{new_album.product_upc}-{new_album.product_upc}", processed_day=today)
                new_master.save()
        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {album_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        [ 'shola.albert@gmail.com', 'digger@wyldpytch.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {album_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['shola.albert@gmail.com', 'digger@wyldpytch.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise




############################
#     TRACK                 #######
############################
@shared_task
def process_track(pk):
    track_file = ProcessedDocument.objects.get(pk=pk)
    try:
        #col_list = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track = pd.read_excel(track_file.file_doc, Index=None).fillna('')
        track.columns = track.columns.str.strip().str.lower()
        if 'isrc' not in track:
            send_mail(
                        'Corrupted File',
                        f''' 
                        Hello Admin 
                        The file {track_file.file_name} was corrupt !
                        Pls login to run it again
                            ''',
                        'Trackeet Recording Panel ',
                        ['shola.albert@gmail.com', 'digger@wyldpytch.com'],
                        fail_silently=False,
                        )
        else:
            pass
        for index, row in track.iterrows():
            check_track = Track.objects.filter(isrc=row['isrc']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists. updating now ")
                check_track.display_upc = row['displayupc'],
                check_track.volume_no = row['volumeno'],
                check_track.track_no = row['trackno'],
                check_track.hidden_track = row['hiddentrack'],
                check_track.title = row['title'],
                check_track.song_version = row['songversion'],
                check_track.genre  = row['genre'],
                check_track.track_duration  = row['trackduration'],
                check_track.preview_clip_start_time  = row['previewclipstarttime'],
                check_track.preview_clip_duration = row['previewclipduration'],
                check_track.p_line = row['p_line'],
                check_track.recording_artist = row['recording artist'],
                check_track.artist  = row['artist'],
                check_track.release_name = row['release name'],
                check_track.parental_advisory = row['parentaladvisory'],
                check_track.label_name = row['labelname'],
                check_track.producer = row['producer'],
                check_track.publisher = row['publisher'],
                check_track.writer = row['writer'],
                check_track.arranger = row['arranger'],
                check_track.territories = row['territories'],
                check_track.exclusive = row['exclusive'],
                check_track.wholesale_price = row['wholesaleprice'],
                check_track.download = row['download'],
                check_track.sales_start_date = row['salesstartdate'],
                check_track.sales_end_date = row['salesenddate'],
                check_track.error = row['error']
                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_upc= row['displayupc']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    new_track = Track(album = check_track_album, display_upc = row['displayupc'], volume_no = row['volumeno'], track_no = row['trackno'], hidden_track  = row['hiddentrack'], title  = row['title'],  song_version = row['songversion'], genre  = row['genre'], isrc = row['isrc'], track_duration = row['trackduration'], preview_clip_start_time  = row['previewclipstarttime'], preview_clip_duration  = row['previewclipduration'], p_line = row['p_line'], recording_artist  = row['recording artist'], artist  = row['artist'], release_name  = row['release name'], parental_advisory  = row['parentaladvisory'], label_name = row['labelname'], producer  = row['producer'], publisher  = row['publisher'], writer  = row['writer'], arranger  = row['arranger'], territories  = row['territories'], exclusive  = row['exclusive'], wholesale_price  = row['wholesaleprice'], download  = row['download'], sales_start_date  = row['salesstartdate'], sales_end_date  = row['salesenddate'], error  = row['error'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS(arranger = new_track.arranger, artist = new_track.artist, data_type = "track", display_upc = new_track.display_upc, download = new_track.download, error = new_track.error, exclusive = new_track.exclusive, genre = new_track.genre, hidden_track = new_track.hidden_track, isrc = new_track.isrc, label_name = new_track.label_name, p_line = new_track.p_line, parental_advisory = new_track.parental_advisory, preview_clip_duration = new_track.preview_clip_duration, preview_clip_start_time = new_track.preview_clip_start_time, producer = new_track.producer, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, sales_end_date = new_track.sales_end_date, sales_start_date = new_track.sales_start_date, song_version = new_track.song_version, territories = new_track.territories, title = new_track.title, track_duration = new_track.track_duration, track_no = new_track.track_no, volume_no = new_track.volume_no,  wholesale_price = new_track.wholesale_price,  writer = new_track.writer, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['displayupc'], product_title = row['title'] , genre = row['genre'], product_artist = row['artist'],  processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, display_upc = row['displayupc'], volume_no = row['volumeno'], track_no = row['trackno'], hidden_track  = row['hiddentrack'], title  = row['title'],  song_version = row['songversion'], genre  = row['genre'], isrc = row['isrc'], track_duration = row['trackduration'], preview_clip_start_time  = row['previewclipstarttime'], preview_clip_duration  = row['previewclipduration'], p_line = row['p_line'], recording_artist  = row['recording artist'], artist  = row['artist'], release_name  = row['release name'], parental_advisory  = row['parentaladvisory'], label_name = row['labelname'], producer  = row['producer'], publisher  = row['publisher'], writer  = row['writer'], arranger  = row['arranger'], territories  = row['territories'], exclusive  = row['exclusive'], wholesale_price  = row['wholesaleprice'], download  = row['download'], sales_start_date  = row['salesstartdate'], sales_end_date  = row['salesenddate'], error  = row['error'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS(arranger = new_track.arranger, artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, download = new_track.download, error = new_track.error, exclusive = new_track.exclusive, genre = new_track.genre, hidden_track = new_track.hidden_track, isrc = new_track.isrc, label_name = new_track.label_name, p_line = new_track.p_line, parental_advisory = new_track.parental_advisory, preview_clip_duration = new_track.preview_clip_duration, preview_clip_start_time = new_track.preview_clip_start_time, producer = new_track.producer, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, sales_end_date = new_track.sales_end_date, sales_start_date = new_track.sales_start_date, song_version = new_track.song_version, territories = new_track.territories, title = new_track.title, track_duration = new_track.track_duration, track_no = new_track.track_no, volume_no = new_track.volume_no,  wholesale_price = new_track.wholesale_price,  writer = new_track.writer, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track,  processed_day=today)
                    new_master.save()
        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {track_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['shola.albert@gmail.com', 'digger@wyldpytch.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {track_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['shola.albert@gmail.com', 'digger@wyldpytch.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise





############################
#     PPL                 #
############################
# @shared_task
# def process_ppl(filename):
#     ppl_file = ProcessedDocument.objects.filter(file_name=filename).first()
#     try:
        
#         ppl = pd.read_excel(ppl_file.file_doc, Index=None).fillna('')
#         ppl.columns = ppl.columns.str.strip().str.lower()
#         print(ppl.columns)
#         if not {'recording_isrc', 'ppl_recording_id'}.issubset(ppl.columns):
#             print("isrc and recording id not present")
#             send_mail(
#                             'Corrupted File',
#                             f''' 
#                             Hello Admin 
#                             The file {ppl_file.file_name} was corrupt !
#                             ISRC and UPC not present !
#                             Pls login to run it again
#                             ''',
#                             'Trackeet Recording Panel ',
#                             ['shola.albert@gmail.com'],
#                             fail_silently=False,
#                             )
#         else:
#             print("isrc and recording id present")
            

#         track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
#         track_raw_data = {
#             'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
#         }

#         album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
#         album_raw_data = {
#             'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
#         }

#         for index, row in ppl.iterrows():
#             track_raw_data['ISRC'].append(row['recording_isrc'])
#             track_raw_data['DisplayUPC'].append(row['ppl_recording_id'])
#             track_raw_data['Artist'].append(row['band_artist_name'])
#             track_raw_data['Title'].append(row['recording_title'])
#             track_raw_data['Release Name'].append(row['recording_title'])
#             track_raw_data['Recording Artist'].append(row['band_artist_name'])
#             track_raw_data['Publisher'].append(row['member_name'])
#             track_raw_data['LabelName'].append(row['member_name'])
            


#             album_raw_data['Product UPC'].append(row['ppl_recording_id'])
#             album_raw_data['Product Title'].append(row['recording_title'])
#             album_raw_data['Product Artist'].append(row['band_artist_name'])



#             check_track = Track.objects.filter(isrc=row['recording_isrc']).first()
#             if check_track:
#                 print(f" track {check_track.isrc} already exists")
#                 if 'ppl_recording_id' in ppl.columns:
#                     if not check_track.display_upc or check_track.display_upc != row['ppl_recording_id']:
#                         print(f"updating display upc ")
#                         check_track.display_upc = row['ppl_recording_id']
#                 else:
#                     print('ppl_recording_id not here')
#                 if 'band_artist_name' in ppl.columns:
#                     if not check_track.artist or check_track.artist != row['band_artist_name']:
#                         print(f"updating track artist")
#                         check_track.artist = row['band_artist_name']
#                     if not check_track.recording_artist or check_track.recording_artist != row['band_artist_name']:
#                         print(f"updating recording artist ")
#                         check_track.recording_artist = row['band_artist_name']
#                 else:
#                     print('band_artist_name not here')
#                 if 'recording_title' in ppl.columns:
#                     if not check_track.title or check_track.title != row['recording_title']:
#                         print(f"updating title ")
#                         check_track.title = row['recording_title']
#                     if not check_track.release_name or check_track.release_name != row['recording_title']:
#                         print(f"updating release name ")
#                         check_track.release_name = row['recording_title']
#                 else:
#                     print('recording_title not here')
#                 if 'member_name' in ppl.columns:
#                     if not check_track.publisher or check_track.publisher != row['member_name']:
#                         print(f"updating publisher ")
#                         check_track.publisher = row['member_name']
#                     if not check_track.label_name or check_track.label_name != row['member_name']:
#                         print(f"updating label name ")
#                         check_track.label_name = row['member_name']
#                 else:
#                     print('member_name not here')
#                 check_track.save()
#             else:
#                 check_track_album = Album.objects.filter(product_upc= row['ppl_recording_id']).first()
#                 if check_track_album:
#                     print(f" track album {check_track_album.product_upc} exists, creating track now! ")
#                     if 'recording_title' in ppl.columns:
#                         if not check_track_album.product_title or check_track_album.product_title != row['recording_title']:
#                             print(f"updating product_title")
#                             check_track_album.product_title = row['recording_title']
#                     else:
#                         print('recording_title not here for check_track_album')
#                     if 'band_artist_name' in ppl.columns:
#                         if not check_track_album.product_artist or check_track_album.product_artist != row['band_artist_name']:
#                             print(f"updating product artist")
#                             check_track_album.product_artist = row['band_artist_name']
#                     check_track_album.save()
#                     new_track = Track(album = check_track_album, isrc=row['recording_isrc'], display_upc= row['ppl_recording_id'], title=row['recording_title'], recording_artist=row['band_artist_name'] ,artist =row['band_artist_name'] , release_name=row['recording_title'], label_name=row['member_name'], publisher=row['member_name'], processed_day=today)
#                     new_track.save()
#                     new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
#                     new_master.save()
#                 else:
                     
#                     print(f"track album does not exists, creating track album and track now! ")
#                     if {'recording_title', 'band_artist_name', 'member_name'}.issubset(ppl.columns):
#                     # if 'recording_title' and  'band_artist_name' and 'member_name' in ppl:
#                         new_track_album = Album(product_upc = row['ppl_recording_id'], product_title = row['recording_title'] , product_artist = row['band_artist_name'],  processed_day=today )
#                         new_track_album.save()
#                         new_track = Track(album = new_track_album, display_upc = row['ppl_recording_id'], title  = row['recording_title'], isrc = row['recording_isrc'], recording_artist  = row['band_artist_name'], artist  = row['band_artist_name'], release_name  = row['band_artist_name'], label_name = row['member_name'], publisher  = row['member_name'], processed_day=today)
#                         new_track.save()
#                         new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
#                         new_master.save()
#                     else:
#                         print('recording_title, band_artist_name, member_name are not available for a new album')

#         print(track_raw_data) 
#         new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
#         # trk_name = f'processed_horus_{today}_track.csv'
#         track_file = new_track_df.to_csv('processed_ppl_track.csv', index = False, header=True)
#         new_track_document = ProcessedTrackFile()
#         new_track_document.document = ppl_file
#         new_track_document.file_name = 'processed_ppl_track.csv'
#         new_track_document.save()

#         with open('processed_ppl_track.csv', 'rb') as csv:
#             new_track_document.file_doc.save('processed_ppl_track.csv', File(csv))

#         print(album_raw_data)
#         new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
#         album_file = new_album_df.to_csv('processed_ppl_album.csv', index = False, header=True)
#         # alb_name = f'processed_horus_{today}_album.csv'
#         new_album_document = ProcessedAlbumFile()
#         new_album_document.document = ppl_file
#         new_album_document.file_name = 'processed_ppl_album.csv'
#         new_album_document.save()

#         with open('processed_ppl_album.csv', 'rb') as csv:
#             new_album_document.file_doc.save('processed_ppl_album.csv', File(csv))


#         send_mail(
#                         'File Processed',
#                         f''' 
#                         Hello Admin 
#                         The file {ppl_file.file_name} has been successfully processed !
#                         Congratulations
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)
#     except (ValueError, NameError, TypeError) as error:
#         err_msg = str(error)
#         print(err_msg)
#         send_mail(
#                         'Error Processing File',
#                         f''' 
#                         Hello Admin 
#                         The file {ppl_file.file_name} is corrupt!
#                         See the details below:
#                         {err_msg}
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)
#     except:
#         print("Unexpected error!")
#         raise

############################
#     PPL  ACCOUNT          #
############################
@shared_task
def process_ppl_account(pk):
    ppl_file = ProcessedDocument.objects.get(pk=pk)
    try:

        ppl = pd.read_excel(ppl_file.file_doc, Index=None).fillna('')
        ppl.columns = ppl.columns.str.strip().str.lower()
        print(ppl.columns)
        if not {'period', 'isrc'}.issubset(ppl.columns):
            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':ppl_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

            
        else:
            pass


        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount',' Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }

        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
        }

        album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album_raw_data = {
            'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
        }

        for index, row in ppl.iterrows():

            track_raw_data['ISRC'].append(row['isrc'])
            track_raw_data['DisplayUPC'].append(row['orchard upc'])
            #TRACK CHECKS 
            check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
            if check_track_master:
                print("PPL CHECK TRACK MASTER EXISTS")

            if check_track_master and check_track_master.track_duration != None or '':
                track_raw_data['TrackDuration'].append(check_track_master.track_duration)
            else:
                track_raw_data['TrackDuration'].append("None")
            
            if check_track_master and check_track_master.download != None or '':
                track_raw_data['Download'].append(check_track_master.download)
            else:
                track_raw_data['Download'].append("None")
            
            if check_track_master and check_track_master.sales_start_date != None or '':
                track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
            else:
                track_raw_data['SalesStartDate'].append("None")
            
            if check_track_master and check_track_master.sales_end_date != None or '':
                track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
            else:
                track_raw_data['SalesEndDate'].append("None")
            
            if check_track_master and check_track_master.error != None or '':
                track_raw_data['Error'].append(check_track_master.error)
            else:
                track_raw_data['Error'].append("None")

            if check_track_master and check_track_master.parental_advisory != None or '':
                track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
            else:
                track_raw_data['ParentalAdvisory'].append("None")

            if check_track_master and check_track_master.preview_clip_start_time != None or '':
                track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
            else:
                track_raw_data['PreviewClipStartTime'].append("None")

            if check_track_master and check_track_master.preview_clip_duration != None or '':
                track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
            else:
                track_raw_data['PreviewClipDuration'].append("None")

            if check_track_master and check_track_master.song_version != None or '':
                track_raw_data['SongVersion'].append(check_track_master.song_version)
            else:
                track_raw_data['SongVersion'].append("None")

            if check_track_master and check_track_master.p_line != None or '':
                track_raw_data['P_LINE'].append(check_track_master.p_line)
            else:
                track_raw_data['P_LINE'].append("None")
            
            if check_track_master and check_track_master.track_no != None or '':
                track_raw_data['TrackNo'].append(check_track_master.track_no)
            else:
                track_raw_data['TrackNo'].append("None")

            if check_track_master and check_track_master.hidden_track != None or '':
                track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
            else:
                track_raw_data['HiddenTrack'].append("None")

            if check_track_master and check_track_master.artist != None or '':
                track_raw_data['Artist'].append(check_track_master.artist)
            else:
                track_raw_data['Artist'].append(row['artist name'])
            
            if check_track_master and check_track_master.title != None or '':
                track_raw_data['Title'].append(check_track_master.title)
            else:
                track_raw_data['Title'].append(row['track name'])

            if check_track_master and check_track_master.release_name != None or '':
                track_raw_data['Release Name'].append(check_track_master.release_name)
            else:
                track_raw_data['Release Name'].append(row['release name'])

            if check_track_master and  check_track_master.recording_artist != None or '':
                track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
            else:
                track_raw_data['Recording Artist'].append(row['artist name'])

            if check_track_master and check_track_master.label_name != None or '':
                track_raw_data['LabelName'].append(check_track_master.label_name)
            else:
                track_raw_data['LabelName'].append(row['imprint label'])

            if check_track_master and check_track_master.volume_no != None or '':
                track_raw_data['VolumeNo'].append(check_track_master.volume_no)
            else:
                track_raw_data['VolumeNo'].append(row['volume'])
            
            if check_track_master and check_track_master.genre != None or '':
                track_raw_data['Genre'].append(check_track_master.genre)
            else:
                track_raw_data['Genre'].append("None")

            if check_track_master and check_track_master.publisher != None or '':
                track_raw_data['Publisher'].append(check_track_master.publisher)
            else:
                track_raw_data['Publisher'].append("None")

            if check_track_master and check_track_master.producer != None or '':
                track_raw_data['Producer'].append(check_track_master.producer)
            else:
                track_raw_data['Producer'].append("None")

            if check_track_master and check_track_master.writer != None or '':
                track_raw_data['Writer'].append(check_track_master.writer)
            else:
                track_raw_data['Writer'].append("None")
            
            if check_track_master and check_track_master.arranger != None or '':
                track_raw_data['Arranger'].append(check_track_master.arranger)
            else:
                track_raw_data['Arranger'].append("None")

            if check_track_master and check_track_master.territories != None or '':
                track_raw_data['Territories'].append(check_track_master.territories)
            else:
                track_raw_data['Territories'].append(row['territory'])
            
            if check_track_master and check_track_master.exclusive != None or '':
                track_raw_data['Exclusive'].append(check_track_master.exclusive)
            else:
                track_raw_data['Exclusive'].append("None")

            if check_track_master and check_track_master.wholesale_price != None or '':
                track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
            else:
                track_raw_data['WholesalePrice'].append("None")

            

            #ALBUM CHECKS

            album_raw_data['Product UPC'].append(row['orchard upc'])

            check_album_master = Master_DRMSYS.objects.filter(display_upc=row['orchard upc'], data_type="Album").first()
            if check_album_master:
                print("PPL CHECK MASTER ALBUM EXISTS")

            if check_album_master and check_album_master.c_line != None or '':
                album_raw_data['CLINE'].append(check_album_master.c_line)
            else:
                album_raw_data['CLINE'].append("None")

            if check_album_master and check_album_master.master_carveouts != None or '':
                album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
            else:
                album_raw_data['Master Carveouts'].append("None")

            if check_album_master and check_album_master.territories_carveouts != None or '':
                album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
            else:
                album_raw_data['Territories Carveouts'].append("None")

            if check_album_master and check_album_master.deleted != None or '':
                album_raw_data['Deleted?'].append(check_album_master.deleted)
            else:
                album_raw_data['Deleted?'].append("None")

            if check_album_master and check_album_master.manufacturer_upc != None or '':
                album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
            else:
                album_raw_data['Manufacturer UPC'].append("None")

            if check_album_master and check_album_master.release_date != None or '':
                album_raw_data['Release Date'].append(check_album_master.release_date)
            else:
                album_raw_data['Release Date'].append("None")

            if check_album_master and check_album_master.artist_url != None or '':
                album_raw_data['Artist URL'].append(check_album_master.artist_url)
            else:
                album_raw_data['Artist URL'].append("None")

            if check_album_master and check_album_master.label_catalogno != None or '':
                album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
            else:
                album_raw_data['Catalog Number'].append(row['label catalog #'])


            if check_album_master and check_album_master.total_volumes != None or '':
                album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
            else:
                album_raw_data['Total Volumes'].append(1)

            if check_album_master and check_album_master.total_tracks != None or '':
                album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
            else:
                album_raw_data['Total Tracks'].append(row['track #'])

            

            if check_album_master and check_album_master.label_name != None or '':
                album_raw_data['Imprint Label'].append(check_album_master.label_name)
            else:
                album_raw_data['Imprint Label'].append(row['imprint label'])

            if check_album_master and check_album_master.genre != None or '':
                album_raw_data['Genre'].append(check_album_master.genre)
            else:
                album_raw_data['Genre'].append("None")

            if check_album_master and check_album_master.title != None or '':
                album_raw_data['Product Title'].append(check_album_master.title)
            else:
                album_raw_data['Product Title'].append(row['track name'])
            
            if check_album_master and check_album_master.artist != None or '':
                album_raw_data['Product Artist'].append(check_album_master.artist)
            else:
                album_raw_data['Product Artist'].append(row['artist name'])


            account_raw_data['Period'].append(row['period'])
            account_raw_data['Activity Period'].append(row['activity period'])
            account_raw_data['Retailer'].append(row['dms'])
            account_raw_data['Territory'].append(row['territory'])
            account_raw_data['Product UPC'].append(row['orchard upc'])
            account_raw_data['Manufacturer UPC'].append(row['orchard upc'])
            account_raw_data['Imprint Label'].append(row['imprint label'])
            account_raw_data['Artist Name'].append(row['artist name'])
            account_raw_data['Product Name'].append(row['track name'])
            account_raw_data['Track Name'].append(row['track name'])
            account_raw_data['Track Artist'].append(row['artist name'])
            account_raw_data['ISRC'].append(row['isrc'])
            account_raw_data['Total'].append(row['gross'])
            account_raw_data['Adjusted Total'].append(row['adjusted gross'])
            account_raw_data['Label Share Net Receipts'].append(row['label share net receipts'])
            
            
            check_track = Master_DRMSYS.objects.filter(isrc=row['isrc'],data_type="Track").first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                
                if {'orchard upc'}.issubset(ppl.columns):
                    if not check_track.display_upc or check_track.display_upc != row['orchard upc']:
                        print(f"updating display upc ")
                        check_track.display_upc = row['orchard upc']
                else:
                    print('orchard upc not here')
                
                if {'artist name'}.issubset(ppl.columns):
                    if not check_track.artist or check_track.artist != row['artist name']:
                        print(f"updating track artist")
                        check_track.artist = row['artist name']
                    if not check_track.recording_artist or check_track.recording_artist != row['artist name']:
                        print(f"updating recording artist ")
                        check_track.recording_artist = row['artist name']
                else:
                    print('artist name not here')
                if {'track name'}.issubset(ppl.columns):
                    if not check_track.title or check_track.title != row['track name']:
                        print(f"updating title ")
                        check_track.title = row['track name']
                    if not check_track.release_name or check_track.release_name != row['track name']:
                        print(f"updating release name ")
                        check_track.release_name = row['track name']
                else:
                    print('track name not here')
                if {'imprint label'}.issubset(ppl.columns):
                    if not check_track.publisher or check_track.publisher != row['imprint label']:
                        print(f"updating publisher ")
                        check_track.publisher = row['imprint label']
                    if not check_track.label_name or check_track.label_name != row['imprint label']:
                        print(f"updating label name ")
                        check_track.label_name = row['imprint label']
                else:
                    print('imprint label not here')
                check_track.save()
            else:
                check_track_album = Master_DRMSYS.objects.filter(display_upc=row['orchard upc'], data_type="Album").first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    
                    if {'track name'}.issubset(ppl.columns):
                        if not check_track_album.product_title or check_track_album.product_title != row['track name']:
                            print(f"updating track name")
                            check_track_album.product_title = row['track name']
                    else:
                        print('track name not here for check_track_album')
                    
                    if {'artist name'}.issubset(ppl.columns):
                        if not check_track_album.product_artist or check_track_album.product_artist != row['artist name']:
                            print(f"updating product artist")
                            check_track_album.product_artist = row['artist name']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['isrc'], display_upc= row['orchard upc'], title=row['track name'], recording_artist=row['artist name'],artist =row['artist name'] , release_name=row['track name'], label_name=row['imprint label'], publisher=row['imprint label'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    
                    print(f"track album does not exists, creating track album and track now! ")
                    if {'track name', 'artist name'}.issubset(ppl.columns):
                    # if 'recording_title' and  'band_artist_name' and 'member_name' in ppl:
                        new_track_album = Album(product_upc = row['orchard upc'], product_title = row['track name'] , product_artist = row['artist name'],  processed_day=today )
                        new_track_album.save()
                        new_track = Track(album = new_track_album, display_upc = row['isrc'], title  = row['track name'], isrc = row['isrc'], recording_artist  = row['artist name'], artist  = row['artist name'], release_name  = row['track name'], label_name = row['imprint label'], publisher  = row['imprint label'], processed_day=today)
                        new_track.save()
                        new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                        new_master.save()
                    else:
                        print('track name, artist name are not available for a new album')




            if  {'period', 'isrc', 'activity period', 'orchard upc', 'imprint label', 'artist name', 'track name'}.issubset(ppl.columns):
                new_account = Accounting( period = row['period'], activity_period = row['activity period'], retailer = row['dms'], territory = row['territory'], orchard_UPC = row['orchard upc'], manufacturer_UPC = row['orchard upc'], imprint_label = row['imprint label'], artist_name = row['artist name'], product_name = row['track name'], track_name = row['track name'], track_artist =  row['artist name'], isrc = row['isrc'], total = row['gross'], adjusted_total = row['adjusted gross'], label_share_net_receipts = row['label share net receipts'], vendor = "PPL", processed_day = today)
                new_account.save()
            else:
                print('Some columns missing ')

        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_ppl_track_output_{today_date}.csv"
        track_file_name_txt  = f"processed_ppl_track_output_{today_date}.txt"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        track_file_txt = new_track_df.to_csv(track_file_name_txt, index = False, header=True, sep='\t')
        #track_file_txt = np.savetxt(track_file_name_txt, new_track_df.values, fmt='%d', delimiter='\t', header="DisplayUPC\tVolumeNo\tTrackNo\tHiddenTrack\tTitle\tSongVersion\tGenre\tISRC\tTrackDuration\tPreviewClipStartTime\tPreviewClipDuration\tP_LINE\tRecording Artist\tArtist\tRelease Name\tParentalAdvisory\tLabelName\tProducer\tPublisher\tWriter\tArranger\tTerritories\tExclusive\tWholesalePrice\tDownload\tSalesStartDate\tSalesEndDate\tError")
        new_track_document = ProcessedTrackFile()
        new_track_document.document = ppl_file
        new_track_document.file_name = track_file_name
        new_track_document.file_name_txt = track_file_name_txt
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        with open(track_file_name_txt, 'rb') as txt:
            new_track_document.file_doc_txt.save(track_file_name_txt, File(txt))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_ppl_album_output_{today_date}.csv"
        album_file_name_txt  = f"processed_ppl_album_output_{today_date}.txt"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        album_file_txt = new_album_df.to_csv(album_file_name_txt, index = False, header=True, sep='\t')
        #album_file_txt = np.savetxt(album_file_name_txt, new_album_df.values, fmt='%d', delimiter='\t', header="Product UPC\tTotal Volumes\tTotal Tracks\tProduct Title\tProduct Artist\tGenre\tCLINE\tRelease Date\tImprint Label\tArtist URL\tCatalog Number\tManufacturer UPC\tDeleted?\tTerritories Carveouts\tMaster Carveouts")
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = ppl_file
        new_album_document.file_name = album_file_name
        new_album_document.file_name_txt = album_file_name_txt
        new_album_document.save()

        with open( album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))

        with open(album_file_name_txt, 'rb') as txt:
            new_album_document.file_doc_txt.save(album_file_name_txt, File(txt))


        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # nan_value = float("NaN")
        # new_account_df = new_account_df.replace("", nan_value)
        # new_account_df = new_account_df.dropna(subset=['Period'])
        # new_account_df = new_account_df.drop_duplicates(keep = False)
        account_file_name  = f"processed_ppl_account_output_{today_date}.csv"
        # trk_name = f'processed_horus_{today}_track.csv'
        track_account = new_account_df.to_csv(account_file_name, index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = ppl_file
        new_account_document.file_name = account_file_name
        new_account_document.save()

        with open(account_file_name, 'rb') as csv:
            new_account_document.file_doc.save( account_file_name, File(csv))


        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':ppl_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

       
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ppl_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise








############################
#     MCPS                 #
############################
# @shared_task
# def process_mcps(filename):
#     mcps_file = ProcessedDocument.objects.filter(file_name=filename).first()
#     try:        
#         col_list = [ 'Distribution ID', 'Manufacturer Number', 'Manufacturer Short Name', 'Manufacturer Name', 'catalogue Number', 'Product Title', 'Artist', 'Tunecode', 'Title', 'Interested Party (1)', 'Interested Party (2)', 'Interested Party (3)', 'Interested Party (4)', 'Period', 'Quantity', 'Territory', 'CAE Number', 'Member', 'Role', 'Share(%)', 'Gross Amount', 'Commission', 'Nett Amount', 'Royalty Amount', 'Invoice & Batch', 'Source', 'Source Description']
#         mcps = pd.read_excel(mcps_file.file_doc, usecols=col_list).fillna('')
#         if 'catalogue Number' not in mcps:
#             send_mail(
#                             'Corrupted File',
#                             f''' 
#                             Hello Admin 
#                             The file {mcps_file.file_name} was corrupt !
#                             Pls login to run it again
#                             ''',
#                             'Trackeet Recording Panel ',
#                             ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                             fail_silently=False,
#                             )
#         else:
#             pass

#         track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
#         track_raw_data = {
#             'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
#                         }

#         album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
#         album_raw_data = {
#             'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
#                         }

#         for index, row in mcps.iterrows():

#             track_raw_data['ISRC'].append(row['Tunecode'])
#             track_raw_data['DisplayUPC'].append(row['Manufacturer Number'])
#             track_raw_data['Artist'].append(row['Interested Party (1)'])
#             track_raw_data['Title'].append(row['Title'])
#             track_raw_data['Release Name'].append(row['Product Title'])
#             track_raw_data['Recording Artist'].append(row['Interested Party (1)'])
#             track_raw_data['Publisher'].append(row['Member'])
#             track_raw_data['LabelName'].append(row['Interested Party (3)'])



#             album_raw_data['Product UPC'].append(row['Manufacturer Number'])
#             album_raw_data['Product Artist'].append(row['Interested Party (1)'])
#             album_raw_data['Release Date'].append(row['Period'])
#             album_raw_data['Catalog Number'].append(row['catalogue Number'])
#             album_raw_data['Imprint Label'].append(row['Interested Party (3)'])

#             check_track = Track.objects.filter(isrc=row['Tunecode']).first()
#             if check_track:
#                 print(f" track {check_track.isrc} already exists")
#                 if not check_track.display_upc or check_track.display_upc != row['Manufacturer Number']:
#                     print(f"updating display upc ")
#                     check_track.display_upc = row['Manufacturer Number']
#                 if not check_track.artist or check_track.artist != row['Interested Party (1)']:
#                     print(f"updating track artist")
#                     check_track.artist = row['Interested Party (1)']
#                 if not check_track.title or check_track.title != row['Title']:
#                     print(f"updating title ")
#                     check_track.title = row['Title']
#                 if not check_track.release_name or check_track.release_name != row['Product Title']:
#                     print(f"updating release name ")
#                     check_track.release_name = row['Product Title']
#                 if not check_track.recording_artist or check_track.recording_artist != row['Interested Party (1)']:
#                     print(f"updating recording artist")
#                     check_track.recording_artist = row['Interested Party (1)']
#                 if not check_track.publisher or check_track.publisher !=  row['Member']:
#                     print(f"updating publisher ")
#                     check_track.publisher = row['Member']
#                 if not check_track.label_name or check_track.label_name != row['Interested Party (3)']:
#                     print(f"updating label name ")
#                     check_track.label_name = row['Interested Party (3)']
#                 check_track.save()
#             else:
#                 check_track_album = Album.objects.filter(product_upc= row['Manufacturer Number']).first()
#                 if check_track_album:
#                     print(f" track album {check_track_album.product_upc} exists, creating track now! ")
#                     if not check_track_album.release_date:
#                         check_track_album.release_date = row['Period']
#                     if not check_track_album.product_artist:
#                         print(f"updating product artist")
#                         check_track_album.product_artist =  row['Interested Party (1)']
#                     if not check_track_album.imprint_label:
#                         check_track_album.imprint_label = row['Interested Party (3)']
#                     if not check_track_album.catalog_number:
#                         check_track_album.catalog_number = row['catalogue Number']
#                     check_track_album.save()
#                     new_track = Track(album = check_track_album, isrc=row['Tunecode'], display_upc= row['Manufacturer Number'], title = row['Title'], recording_artist=row['Interested Party (1)'] ,artist =row['Interested Party (1)'] , release_name=row['Product Title'], label_name=row['Interested Party (3)'], publisher=row['Member'], processed_day=today)
#                     new_track.save()
#                     new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
#                     new_master.save()
                    
#                 else:
#                     print(f"track album does not exists, creating track album and track now! ")
#                     new_track_album = Album(product_upc = row['Manufacturer Number'] , product_artist = row['Interested Party (1)'],release_date = row['Period'], imprint_label = row['Interested Party (3)'], catalog_number = row['catalogue Number'],  processed_day=today )
#                     new_track_album.save()
#                     new_track = Track(album = new_track_album, display_upc = row['Manufacturer Number'], title  = row['Title'], isrc =row['Tunecode'], recording_artist  = row['Interested Party (1)'], artist  = row['Interested Party (1)'], release_name  = row['Product Title'], label_name = row['Interested Party (3)'], publisher  = row['Member'], processed_day=today)
#                     new_track.save()
#                     new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
#                     new_master.save()
                    
#         print(track_raw_data) 
#         new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
#         # trk_name = f'processed_horus_{today}_track.csv'
#         track_file = new_track_df.to_csv('processed_mcps_track.csv', index = False, header=True)
#         new_track_document = ProcessedTrackFile()
#         new_track_document.document = mcps_file
#         new_track_document.file_name = 'processed_mcps_track.csv'
#         new_track_document.save()

#         with open('processed_mcps_track.csv', 'rb') as csv:
#             new_track_document.file_doc.save('processed_mcps_track.csv', File(csv))

#         print(album_raw_data)
#         new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
#         album_file = new_album_df.to_csv('processed_mcps_album.csv', index = False, header=True)
#         # alb_name = f'processed_horus_{today}_album.csv'
#         new_album_document = ProcessedAlbumFile()
#         new_album_document.document = mcps_file
#         new_album_document.file_name = 'processed_mcps_album.csv'
#         new_album_document.save()

#         with open('processed_mcps_album.csv', 'rb') as csv:
#             new_album_document.file_doc.save('processed_mcps_album.csv', File(csv))

#         send_mail(
#                         'File Processed',
#                         f''' 
#                         Hello Admin 
#                         The file {mcps_file.file_name} has been successfully processed !
#                         Congratulations
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)
#     except (ValueError, NameError, TypeError) as error:
#         err_msg = str(error)
#         print(err_msg)
#         send_mail(
#                         'Error Processing File',
#                         f''' 
#                         Hello Admin 
#                         The file {mcps_file.file_name} is corrupt!
#                         See the details below:
#                         {err_msg}
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)
#     except:
#         print("Unexpected error!")
#         raise





############################
#     MCPS  ACCOUNT        #
############################
@shared_task
def process_mcps_account(pk):
    mcps_file = ProcessedDocument.objects.get(pk=pk)
    
    try:
        
        mcps = pd.read_excel(mcps_file.file_doc, Index=None).fillna('')
        mcps.columns = mcps.columns.str.strip().str.lower()
        
        if not {'catalogue number'}.issubset(mcps.columns):
            
            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':mcps_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com','digger@wyldpytch.com' ], html_message=html_message)
            #send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com' ], html_message=html_message)
            
        else:
            pass
        
        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
                        }

        album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album_raw_data = {
            'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
                        }

        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }

        for index, row in mcps.iterrows():


            track_raw_data['ISRC'].append(row['tunecode'])
            
            check_track_master = Master_DRMSYS.objects.filter(isrc=row['tunecode'], data_type='Track').first()
            if check_track_master:
                print("MCPS CHECK TRACK MASTER EXISTS")

            if check_track_master and check_track_master.track_duration != None or '':
                track_raw_data['TrackDuration'].append(check_track_master.track_duration)
            else:
                track_raw_data['TrackDuration'].append("None")

            if check_track_master and check_track_master.display_upc != None or '':
                track_raw_data['DisplayUPC'].append(check_track_master.display_upc)
            else:
                track_raw_data['DisplayUPC'].append(row['manufacturer number'])
            
            if check_track_master and check_track_master.download != None or '':
                track_raw_data['Download'].append(check_track_master.download)
            else:
                track_raw_data['Download'].append("None")
            
            if check_track_master and check_track_master.sales_start_date != None or '':
                track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
            else:
                track_raw_data['SalesStartDate'].append("None")
            
            if check_track_master and check_track_master.sales_end_date != None or '':
                track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
            else:
                track_raw_data['SalesEndDate'].append("None")
            
            if check_track_master and check_track_master.error != None or '':
                track_raw_data['Error'].append(check_track_master.error)
            else:
                track_raw_data['Error'].append("None")

            if check_track_master and check_track_master.parental_advisory != None or '':
                track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
            else:
                track_raw_data['ParentalAdvisory'].append("None")

            if check_track_master and check_track_master.preview_clip_start_time != None or '':
                track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
            else:
                track_raw_data['PreviewClipStartTime'].append("None")

            if check_track_master and check_track_master.preview_clip_duration != None or '':
                track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
            else:
                track_raw_data['PreviewClipDuration'].append("None")

            if check_track_master and check_track_master.song_version != None or '':
                track_raw_data['SongVersion'].append(check_track_master.song_version)
            else:
                track_raw_data['SongVersion'].append("None")

            if check_track_master and check_track_master.p_line != None or '':
                track_raw_data['P_LINE'].append(check_track_master.p_line)
            else:
                track_raw_data['P_LINE'].append("None")
            
            if check_track_master and check_track_master.track_no != None or '':
                track_raw_data['TrackNo'].append(check_track_master.track_no)
            else:
                track_raw_data['TrackNo'].append("None")

            if check_track_master and check_track_master.hidden_track != None or '':
                track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
            else:
                track_raw_data['HiddenTrack'].append("None")

            if check_track_master and check_track_master.artist != None or '':
                track_raw_data['Artist'].append(check_track_master.artist)
            else:
                track_raw_data['Artist'].append(row['interested party (1)'])
            
            if check_track_master and check_track_master.title != None or '':
                track_raw_data['Title'].append(check_track_master.title)
            else:
                track_raw_data['Title'].append(row['interested party (3)'])

            if check_track_master and check_track_master.release_name != None or '':
                track_raw_data['Release Name'].append(check_track_master.release_name)
            else:
                track_raw_data['Release Name'].append(row['product title'])

            if check_track_master and  check_track_master.recording_artist != None or '':
                track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
            else:
                track_raw_data['Recording Artist'].append(row['interested party (1)'])

            if check_track_master and check_track_master.label_name != None or '':
                track_raw_data['LabelName'].append(check_track_master.label_name)
            else:
                track_raw_data['LabelName'].append(row['interested party (3)'])

            if check_track_master and check_track_master.volume_no != None or '':
                track_raw_data['VolumeNo'].append(check_track_master.volume_no)
            else:
                track_raw_data['VolumeNo'].append("None")
            
            if check_track_master and check_track_master.genre != None or '':
                track_raw_data['Genre'].append(check_track_master.genre)
            else:
                track_raw_data['Genre'].append("None")

            if check_track_master and check_track_master.publisher != None or '':
                track_raw_data['Publisher'].append(check_track_master.publisher)
            else:
                track_raw_data['Publisher'].append(row['member'])

            if check_track_master and check_track_master.producer != None or '':
                track_raw_data['Producer'].append(check_track_master.producer)
            else:
                track_raw_data['Producer'].append("None")

            if check_track_master and check_track_master.writer != None or '':
                track_raw_data['Writer'].append(check_track_master.writer)
            else:
                track_raw_data['Writer'].append("None")
            
            if check_track_master and check_track_master.arranger != None or '':
                track_raw_data['Arranger'].append(check_track_master.arranger)
            else:
                track_raw_data['Arranger'].append("None")

            if check_track_master and check_track_master.territories != None or '':
                track_raw_data['Territories'].append(check_track_master.territories)
            else:
                track_raw_data['Territories'].append("None")
            
            if check_track_master and check_track_master.exclusive != None or '':
                track_raw_data['Exclusive'].append(check_track_master.exclusive)
            else:
                track_raw_data['Exclusive'].append("None")

            if check_track_master and check_track_master.wholesale_price != None or '':
                track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
            else:
                track_raw_data['WholesalePrice'].append("None")

            
            #ALBUM CHECKS

            album_raw_data['Product UPC'].append(row['manufacturer number'])
            check_album_master = Master_DRMSYS.objects.filter(display_upc=row['manufacturer number'], data_type="Album").first()
            if check_album_master:
                print("MCPS CHECK MASTER ALBUM EXISTS")

            if check_album_master and check_album_master.c_line != None or '':
                album_raw_data['CLINE'].append(check_album_master.c_line)
            else:
                album_raw_data['CLINE'].append("None")

            if check_album_master and check_album_master.master_carveouts != None or '':
                album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
            else:
                album_raw_data['Master Carveouts'].append("None")

            if check_album_master and check_album_master.territories_carveouts != None or '':
                album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
            else:
                album_raw_data['Territories Carveouts'].append("None")

            if check_album_master and check_album_master.deleted != None or '':
                album_raw_data['Deleted?'].append(check_album_master.deleted)
            else:
                album_raw_data['Deleted?'].append("None")

            if check_album_master and check_album_master.manufacturer_upc != None or '':
                album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
            else:
                album_raw_data['Manufacturer UPC'].append("None")

            if check_album_master and check_album_master.release_date != None or '':
                album_raw_data['Release Date'].append(check_album_master.release_date)
            else:
                album_raw_data['Release Date'].append(row['period'])

            if check_album_master and check_album_master.artist_url != None or '':
                album_raw_data['Artist URL'].append(check_album_master.artist_url)
            else:
                album_raw_data['Artist URL'].append("None")

            if check_album_master and check_album_master.label_catalogno != None or '':
                album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
            else:
                album_raw_data['Catalog Number'].append(row['catalogue number'])


            check_master_for_cat = Master_DRMSYS.objects.filter(label_catalogno=row['catalogue number'])
            if check_master_for_cat:
                total_vol = check_master_for_cat.count()
                album_raw_data['Total Volumes'].append(str(total_vol))
            else:
                album_raw_data['Total Volumes'].append('1')
       
            if check_album_master and check_album_master.total_tracks != None or '':
                album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
            else:
                album_raw_data['Total Tracks'].append(1)

            if check_album_master and check_album_master.label_name != None or '':
                album_raw_data['Imprint Label'].append(check_album_master.label_name)
            else:
                album_raw_data['Imprint Label'].append(row['interested party (3)'])

            if check_album_master and check_album_master.genre != None or '':
                album_raw_data['Genre'].append(check_album_master.genre)
            else:
                album_raw_data['Genre'].append("None")

            if check_album_master and check_album_master.title != None or '':
                album_raw_data['Product Title'].append(check_album_master.title)
            else:
                album_raw_data['Product Title'].append("None")
            
            if check_album_master and check_album_master.artist != None or '':
                album_raw_data['Product Artist'].append(check_album_master.artist)
            else:
                album_raw_data['Product Artist'].append(row['interested party (1)'])



           
            account_raw_data['Period'].append(row['distribution id'])
            account_raw_data['Activity Period'].append(row['period'])
            account_raw_data['Retailer'].append(row['manufacturer name'])
            account_raw_data['Territory'].append(row['territory'])
            account_raw_data['Product UPC'].append(row['catalogue number'])
            account_raw_data['Project Code'].append(row['manufacturer short name'])
            account_raw_data['Product Code'].append(row['tunecode'])
            account_raw_data['Manufacturer UPC'].append(row['manufacturer number'])
            account_raw_data['Subaccount'].append(row['interested party (2)'])
            account_raw_data['Imprint Label'].append(row['interested party (3)'])
            account_raw_data['Artist Name'].append(row['artist'])
            account_raw_data['Product Name'].append(row['product title'])
            account_raw_data['Track Name'].append(row['title'])
            account_raw_data['Track Artist'].append(row['interested party (1)'])
            account_raw_data['ISRC'].append(row['tunecode'])
            account_raw_data['Volume'].append(row['quantity'])
            account_raw_data['Trans Type'].append(row['source'])
            account_raw_data['Trans Type Description'].append(row['source description'])
            account_raw_data['Discount'].append(row['commission'])
            account_raw_data['Price'].append(row['gross amount'])
            account_raw_data['Quantity'].append(row['quantity'])
            account_raw_data['Total'].append(row['nett amount'])
           
            check_track = Track.objects.filter(isrc=row['tunecode']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if not check_track.display_upc or check_track.display_upc != row['manufacturer number']:
                    print(f"updating display upc ")
                    check_track.display_upc = row['manufacturer number']
                if not check_track.artist or check_track.artist != row['interested party (1)']:
                    print(f"updating track artist")
                    check_track.artist = row['interested party (1)']
                if not check_track.title or check_track.title != row['title']:
                    print(f"updating title ")
                    check_track.title = row['title']
                if not check_track.release_name or check_track.release_name != row['product title']:
                    print(f"updating release name ")
                    check_track.release_name = row['product title']
                if not check_track.recording_artist or check_track.recording_artist != row['interested party (1)']:
                    print(f"updating recording artist")
                    check_track.recording_artist = row['interested party (1)']
                if not check_track.publisher or check_track.publisher !=  row['member']:
                    print(f"updating publisher ")
                    check_track.publisher = row['member']
                if not check_track.label_name or check_track.label_name != row['interested party (3)']:
                    print(f"updating label name ")
                    check_track.label_name = row['interested party (3)']
                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_upc= row['manufacturer number']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    if not check_track_album.release_date:
                        check_track_album.release_date = row['period']
                    if not check_track_album.product_artist:
                        print(f"updating product artist")
                        check_track_album.product_artist =  row['interested party (1)']
                    if not check_track_album.imprint_label:
                        check_track_album.imprint_label = row['interested party (3)']
                    if not check_track_album.catalog_number:
                        check_track_album.catalog_number = row['catalogue number']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['tunecode'], display_upc= row['manufacturer number'], title = row['title'], recording_artist=row['interested party (1)'] ,artist =row['interested party (1)'] , release_name=row['product title'], label_name=row['interested party (3)'], publisher=row['member'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                    
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['manufacturer number'] , product_artist = row['interested party (1)'],release_date = row['period'], imprint_label = row['interested party (3)'], catalog_number = row['catalogue number'],  processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, display_upc = row['manufacturer number'], title  = row['title'], isrc =row['tunecode'], recording_artist  = row['interested party (1)'], artist  = row['interested party (1)'], release_name  = row['product title'], label_name = row['interested party (3)'], publisher  = row['member'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()


            new_account = Accounting(period = row['distribution id'], activity_period=row['period'], retailer = row['manufacturer name'], territory = row['territory'], orchard_UPC = row['catalogue number'], manufacturer_UPC = row['manufacturer number'], project_code = row['manufacturer short name'], product_code = row['tunecode'], subaccount = row['interested party (2)'], imprint_label = row['interested party (3)'], artist_name = row['artist'], product_name = row['product title'], track_name = row['title'], track_artist = row['interested party (1)'], isrc =  row['tunecode'], volume = row['quantity'], trans_type = row['source'], trans_type_description = row['source description'], discount = row['commission'], actual_price = row['gross amount'], quantity = row['quantity'], total = row['nett amount'],vendor="MCPS", processed_day=today)
            new_account.save()
        
        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_mcps_track_output_{today_date}.csv"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = mcps_file
        new_track_document.file_name = track_file_name
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_mcps_album_output_{today_date}.csv"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = mcps_file
        new_album_document.file_name = album_file_name
        new_album_document.save()

        with open(album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))

        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        # nan_value = float("NaN")
        # new_account_df = new_account_df.replace("", nan_value)
        # new_account_df = new_account_df.dropna(subset=['Period'])
        # new_account_df = new_account_df.drop_duplicates(keep = False)
        account_file_name  = f"processed_mcps_account_output_{today_date}.csv"
        track_account = new_account_df.to_csv(account_file_name, index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = mcps_file
        new_account_document.file_name = account_file_name
        new_account_document.save()

        with open(account_file_name, 'rb') as csv:
            new_account_document.file_doc.save(account_file_name, File(csv))

        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':mcps_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        
        #send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)


    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg) 

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':mcps_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        #send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)


        
    except:
        print("Unexpected error!")
        raise


############################
#     DITTO                #
############################
@shared_task
def process_ditto(pk):
    ditto_file = ProcessedDocument.objects.get(pk=pk)
    try:
       
        ditto = pd.read_excel(ditto_file.file_doc, Index=None).fillna('')
        ditto.columns = ditto.columns.str.strip().str.lower()
        
        print(ditto.columns)
        if not {'isrc code', 'upc code'}.issubset(ditto.columns):
            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':ditto_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)

            
        else:
            pass

        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
        }

        album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album_raw_data = { 
            'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
        }

        for index, row in ditto.iterrows():
            track_raw_data['ISRC'].append(row['isrc code'])
            track_raw_data['DisplayUPC'].append(row['upc code'])
            track_raw_data['Artist'].append(row['primary artists (commas seperated) (required)'])
            track_raw_data['Title'].append(row['track titles (required)'])
            track_raw_data['Genre'].append(row['genre (required)'])
            track_raw_data['Release Name'].append(row['release title (required)'])
            track_raw_data['Recording Artist'].append(row['primary artists (commas seperated) (required)'])
            track_raw_data['Publisher'].append(row['publisher (required)'])
            track_raw_data['LabelName'].append(row['label (required)'])
            track_raw_data['Territories'].append(row['territories to deliver (world or comma seperated list of country codes br,gb,us) (required)'])
            track_raw_data['SalesStartDate'].append(row['digital release date'])


            album_raw_data['Product UPC'].append(row['upc code'])
            album_raw_data['Total Volumes'].append(row['volume number (required)'])
            album_raw_data['Genre'].append(row['genre (required)'])
            album_raw_data['Product Title'].append(row['release title (required)'])
            album_raw_data['Release Date'].append(row['digital release date'])
            album_raw_data['Product Artist'].append(row['primary artists (commas seperated) (required)'])
            album_raw_data['Imprint Label'].append(row['label (required)'])
            album_raw_data['Catalog Number'].append(row['track catalog number'])

            check_track = Track.objects.filter(isrc=row['isrc code']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if not check_track.display_upc:
                    print(f"updating display upc ")
                    check_track.display_upc = row['upc code']
                if 'primary artists (commas seperated) (required)' in ditto.columns:
                    if not check_track.artist:
                        print(f"updating track artist")
                        check_track.artist = row['Primary Artists (commas seperated) (required)']
                    if not check_track.recording_artist:
                        print(f"updating recording artist")
                        check_track.recording_artist = row['Primary Artists (commas seperated) (required)']
                if 'track titles (required)' in ditto.columns:
                    if not check_track.title:
                        print(f"updating title ")
                        check_track.title = row['track titles (required)']
                if 'genre (required)' in ditto.columns:
                    if not check_track.genre:
                        check_track.genre = row['genre (required)']
                if 'release title (required)' in ditto.columns:
                    if not check_track.release_name:
                        print(f"updating release name ")
                        check_track.release_name = row['release title (required)']
                if 'publisher (required)' in ditto.columns:
                    if not check_track.publisher:
                        print(f"updating publisher ")
                        check_track.publisher = row['publisher (required)']
                if 'label (required)' in ditto.columns:
                    if not check_track.label_name:
                        print(f"updating label name ")
                        check_track.label_name = row['label (required)']
                if 'territories to deliver (world or comma seperated list of country codes br,gb,us) (required)' in ditto.columns:
                    if not check_track.territories:
                        check_track.territories = row['territories to deliver (world or comma seperated list of country codes br,gb,us) (required)']
                if 'digital release date' in ditto.columns:
                    if not check_track.sales_start_date:
                        check_track.sales_start_date = row['digital release date']
                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_upc= row['upc code']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    if 'volume number (required)' in ditto.columns:
                        if not check_track_album.total_volume:
                            check_track_album.total_volume = row['volume number (required)']
                    if 'release title (required)' in ditto.columns:
                        if not check_track_album.product_title:
                            check_track_album.product_title = row['release title (required)']
                    if 'genre (required)' in ditto.columns:
                        if not check_track_album.genre:
                            check_track_album.genre = row['genre (required)']
                    if 'digital release date' in ditto.columns:
                        if not check_track_album.release_date:
                            check_track_album.release_date = row['digital release date']
                    if 'primary artists (commas seperated) (required)' in ditto.columns:
                        if not check_track_album.product_artist:
                            print(f"updating product artist")
                            check_track_album.product_artist =  row['primary artists (commas seperated) (required)']
                    if 'label (required)' in ditto.columns:
                        if not check_track_album.imprint_label:
                            check_track_album.imprint_label = row['label (required)']
                    if 'track catalog number' in ditto.columns:
                        if not check_track_album.catalog_number:
                            check_track_album.catalog_number = row['track catalog number']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['isrc code'],genre=row['genre (required)'], display_upc= row['upc code'], title = row['track titles (required)'], recording_artist=row['primary artists (commas seperated) (required)'], artist = row['primary artists (commas seperated) (required)'] , release_name=row['release title (required)'], label_name=row['label (required)'], publisher=row['publisher (required)'], territories = row['territories to deliver (world or comma seperated list of country codes br,gb,us) (required)'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['upc code'] , product_title = row['release title (required)'], product_artist = row['primary artists (commas seperated) (required)'], genre = row['genre (required)'], release_date = row['digital release date'], imprint_label = row['label (required)'], catalog_number = row['track catalog number'],  processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, display_upc = row['upc code'], genre=row['genre (required)'], title  = row['track titles (required)'], isrc = row['isrc code'], recording_artist  = row['primary artists (commas seperated) (required)'], artist  = row['primary artists (commas seperated) (required)'], release_name  = row['release title (required)'], label_name = row['label (required)'],territories = row['territories to deliver (world or comma seperated list of country codes br,gb,us) (required)'], publisher  = row['publisher (required)'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()

        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_ditto_track_output_{today_date}.csv"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = ditto_file
        new_track_document.file_name = track_file_name
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_ditto_album_output_{today_date}.csv"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = ditto_file
        new_album_document.file_name = album_file_name
        new_album_document.save()

        with open(album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))

        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':ditto_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)


        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ditto_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise

############################
#     DITTO ACCOUNT       #
############################
@shared_task
def process_ditto_account(pk):
    ditto_file = ProcessedDocument.objects.get(pk=pk)
    try:
        ditto = pd.read_excel(ditto_file.file_doc, Index=None).fillna('')
        ditto.columns = ditto.columns.str.strip().str.lower()
        print(ditto.columns)
        if not {'manual barcode', 'isrc'}.issubset(ditto.columns):
            
            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':ditto_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)
 
            
        else:
            pass
        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount',' Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],' Project Code':[],'Product Code':[],'Subaccount':[],' Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }
        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
                        }

        album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album_raw_data = {
            'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
                        }

        for index, row in ditto.iterrows():
            if  {'manual barcode', 'isrc'}.issubset(ditto.columns):

                track_raw_data['ISRC'].append(row['isrc'])


                
                check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                if check_track_master:
                    print("DITTO CHECK TRACK MASTER EXISTS")

                if check_track_master and check_track_master.track_duration != None or '':
                    track_raw_data['TrackDuration'].append(check_track_master.track_duration)
                else:
                    track_raw_data['TrackDuration'].append("None")

                if check_track_master and check_track_master.display_upc != None or '':
                    track_raw_data['DisplayUPC'].append(check_track_master.display_upc)
                else:
                    track_raw_data['DisplayUPC'].append(row['manual barcode'])
                
                if check_track_master and check_track_master.download != None or '':
                    track_raw_data['Download'].append(check_track_master.download)
                else:
                    track_raw_data['Download'].append("None")
                
                if check_track_master and check_track_master.sales_start_date != None or '':
                    track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
                else:
                    track_raw_data['SalesStartDate'].append("None")
                
                if check_track_master and check_track_master.sales_end_date != None or '':
                    track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
                else:
                    track_raw_data['SalesEndDate'].append("None")
                
                if check_track_master and check_track_master.error != None or '':
                    track_raw_data['Error'].append(check_track_master.error)
                else:
                    track_raw_data['Error'].append("None")

                if check_track_master and check_track_master.parental_advisory != None or '':
                    track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
                else:
                    track_raw_data['ParentalAdvisory'].append("None")

                if check_track_master and check_track_master.preview_clip_start_time != None or '':
                    track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
                else:
                    track_raw_data['PreviewClipStartTime'].append("None")

                if check_track_master and check_track_master.preview_clip_duration != None or '':
                    track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
                else:
                    track_raw_data['PreviewClipDuration'].append("None")

                if check_track_master and check_track_master.song_version != None or '':
                    track_raw_data['SongVersion'].append(check_track_master.song_version)
                else:
                    track_raw_data['SongVersion'].append("None")

                if check_track_master and check_track_master.p_line != None or '':
                    track_raw_data['P_LINE'].append(check_track_master.p_line)
                else:
                    track_raw_data['P_LINE'].append("None")
                
                if check_track_master and check_track_master.track_no != None or '':
                    track_raw_data['TrackNo'].append(check_track_master.track_no)
                else:
                    track_raw_data['TrackNo'].append("None")

                if check_track_master and check_track_master.hidden_track != None or '':
                    track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
                else:
                    track_raw_data['HiddenTrack'].append("None")

                if check_track_master and check_track_master.artist != None or '':
                    track_raw_data['Artist'].append(check_track_master.artist)
                else:
                    track_raw_data['Artist'].append(row['artist display name'])
                
                if check_track_master and check_track_master.title != None or '':
                    track_raw_data['Title'].append(check_track_master.title)
                else:
                    track_raw_data['Title'].append(row['track title'])

                if check_track_master and check_track_master.release_name != None or '':
                    track_raw_data['Release Name'].append(check_track_master.release_name)
                else:
                    track_raw_data['Release Name'].append(row['track title'])

                if check_track_master and  check_track_master.recording_artist != None or '':
                    track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
                else:
                    track_raw_data['Recording Artist'].append(row['artist display name'])

                if check_track_master and check_track_master.label_name != None or '':
                    track_raw_data['LabelName'].append(check_track_master.label_name)
                else:
                    track_raw_data['LabelName'].append("None")

                if check_track_master and check_track_master.volume_no != None or '':
                    track_raw_data['VolumeNo'].append(check_track_master.volume_no)
                else:
                    track_raw_data['VolumeNo'].append("1")
                
                if check_track_master and check_track_master.genre != None or '':
                    track_raw_data['Genre'].append(check_track_master.genre)
                else:
                    track_raw_data['Genre'].append("None")

                if check_track_master and check_track_master.publisher != None or '':
                    track_raw_data['Publisher'].append(check_track_master.publisher)
                else:
                    track_raw_data['Publisher'].append("None")

                if check_track_master and check_track_master.producer != None or '':
                    track_raw_data['Producer'].append(check_track_master.producer)
                else:
                    track_raw_data['Producer'].append("None")

                if check_track_master and check_track_master.writer != None or '':
                    track_raw_data['Writer'].append(check_track_master.writer)
                else:
                    track_raw_data['Writer'].append("None")
                
                if check_track_master and check_track_master.arranger != None or '':
                    track_raw_data['Arranger'].append(check_track_master.arranger)
                else:
                    track_raw_data['Arranger'].append("None")

                if check_track_master and check_track_master.territories != None or '':
                    track_raw_data['Territories'].append(check_track_master.territories)
                else:
                    track_raw_data['Territories'].append(row['country'])
                
                if check_track_master and check_track_master.exclusive != None or '':
                    track_raw_data['Exclusive'].append(check_track_master.exclusive)
                else:
                    track_raw_data['Exclusive'].append("None")

                if check_track_master and check_track_master.wholesale_price != None or '':
                    track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
                else:
                    track_raw_data['WholesalePrice'].append("None")



                
                #ALBUM CHECKS

                album_raw_data['Product UPC'].append(row['manual barcode'])
                check_album_master = Master_DRMSYS.objects.filter(display_upc=row['manual barcode'], data_type="Album").first()
                if check_album_master:
                    print("DITTO CHECK MASTER ALBUM EXISTS")

                if check_album_master and check_album_master.c_line != None or '':
                    album_raw_data['CLINE'].append(check_album_master.c_line)
                else:
                    album_raw_data['CLINE'].append("None")

                if check_album_master and check_album_master.master_carveouts != None or '':
                    album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
                else:
                    album_raw_data['Master Carveouts'].append("None")

                if check_album_master and check_album_master.territories_carveouts != None or '':
                    album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
                else:
                    album_raw_data['Territories Carveouts'].append("None")

                if check_album_master and check_album_master.deleted != None or '':
                    album_raw_data['Deleted?'].append(check_album_master.deleted)
                else:
                    album_raw_data['Deleted?'].append("None")

                if check_album_master and check_album_master.manufacturer_upc != None or '':
                    album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
                else:
                    album_raw_data['Manufacturer UPC'].append("None")

                if check_album_master and check_album_master.release_date != None or '':
                    album_raw_data['Release Date'].append(check_album_master.release_date)
                else:
                    album_raw_data['Release Date'].append("None")

                if check_album_master and check_album_master.artist_url != None or '':
                    album_raw_data['Artist URL'].append(check_album_master.artist_url)
                else:
                    album_raw_data['Artist URL'].append("None")

                if check_album_master and check_album_master.label_catalogno != None or '':
                    album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
                else:
                    album_raw_data['Catalog Number'].append("None")


                # check_master_for_cat = Master_DRMSYS.objects.filter(label_catalogno=row['catalogue number'])
                # if check_master_for_cat:
                #     total_vol = check_master_for_cat.count()
                #     album_raw_data['Total Volumes'].append(str(total_vol))
                # else:
                #     album_raw_data['Total Volumes'].append('1')
                
                if check_album_master and check_album_master.total_volumes != None or '':
                    album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
                else:
                    album_raw_data['Total Volumes'].append('1')

                if check_album_master and check_album_master.total_tracks != None or '':
                    album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
                else:
                    album_raw_data['Total Tracks'].append('1')

                if check_album_master and check_album_master.label_name != None or '':
                    album_raw_data['Imprint Label'].append(check_album_master.label_name)
                else:
                    album_raw_data['Imprint Label'].append("None")

                if check_album_master and check_album_master.genre != None or '':
                    album_raw_data['Genre'].append(check_album_master.genre)
                else:
                    album_raw_data['Genre'].append("None")

                if check_album_master and check_album_master.title != None or '':
                    album_raw_data['Product Title'].append(check_album_master.title)
                else:
                    album_raw_data['Product Title'].append(row['track title'])
                
                if check_album_master and check_album_master.artist != None or '':
                    album_raw_data['Product Artist'].append(check_album_master.artist)
                else:
                    album_raw_data['Product Artist'].append(row['artist display name'])

                


                account_raw_data['Period'].append(row['end date'])
                account_raw_data['Territory'].append(row['country'])
                account_raw_data['Activity Period'].append(f"{row['begin date']} - {row['end date']}")
                account_raw_data['Product UPC'].append(row['manual barcode'])
                account_raw_data['Manufacturer UPC'].append(row['manual barcode'])
                account_raw_data['Track Artist'].append(row['artist display name'])
                account_raw_data['ISRC'].append(row['isrc'])
                account_raw_data['Track Name'].append(row['track title'])
                account_raw_data['Unit Price'].append(row['unit price'])
                account_raw_data['Price'].append(row['unit price'])
                account_raw_data['Quantity'].append(row['units'])
                account_raw_data['Total'].append(row['earnings'])
                account_raw_data['Label Share Net Receipts'].append(row['earnings'])

                new_account = Accounting( period = row['end date'], activity_period =f"{row['begin date']} - {row['end date']}", orchard_UPC  = row['manual barcode'], manufacturer_UPC = row['manual barcode'],  artist_name = row['artist display name'], track_name = row['track title'], track_artist = row['artist display name'], isrc = row['isrc'], unit_price = row['unit price'], actual_price = row['unit price'], quantity = row['units'], total = row['earnings'], label_share_net_receipts = row['earnings'], vendor="DITTO", processed_day=today)
                new_account.save()
        
        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_ditto_track_output_{today_date}.csv"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = ditto_file
        new_track_document.file_name = track_file_name
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_ditto_album_output_{today_date}.csv"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = ditto_file
        new_album_document.file_name = album_file_name
        new_album_document.save()

        with open(album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))


        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        # nan_value = float("NaN")
        # new_account_df = new_account_df.replace("", nan_value)
        # new_account_df = new_account_df.dropna(subset=['Period'])
        # new_account_df = new_account_df.drop_duplicates(keep = False)
        account_file_name  = f"processed_ditto_account_output_{today_date}.csv"
        track_account = new_account_df.to_csv(account_file_name, index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = ditto_file
        new_account_document.file_name = account_file_name
        new_account_document.save()

        with open(account_file_name, 'rb') as csv:
            new_account_document.file_doc.save(account_file_name, File(csv))
        
        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':ditto_file.file_name,'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'

        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com' , 'digger@wyldpytch.com'], html_message=html_message)

        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ditto_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com' , 'digger@wyldpytch.com'], html_message=html_message)


        
    except:
        print("Unexpected error!")
        raise



############################
#     HORUS                #
############################
@shared_task
def process_horus(pk):
    horus_file = ProcessedDocument.objects.get(pk=pk)
    try:
        skprws = list(range(0,10))
        horus = pd.read_excel(horus_file.file_doc, skiprows=skprws, usecols = "A:BI").fillna('')
        horus.columns = horus.columns.str.strip().str.lower()
        print(horus.columns)
        if not {'barcode'}.issubset(horus.columns):
            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':horus_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
            
        else: 
            pass
        
        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
        }

        album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album_raw_data = {
            'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
        }

        # new_track_df = pd.DataFrame(raw_data, columns = track_col)
        # new_track_df.to_csv(f'processed_horus_{today}', index=False)
        


        for index, row in horus.iterrows():
            track_raw_data['DisplayUPC'].append(row['barcode'])
            track_raw_data['ISRC'].append(row['isrc'])
            track_raw_data['Genre'].append(row['genre(s)'])
            track_raw_data['Title'].append(row['track title'])
            track_raw_data['Artist'].append(row['display artist'])
            track_raw_data['Release Name'].append(row['product title'])
            track_raw_data['Recording Artist'].append(row['artist(s)'])
            track_raw_data['Publisher'].append(row['publisher(s)'])
            track_raw_data['LabelName'].append(row['label'])
            track_raw_data['Writer'].append(row['composer(s)'])
            track_raw_data['SalesStartDate'].append(row['release start date'])

            album_raw_data['Product UPC'].append(row['barcode'])
            album_raw_data['Total Volumes'].append(row['volume total'])
            album_raw_data['Product Title'].append(row['product title'])
            album_raw_data['Genre'].append(row['genre(s)'])
            album_raw_data['Release Date'].append(row['release start date'])
            album_raw_data['Product Artist'].append(row['artist(s)'])
            album_raw_data['Imprint Label'].append(row['label'])
            album_raw_data['Catalog Number'].append(row['catalogue no.'])

            # new_track_album = Album(product_upc = row['barcode'] , product_title = row['product title'], product_artist = row['artist(s)'], genre = row['genre(s)'], release_date = row['release start date'], imprint_label = row['label'], catalog_number= row['catalogue no.'],territories_carveouts= row['licensed territories to include'], processed_day=today )
            # new_track_album.save()
            # new_track = Track(isrc=row['isrc'],genre=row['genre(s)'], display_upc= row['barcode'], title = row['track title'], recording_artist=row['artist(s)'], artist = row['display artist'] , release_name=row['product title'], label_name=row['label'], publisher=row['publisher(s)'], producer=['producer(s)'],  processed_day=today)
            # new_track.save()
            # new_master = Master_DRMSYS( artist = new_track.artist, data_type="Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"-{new_track.isrc}" , track = new_track , processed_day=today)
            # new_master.save() 
            # new_master = Master_DRMSYS( artist = new_track_album.product_artist, data_type="Album", display_upc = new_track_album.product_upc, label_name = new_track_album.imprint_label, recording_artist = new_track_album.product_artist, title = new_track_album.product_title, zIDKey_UPCISRC = f"{new_track_album.product_upc}" , album = new_track_album , processed_day=today)
            # new_master.save()
            check_track = Track.objects.filter(isrc=row['isrc']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if 'barcode' in horus.columns:
                    if not check_track.display_upc:
                        print(f"updating display upc ")
                        check_track.display_upc = row['barcode']
                if 'display artist' in horus.columns:
                    if not check_track.artist:
                        print(f"updating track artist")
                        check_track.artist = row['display artist']
                if 'track title' in horus.columns:
                    if not check_track.title:
                        print(f"updating title")
                        check_track.title = row['track title']
                if 'genre(s)' in horus.columns:
                    if not check_track.genre:
                        check_track.genre = row['genre(s)']
                if 'product title' in horus.columns:
                    if not check_track.release_name:
                        print(f"updating release name ")
                        check_track.release_name = row['product title']
                if 'artist(s)' in horus.columns:
                    if not check_track.recording_artist:
                        print(f"updating recording artist")
                        check_track.recording_artist = row['artist(s)']
                if 'publisher(s)' in horus.columns:
                    if not check_track.publisher:
                        print(f"updating publisher ")
                        check_track.publisher = row['publisher(s)']
                if 'label' in horus.columns:
                    if not check_track.label_name:
                        print(f"updating label name ")
                        check_track.label_name = row['label']
                if 'composer(s)' in horus.columns:
                    if not check_track.writer:
                        check_track.writer = row['composer(s)']
                if 'release start date' in horus.columns:
                    if not check_track.sales_start_date:
                        check_track.sales_start_date = row['release start date']
                check_track.save()

            else:
                check_track_album = Album.objects.filter(product_upc= row['barcode']).first()
                if check_track_album:
                    print(f"track album {check_track_album.product_upc} exists, creating track now! ")
                    if 'volume total' in horus.columns:
                        if not check_track_album.total_volume:
                            check_track_album.total_volume = row['volume total']
                    if 'product title' in horus.columns:
                        if not check_track_album.product_title:
                            check_track_album.product_title = row['product title']
                    if 'genre(s)' in horus.columns:
                        if not check_track_album.genre:
                            check_track_album.genre = row['genre(s)']
                    if 'release start date' in horus.columns:
                        if not check_track_album.release_date:
                            check_track_album.release_date = row['release start date']
                    if 'artist(s)' in horus.columns:
                        if not check_track_album.product_artist:
                            print(f"updating product artist")
                            check_track_album.product_artist =  row['artist(s)']
                    if 'label' in horus.columns:
                        if not check_track_album.imprint_label:
                            check_track_album.imprint_label = row['label']
                    if 'catalogue no.' in horus.columns:
                        if not check_track_album.catalog_number:
                            check_track_album.catalog_number = row['catalogue no.']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['isrc'],genre=row['genre(s)'], display_upc= row['barcode'], title = row['track title'], recording_artist=row['artist(s)'], artist = row['display artist'] , release_name=row['product title'], label_name=row['label'], publisher=row['publisher(s)'], processed_day=today)
                    new_track.save()

                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['barcode'] , product_title = row['product title'], product_artist = row['artist(s)'], genre = row['genre(s)'], release_date = row['release start date'], imprint_label = row['label'], catalog_number= row['catalogue no.'],territories_carveouts= row['licensed territories to include'], processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, isrc=row['isrc'],genre=row['genre(s)'], display_upc= row['barcode'], title = row['track title'], recording_artist=row['artist(s)'], artist = row['display artist'] , release_name=row['product title'], label_name=row['label'], publisher=row['publisher(s)'], producer=['producer(s)'],  processed_day=today)
                    new_track.save() 
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type="Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()

        
        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_horus_track_output_{today_date}.csv"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = horus_file
        new_track_document.file_name = track_file_name
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_horus_album_output_{today_date}.csv"
        album_file = new_album_df.to_csv( album_file_name, index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = horus_file
        new_album_document.file_name = album_file_name
        new_album_document.save()

        with open(album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))
        

        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':horus_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url})
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)


        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':horus_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise



############################
#     HORUS ACCOUNT         #
############################

@shared_task
def process_horus_account(pk):
    horus_file = ProcessedDocument.objects.get(pk=pk)
    statement_type = None
    p = lambda x: x/100
    try:
        horus = pd.read_excel(horus_file.file_doc, Index=None).fillna('')
        # nan_value = float("NaN")
        # horus = horus.replace("", nan_value)
        # horus = horus.dropna(subset=['ISRC'])
        horus.columns = horus.columns.str.strip().str.lower()
        print(horus.columns)

        if {'asset channel id', 'asset id'}.issubset(horus.columns):
            statement_type = 'yt'
        else:
            statement_type = 'cisk'

        if statement_type == 'cisk':

            if not {'barcode', 'isrc'}.issubset(horus.columns):

                subject = 'Corrupted File'
                html_message = render_to_string('mail/corrupt.html', {'file_name':horus_file.file_name, })
                plain_message = strip_tags(html_message)
                from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
                
                send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
            
                
            else:
                pass

            track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
            track_raw_data = {
                'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
                        }

            album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
            album_raw_data = {
                'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
                            }

            account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
            account_raw_data = {
                    'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                                }

            for index, row in horus.iterrows():
                if  {'barcode', 'isrc'}.issubset(horus.columns):

                    track_raw_data['ISRC'].append(row['isrc'])
                    check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                    if check_track_master:
                        print("HORUS CHECK TRACK MASTER EXISTS")

                    if check_track_master and check_track_master.track_duration != None or '':
                        track_raw_data['TrackDuration'].append(check_track_master.track_duration)
                    else:
                        track_raw_data['TrackDuration'].append("None")

                    if check_track_master and check_track_master.display_upc != None or '':
                        track_raw_data['DisplayUPC'].append(check_track_master.display_upc)
                    else:
                        track_raw_data['DisplayUPC'].append(row['barcode'])
                    
                    if check_track_master and check_track_master.download != None or '':
                        track_raw_data['Download'].append(check_track_master.download)
                    else:
                        track_raw_data['Download'].append("None")
                    
                    if check_track_master and check_track_master.sales_start_date != None or '':
                        track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
                    else:
                        track_raw_data['SalesStartDate'].append("None")
                    
                    if check_track_master and check_track_master.sales_end_date != None or '':
                        track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
                    else:
                        track_raw_data['SalesEndDate'].append("None")
                    
                    if check_track_master and check_track_master.error != None or '':
                        track_raw_data['Error'].append(check_track_master.error)
                    else:
                        track_raw_data['Error'].append("None")

                    if check_track_master and check_track_master.parental_advisory != None or '':
                        track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
                    else:
                        track_raw_data['ParentalAdvisory'].append("None")

                    if check_track_master and check_track_master.preview_clip_start_time != None or '':
                        track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
                    else:
                        track_raw_data['PreviewClipStartTime'].append("None")

                    if check_track_master and check_track_master.preview_clip_duration != None or '':
                        track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
                    else:
                        track_raw_data['PreviewClipDuration'].append("None")

                    if check_track_master and check_track_master.song_version != None or '':
                        track_raw_data['SongVersion'].append(check_track_master.song_version)
                    else:
                        track_raw_data['SongVersion'].append("None")

                    if check_track_master and check_track_master.p_line != None or '':
                        track_raw_data['P_LINE'].append(check_track_master.p_line)
                    else:
                        track_raw_data['P_LINE'].append("None")
                    
                    if check_track_master and check_track_master.track_no != None or '':
                        track_raw_data['TrackNo'].append(check_track_master.track_no)
                    else:
                        track_raw_data['TrackNo'].append("None")

                    if check_track_master and check_track_master.hidden_track != None or '':
                        track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
                    else:
                        track_raw_data['HiddenTrack'].append("None")

                    if check_track_master and check_track_master.artist != None or '':
                        track_raw_data['Artist'].append(check_track_master.artist)
                    else:
                        track_raw_data['Artist'].append(row['artist'])
                    
                    if check_track_master and check_track_master.title != None or '':
                        track_raw_data['Title'].append(check_track_master.title)
                    else:
                        track_raw_data['Title'].append(row['track title'])

                    if check_track_master and check_track_master.release_name != None or '':
                        track_raw_data['Release Name'].append(check_track_master.release_name)
                    else:
                        track_raw_data['Release Name'].append(row['release title'])

                    if check_track_master and  check_track_master.recording_artist != None or '':
                        track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
                    else:
                        track_raw_data['Recording Artist'].append(row['artist'])

                    if check_track_master and check_track_master.label_name != None or '':
                        track_raw_data['LabelName'].append(check_track_master.label_name)
                    else:
                        track_raw_data['LabelName'].append(row['label'])

                    if check_track_master and check_track_master.volume_no != None or '':
                        track_raw_data['VolumeNo'].append(check_track_master.volume_no)
                    else:
                        track_raw_data['VolumeNo'].append(row['quantity'])
                    
                    if check_track_master and check_track_master.genre != None or '':
                        track_raw_data['Genre'].append(check_track_master.genre)
                    else:
                        track_raw_data['Genre'].append("None")

                    if check_track_master and check_track_master.publisher != None or '':
                        track_raw_data['Publisher'].append(check_track_master.publisher)
                    else:
                        track_raw_data['Publisher'].append("None")

                    if check_track_master and check_track_master.producer != None or '':
                        track_raw_data['Producer'].append(check_track_master.producer)
                    else:
                        track_raw_data['Producer'].append("None")

                    if check_track_master and check_track_master.writer != None or '':
                        track_raw_data['Writer'].append(check_track_master.writer)
                    else:
                        track_raw_data['Writer'].append("None")
                    
                    if check_track_master and check_track_master.arranger != None or '':
                        track_raw_data['Arranger'].append(check_track_master.arranger)
                    else:
                        track_raw_data['Arranger'].append("None")

                    if check_track_master and check_track_master.territories != None or '':
                        track_raw_data['Territories'].append(check_track_master.territories)
                    else:
                        track_raw_data['Territories'].append(row['country of sale'])
                    
                    if check_track_master and check_track_master.exclusive != None or '':
                        track_raw_data['Exclusive'].append(check_track_master.exclusive)
                    else:
                        track_raw_data['Exclusive'].append("None")

                    if check_track_master and check_track_master.wholesale_price != None or '':
                        track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
                    else:
                        track_raw_data['WholesalePrice'].append("None")

                    

                    #ALBUM CHECKS

                    album_raw_data['Product UPC'].append(row['barcode'])
                    check_album_master = Master_DRMSYS.objects.filter(display_upc=row['barcode'], data_type="Album").first()
                    if check_album_master:
                        print("PPL CHECK MASTER ALBUM EXISTS")

                    if check_album_master and check_album_master.c_line != None or '':
                        album_raw_data['CLINE'].append(check_album_master.c_line)
                    else:
                        album_raw_data['CLINE'].append("None")

                    if check_album_master and check_album_master.master_carveouts != None or '':
                        album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
                    else:
                        album_raw_data['Master Carveouts'].append("None")

                    if check_album_master and check_album_master.territories_carveouts != None or '':
                        album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
                    else:
                        album_raw_data['Territories Carveouts'].append("None")

                    if check_album_master and check_album_master.deleted != None or '':
                        album_raw_data['Deleted?'].append(check_album_master.deleted)
                    else:
                        album_raw_data['Deleted?'].append("None")

                    if check_album_master and check_album_master.manufacturer_upc != None or '':
                        album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
                    else:
                        album_raw_data['Manufacturer UPC'].append("None")

                    if check_album_master and check_album_master.release_date != None or '':
                        album_raw_data['Release Date'].append(check_album_master.release_date)
                    else:
                        album_raw_data['Release Date'].append("None")

                    if check_album_master and check_album_master.artist_url != None or '':
                        album_raw_data['Artist URL'].append(check_album_master.artist_url)
                    else:
                        album_raw_data['Artist URL'].append("None")

                    if check_album_master and check_album_master.label_catalogno != None or '':
                        album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
                    else:
                        album_raw_data['Catalog Number'].append(row['catalogue number'])


                    check_master_for_cat = Master_DRMSYS.objects.filter(label_catalogno=row['catalogue number'])
                    if check_master_for_cat:
                        total_vol = check_master_for_cat.count()
                        album_raw_data['Total Volumes'].append(str(total_vol))
                    else:
                        album_raw_data['Total Volumes'].append('1')
                    
                    # if check_album_master and check_album_master.total_volumes != None or '':
                    #     album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
                    # else:
                    #     album_raw_data['Total Volumes'].append(row['quantity'])

                    if check_album_master and check_album_master.total_tracks != None or '':
                        album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
                    else:
                        album_raw_data['Total Tracks'].append(1)

                    if check_album_master and check_album_master.label_name != None or '':
                        album_raw_data['Imprint Label'].append(check_album_master.label_name)
                    else:
                        album_raw_data['Imprint Label'].append(row['label'])

                    if check_album_master and check_album_master.genre != None or '':
                        album_raw_data['Genre'].append(check_album_master.genre)
                    else:
                        album_raw_data['Genre'].append("None")

                    if check_album_master and check_album_master.title != None or '':
                        album_raw_data['Product Title'].append(check_album_master.title)
                    else:
                        album_raw_data['Product Title'].append(row['track title'])
                    
                    if check_album_master and check_album_master.artist != None or '':
                        album_raw_data['Product Artist'].append(check_album_master.artist)
                    else:
                        album_raw_data['Product Artist'].append(row['artist'])




                    account_raw_data['Period'].append(row['sales period'])
                    account_raw_data['Activity Period'].append(f"{row['date of sale']} - {row['date entered']} ")
                    account_raw_data['Retailer'].append(row['store'])
                    account_raw_data['Product UPC'].append(row['barcode'])
                    account_raw_data['Territory'].append(row['country of sale'])
                    account_raw_data['Project Code'].append(row['track title'])
                    account_raw_data['Product Code'].append(row['barcode'])
                    account_raw_data['Imprint Label'].append(row['label'])
                    account_raw_data['Artist Name'].append(row['artist'])
                    account_raw_data['Product Name'].append(row['track title'])
                    account_raw_data['ISRC'].append(row['isrc'])
                    account_raw_data['Volume'].append(row['quantity'])
                    account_raw_data['Unit Price'].append(row['amount paid by store'])
                    # account_raw_data['Discount'].append(row['withhold'])
                    account_raw_data['Price'].append(row['amount remaining'])
                    account_raw_data['Quantity'].append(row['quantity'])
                    account_raw_data['Total'].append(row['amount remaining'])
                    account_raw_data['Label Share Net Receipts'].append(row['line total to be paid to you'])


                    new_account = Accounting( period = row['sales period'], activity_period =f"{row['date of sale']} - {row['date entered']} ", retailer = row['store'], territory = row['country of sale'], orchard_UPC = row['barcode'], manufacturer_UPC = row['barcode'], imprint_label = row['label'], artist_name = row['artist'], product_name = row['track title'], track_name = row['track title'], track_artist = row['artist'], isrc = row['isrc'], volume = row['quantity'], unit_price = row['amount paid by store'], actual_price = row['amount remaining'], quantity = row['amount remaining'], total = row['amount remaining'], label_share_net_receipts = row['line total to be paid to you'], vendor="HORUS", processed_day=today)
                    new_account.save()

            print(account_raw_data) 
            new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
            # trk_name = f'processed_horus_{today}_track.csv'


            # nan_value = float("NaN")
            # new_account_df = new_account_df.replace("", nan_value)
            # new_account_df = new_account_df.dropna(subset=['Period'])
            # new_account_df = new_account_df.drop_duplicates(keep = False)


            account_file_name  = f"processed_horus_account_output_{today_date}.csv"
            track_account = new_account_df.to_csv(account_file_name,  index = False, header=True)
            new_account_document = ProcessedAccountFile()
            new_account_document.document = horus_file
            new_account_document.file_name = account_file_name
            new_account_document.save()

            with open(account_file_name, 'rb') as csv:
                new_account_document.file_doc.save(account_file_name, File(csv))

            print(track_raw_data) 
            new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
            # trk_name = f'processed_horus_{today}_track.csv'
            new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
            track_file_name  = f"processed_horus_track_output_{today_date}.csv"
            track_file = new_track_df.to_csv(track_file_name,  index = False, header=True)
            new_track_document = ProcessedTrackFile()
            new_track_document.document = horus_file
            new_track_document.file_name = track_file_name
            new_track_document.save()

            with open(track_file_name, 'rb') as csv:
                new_track_document.file_doc.save(track_file_name, File(csv))

            print(album_raw_data)
            new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
            new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
            album_file_name  = f"processed_horus_album_output_{today_date}.csv"
            album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
            # alb_name = f'processed_horus_{today}_album.csv'
            new_album_document = ProcessedAlbumFile()
            new_album_document.document = horus_file
            new_album_document.file_name = album_file_name
            new_album_document.save()

            with open(album_file_name, 'rb') as csv: 
                new_album_document.file_doc.save(album_file_name, File(csv))
            

            subject = 'File Processed'
            html_message = render_to_string('mail/processed.html', {'file_name':horus_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

            
        # YOUTUBE STATEMENT TYPE VARIATION  
        elif statement_type == 'yt':
            if not {'upc', 'isrc'}.issubset(horus.columns):

                subject = 'Corrupted File'
                html_message = render_to_string('mail/corrupt.html', {'err_msg':err_msg, 'file_name':horus_file.file_name, })
                plain_message = strip_tags(html_message)
                from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
                
                send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
            else:
                pass

            track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
            track_raw_data = {
                'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
                        }

            album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
            album_raw_data = {
                'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
                            }

            account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
            account_raw_data = {
                    'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                                }

            for index, row in horus.iterrows():
                if  {'upc', 'isrc'}.issubset(horus.columns):

                    track_raw_data['ISRC'].append(row['isrc'])

                    check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                    if check_track_master:
                        print("HORUS CHECK TRACK MASTER EXISTS")

                    if check_track_master and check_track_master.track_duration != None or '':
                        track_raw_data['TrackDuration'].append(check_track_master.track_duration)
                    else:
                        track_raw_data['TrackDuration'].append("None")

                    if check_track_master and check_track_master.display_upc != None or '':
                        track_raw_data['DisplayUPC'].append(check_track_master.display_upc)
                    else:
                        track_raw_data['DisplayUPC'].append(row['upc'])
                    
                    if check_track_master and check_track_master.download != None or '':
                        track_raw_data['Download'].append(check_track_master.download)
                    else:
                        track_raw_data['Download'].append("None")
                    
                    if check_track_master and check_track_master.sales_start_date != None or '':
                        track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
                    else:
                        track_raw_data['SalesStartDate'].append("None")
                    
                    if check_track_master and check_track_master.sales_end_date != None or '':
                        track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
                    else:
                        track_raw_data['SalesEndDate'].append("None")
                    
                    if check_track_master and check_track_master.error != None or '':
                        track_raw_data['Error'].append(check_track_master.error)
                    else:
                        track_raw_data['Error'].append("None")

                    if check_track_master and check_track_master.parental_advisory != None or '':
                        track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
                    else:
                        track_raw_data['ParentalAdvisory'].append("None")

                    if check_track_master and check_track_master.preview_clip_start_time != None or '':
                        track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
                    else:
                        track_raw_data['PreviewClipStartTime'].append("None")

                    if check_track_master and check_track_master.preview_clip_duration != None or '':
                        track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
                    else:
                        track_raw_data['PreviewClipDuration'].append("None")

                    if check_track_master and check_track_master.song_version != None or '':
                        track_raw_data['SongVersion'].append(check_track_master.song_version)
                    else:
                        track_raw_data['SongVersion'].append("None")

                    if check_track_master and check_track_master.p_line != None or '':
                        track_raw_data['P_LINE'].append(check_track_master.p_line)
                    else:
                        track_raw_data['P_LINE'].append("None")
                    
                    if check_track_master and check_track_master.track_no != None or '':
                        track_raw_data['TrackNo'].append(check_track_master.track_no)
                    else:
                        track_raw_data['TrackNo'].append("None")

                    if check_track_master and check_track_master.hidden_track != None or '':
                        track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
                    else:
                        track_raw_data['HiddenTrack'].append("None")

                    if check_track_master and check_track_master.artist != None or '':
                        track_raw_data['Artist'].append(check_track_master.artist)
                    else:
                        track_raw_data['Artist'].append(row['artist'])
                    
                    if check_track_master and check_track_master.title != None or '':
                        track_raw_data['Title'].append(check_track_master.title)
                    else:
                        track_raw_data['Title'].append(row['asset title'])

                    if check_track_master and check_track_master.release_name != None or '':
                        track_raw_data['Release Name'].append(check_track_master.release_name)
                    else:
                        track_raw_data['Release Name'].append(row['asset title'])

                    if check_track_master and  check_track_master.recording_artist != None or '':
                        track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
                    else:
                        track_raw_data['Recording Artist'].append(row['artist'])

                    if check_track_master and check_track_master.label_name != None or '':
                        track_raw_data['LabelName'].append(check_track_master.label_name)
                    else:
                        track_raw_data['LabelName'].append(row['label'])

                    if check_track_master and check_track_master.volume_no != None or '':
                        track_raw_data['VolumeNo'].append(check_track_master.volume_no)
                    else:
                        track_raw_data['VolumeNo'].append(1)
                    
                    if check_track_master and check_track_master.genre != None or '':
                        track_raw_data['Genre'].append(check_track_master.genre)
                    else:
                        track_raw_data['Genre'].append("None")

                    if check_track_master and check_track_master.publisher != None or '':
                        track_raw_data['Publisher'].append(check_track_master.publisher)
                    else:
                        track_raw_data['Publisher'].append("None")

                    if check_track_master and check_track_master.producer != None or '':
                        track_raw_data['Producer'].append(check_track_master.producer)
                    else:
                        track_raw_data['Producer'].append("None")

                    if check_track_master and check_track_master.writer != None or '':
                        track_raw_data['Writer'].append(check_track_master.writer)
                    else:
                        track_raw_data['Writer'].append("None")
                    
                    if check_track_master and check_track_master.arranger != None or '':
                        track_raw_data['Arranger'].append(check_track_master.arranger)
                    else:
                        track_raw_data['Arranger'].append("None")

                    if check_track_master and check_track_master.territories != None or '':
                        track_raw_data['Territories'].append(check_track_master.territories)
                    else:
                        track_raw_data['Territories'].append(row['country'])
                    
                    if check_track_master and check_track_master.exclusive != None or '':
                        track_raw_data['Exclusive'].append(check_track_master.exclusive)
                    else:
                        track_raw_data['Exclusive'].append("None")

                    if check_track_master and check_track_master.wholesale_price != None or '':
                        track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
                    else:
                        track_raw_data['WholesalePrice'].append("None")

                    

                    #ALBUM CHECKS

                    album_raw_data['Product UPC'].append(row['upc'])
                    check_album_master = Master_DRMSYS.objects.filter(display_upc=row['upc'], data_type="Album").first()
                    if check_album_master:
                        print("PPL CHECK MASTER ALBUM EXISTS")

                    if check_album_master and check_album_master.c_line != None or '':
                        album_raw_data['CLINE'].append(check_album_master.c_line)
                    else:
                        album_raw_data['CLINE'].append("None")

                    if check_album_master and check_album_master.master_carveouts != None or '':
                        album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
                    else:
                        album_raw_data['Master Carveouts'].append("None")

                    if check_album_master and check_album_master.territories_carveouts != None or '':
                        album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
                    else:
                        album_raw_data['Territories Carveouts'].append("None")

                    if check_album_master and check_album_master.deleted != None or '':
                        album_raw_data['Deleted?'].append(check_album_master.deleted)
                    else:
                        album_raw_data['Deleted?'].append("None")

                    if check_album_master and check_album_master.manufacturer_upc != None or '':
                        album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
                    else:
                        album_raw_data['Manufacturer UPC'].append("None")

                    if check_album_master and check_album_master.release_date != None or '':
                        album_raw_data['Release Date'].append(check_album_master.release_date)
                    else:
                        album_raw_data['Release Date'].append("None")

                    if check_album_master and check_album_master.artist_url != None or '':
                        album_raw_data['Artist URL'].append(check_album_master.artist_url)
                    else:
                        album_raw_data['Artist URL'].append("None")

                    if check_album_master and check_album_master.label_catalogno != None or '':
                        album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
                    else:
                        album_raw_data['Catalog Number'].append(row['asset id'])


                    if check_album_master and check_album_master.total_volumes != None or '':
                        album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
                    else:
                        album_raw_data['Total Volumes'].append(row['owned views'])

                    if check_album_master and check_album_master.total_tracks != None or '':
                        album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
                    else:
                        album_raw_data['Total Tracks'].append(1)

                    if check_album_master and check_album_master.label_name != None or '':
                        album_raw_data['Imprint Label'].append(check_album_master.label_name)
                    else:
                        album_raw_data['Imprint Label'].append(row['label'])

                    if check_album_master and check_album_master.genre != None or '':
                        album_raw_data['Genre'].append(check_album_master.genre)
                    else:
                        album_raw_data['Genre'].append("None")

                    if check_album_master and check_album_master.title != None or '':
                        album_raw_data['Product Title'].append(check_album_master.title)
                    else:
                        album_raw_data['Product Title'].append(row['asset title'])
                    
                    if check_album_master and check_album_master.artist != None or '':
                        album_raw_data['Product Artist'].append(check_album_master.artist)
                    else:
                        album_raw_data['Product Artist'].append(row['artist'])


                    account_raw_data['Period'].append(row['day'])
                    # account_raw_data['Activity Period'].append(f"{row['date of sale']} - {row['date entered']} ")
                    account_raw_data['Retailer'].append('youtube')
                    account_raw_data['Product UPC'].append(row['upc'])
                    account_raw_data['Territory'].append(row['country'])
                    account_raw_data['Project Code'].append(row['asset title'])
                    account_raw_data['Product Code'].append(row['asset id'])
                    account_raw_data['Imprint Label'].append(row['label'])
                    account_raw_data['Artist Name'].append(row['artist'])
                    account_raw_data['Product Name'].append(row['asset title'])
                    account_raw_data['ISRC'].append(row['isrc'])
                    account_raw_data['Volume'].append(row['owned views'])
                    #account_raw_data['Unit Price'].append(row['amount paid by store'])
                    # account_raw_data['Discount'].append(row['withhold'])
                    account_raw_data['Price'].append(row['youtube revenue split'])
                    account_raw_data['Quantity'].append(row['owned views'])
                    
                    revenue = row['partner revenue'] - row['partner revenue'] * p(20)
                    account_raw_data['Total'].append(revenue)
                    account_raw_data['Label Share Net Receipts'].append(revenue)


                    new_account = Accounting( period = row['day'], retailer = 'youtube', territory = row['country'], orchard_UPC = row['upc'], manufacturer_UPC = row['upc'], imprint_label = row['label'], artist_name = row['artist'], product_name = row['asset title'], track_name = row['asset title'], track_artist = row['artist'], isrc = row['isrc'], volume = row['owned views'],  quantity = row['owned views'], total = revenue, label_share_net_receipts = revenue, vendor="HORUS", processed_day=today)
                    new_account.save()

            print(account_raw_data) 
            new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
            # trk_name = f'processed_horus_{today}_track.csv'
            nan_value = float("NaN")
            new_account_df = new_account_df.replace("", nan_value)
            new_account_df = new_account_df.dropna(subset=['Period'])
            new_account_df = new_account_df.drop_duplicates(keep = False)
            account_file_name  = f"processed_horus_account_output_{today_date}.csv"
            track_account = new_account_df.to_csv(account_file_name,  index = False, header=True)
            new_account_document = ProcessedAccountFile()
            new_account_document.document = horus_file
            new_account_document.file_name = account_file_name
            new_account_document.save()

            with open(account_file_name, 'rb') as csv:
                new_account_document.file_doc.save(account_file_name, File(csv))

            print(track_raw_data) 
            new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
            # trk_name = f'processed_horus_{today}_track.csv'
            new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
            track_file_name  = f"processed_horus_track_output_{today_date}.csv"
            track_file = new_track_df.to_csv(track_file_name,  index = False, header=True)
            new_track_document = ProcessedTrackFile()
            new_track_document.document = horus_file
            new_track_document.file_name = track_file_name
            new_track_document.save()

            with open(track_file_name, 'rb') as csv:
                new_track_document.file_doc.save(track_file_name, File(csv))

            print(album_raw_data)
            new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
            new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
            album_file_name  = f"processed_horus_album_output_{today_date}.csv"
            album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
            # alb_name = f'processed_horus_{today}_album.csv'
            new_album_document = ProcessedAlbumFile()
            new_album_document.document = horus_file
            new_album_document.file_name = album_file_name
            new_album_document.save()

            with open(album_file_name, 'rb') as csv:
                new_album_document.file_doc.save(album_file_name, File(csv))
            
            subject = 'File Processed'
            html_message = render_to_string('mail/processed.html', {'file_name':horus_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':horus_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

       
    except:
        print("Unexpected error!")
        raise







############################
#     BMI                  #
############################
# @shared_task
# def process_bmi(filename):
#     bmi_file = ProcessedDocument.objects.filter(file_name=filename).first()
#     try:
#         #col_list = [ 'PERIOD', 'W OR P', 'PARTICIPANT NAME',	 'PARTICIPANT #', 'IP #', 'TITLE NAME',	 'ARTISTS ', 'LABEL IMPRINT', 'TITLE #', 'PERF SOURCE', 'COUNTRY OF PERFORMANCE',	 'SHOW NAME', 'EPISODE NAME', 'SHOW #', 'USE CODE', 'TIMING', 'PARTICIPANT %', 'PERF COUNT', 'BONUS LEVEL', 'ROYALTY AMOUNT', 'WITHHOLD', 'PERF PERIOD', 'CURRENT ACTIVITY AMT', 'HITS SONG OR TV NET SUPER USAGE BONUS', 'STANDARDS OR TV NET THEME BONUS', 'FOREIGN SOCIETY ADJUSTMENT', 'COMPANY CODE',	 'COMPANY NAME' ]
#         bmi = pd.read_excel(bmi_file.file_doc, Index=None).fillna('')
#         bmi.columns = bmi.columns.str.strip().str.lower()
       
#         print(bmi.columns)
#         if not {'title #', 'ip #'}.issubset(bmi.columns):
#             send_mail(
#                             'Corrupted File',
#                             f''' 
#                             Hello Admin 
#                             The file {bmi_file.file_name} was corrupt !
#                             ISRC or UPC missing!
#                             Pls login to run it again
#                             ''',
#                             'Trackeet Recording Panel ',
#                             ['shola.albert@gmail.com'],
#                             fail_silently=False,
#                             )
#         else:
#             pass

#         track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
#         track_raw_data = {
#             'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
#         }


#         for index, row in bmi.iterrows():
#             track_raw_data['ISRC'].append(row['title #'])
#             track_raw_data['Title'].append(row['title name'])
#             track_raw_data['Artist'].append(row['artists'])
#             track_raw_data['Recording Artist'].append(row['artists'])
#             track_raw_data['LabelName'].append(row['label imprint'])

#             check_track = Track.objects.filter(isrc=row['title #']).first()
#             if check_track:
#                 print(f" track {check_track.isrc} already exists")
#                 if 'title name' in bmi.columns:
#                     if not check_track.title:
#                         print(f"updating title ")
#                         check_track.title = row['title name']
#                 if 'artists' in bmi.columns:
#                     if not check_track.artist:
#                         check_track.artist = row['artists']
#                     if not check_track.recording_artist:
#                         print(f"updating recording artist")
#                         check_track.recording_artist = row['artists']
#                 if 'label imprint' in bmi.columns:
#                     if not check_track.label_name:
#                         print(f"updating label name ")
#                         check_track.label_name = row['label imprint']
#                 check_track.save()
#             else:
#                 print(f"track does not exists, creating track now! ")
#                 if {'title name', 'title #', 'artists'}.issubset(bmi.columns):
#                     new_track = Track( title  = row['title name'], isrc = row['title #'], recording_artist  = row['artists'], artist  = row['artists'], label_name = row['label imprint'], processed_day=today)
#                     new_track.save()
#                     new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track",  isrc = new_track.isrc, label_name = new_track.label_name, recording_artist = new_track.recording_artist, title = new_track.title, track = new_track , processed_day=today)
#                     new_master.save()
#                 else:
#                     print("Some colums are missing")

#         send_mail(
#                         'File Processed',
#                         f''' 
#                         Hello Admin 
#                         The file {bmi_file.file_name} has been successfully processed !
#                         Congratulations
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)

#         print(track_raw_data) 
#         new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
#         # trk_name = f'processed_horus_{today}_track.csv'
#         track_file = new_track_df.to_csv('processed_bmi_track.csv', index = False, header=True)
#         new_track_document = ProcessedTrackFile()
#         new_track_document.document = bmi_file
#         new_track_document.file_name = 'processed_bmi_track.csv'
#         new_track_document.save()

#         with open('processed_bmi_track.csv', 'rb') as csv:
#             new_track_document.file_doc.save('processed_bmi_track.csv', File(csv))

        

#     except (ValueError, NameError, TypeError) as error:
#         err_msg = str(error)
#         print(err_msg)
#         send_mail(
#                         'Error Processing File',
#                         f''' 
#                         Hello Admin 
#                         The file {bmi_file.file_name} is corrupt!
#                         See the details below:
#                         {err_msg}
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)
#     except:
#         print("Unexpected error!")
#         raise



#         send_mail(
#                         'File Processed',
#                         f''' 
#                         Hello Admin 
#                         The file {bmi_file.file_name} has been successfully processed !
#                         Congratulations
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)


############################
#     BMI ACCOUNT          #
############################
@shared_task
def process_bmi_account(pk):
    bmi_file = ProcessedDocument.objects.get(pk=pk)
    try:
        bmi = pd.read_excel(bmi_file.file_doc, Index=None).fillna('')
        bmi.columns = bmi.columns.str.strip().str.lower()
        print(bmi.columns)

        if not {'title #', 'ip #'}.issubset(bmi.columns):

            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':bmi_file.file_name})
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)

        else:
            pass

        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }
        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
        }
        
        album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album_raw_data = {
            'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
        }

        for index, row in bmi.iterrows():

            if  {'title #', 'ip #', 'period'}.issubset(bmi.columns):

                check_master = Master_DRMSYS.objects.filter(isrc=row['title #']).first()
                if check_master:
                    if check_master.display_upc != None or '' :
                        album_raw_data['Product UPC'].append(check_master.display_upc)
            
                    if check_master.artist != None or '' :
                        track_raw_data['Artist'].append(check_master.artist)
                        album_raw_data['Product Artist'].append(check_master.artist)
                        account_raw_data['Artist Name'].append(check_master.artist)
                    
                    if check_master.title != None or '' :
                        track_raw_data['Title'].append(check_master.title)
                        album_raw_data['Product Title'].append(check_master.title)
                    else:
                        track_raw_data['Title'].append(row['title name'])
                        album_raw_data['Product Title'].append(row['title name'])

                    if check_master.release_name != None or '' :
                        track_raw_data['Release Name'].append(check_master.release_name)
                
                    if check_master.recording_artist != None or '' :
                        track_raw_data['Recording Artist'].append(check_master.recording_artist)
                    
                    if check_master.label_name != None or '' :
                        track_raw_data['LabelName'].append(check_master.label_name)
                    
                    if check_master.volume_no != None or '' :
                        track_raw_data['VolumeNo'].append(check_master.volume_no)
                    else:
                        track_raw_data['VolumeNo'].append(1)
                    
                    if check_master.genre != None or '' :
                        track_raw_data['Genre'].append(check_master.genre)
                        album_raw_data['Genre'].append(check_master.genre)

                    if check_master.publisher != None or '' :
                        track_raw_data['Publisher'].append(check_master.publisher)

                    if check_master.writer != None or '' :
                        track_raw_data['Writer'].append(check_master.writer)

                    if check_master.territories != None or '' :
                        track_raw_data['Territories'].append(check_master.territories)

                    if check_master.release_date != None or '' :
                        album_raw_data['Release Date'].append(check_master.release_date)



                track_raw_data['ISRC'].append(row['title #'])



                account_raw_data['Period'].append(row['period'])
                account_raw_data['Activity Period'].append(row['perf period'])
                account_raw_data['Retailer'].append(row['perf source'])
                account_raw_data['Product UPC'].append(row['ip #'])
                account_raw_data['Territory'].append(row['country of performance'])
                account_raw_data['Project Code'].append(row['show name'])
                account_raw_data['Product Code'].append(row['episode name'])

                
                account_raw_data['Product Name'].append(row['title name'])
                account_raw_data['ISRC'].append(row['title #'])
                account_raw_data['Volume'].append(row['perf count'])
                account_raw_data['Unit Price'].append(row['current activity amt'])
                account_raw_data['Discount'].append(row['withhold'])
                account_raw_data['Price'].append(row['current activity amt'])
                account_raw_data['Quantity'].append(row['perf count'])
                account_raw_data['Total'].append(row['royalty amount'])
                account_raw_data['Label Share Net Receipts'].append(row['royalty amount'])

                check_track = Track.objects.filter(isrc=row['title #']).first()
                if check_track:
                    print(f" track {check_track.isrc} already exists")
                    if 'title name' in bmi.columns:
                        if not check_track.title:
                            print(f"updating title ")
                            check_track.title = row['title name']
                    
                    
                    check_track.save()
                else:
                    print(f"track does not exists, creating track now! ")
                    if {'title name', 'title #'}.issubset(bmi.columns):
                        new_track = Track( title  = row['title name'], isrc = row['title #'],  processed_day=today)
                        new_track.save()
                        new_master = Master_DRMSYS( data_type = "Track",  isrc = new_track.isrc, title = new_track.title, track = new_track , processed_day=today)
                        new_master.save()
                    else:
                        print("Some colums are missing")

                new_account = Accounting( period = row['period'], activity_period = row['perf period'], retailer = row['perf source'], territory = row['country of performance'], orchard_UPC = row['ip #'], manufacturer_UPC = row['ip #'], project_code = row['show name'], product_code = row['episode name'],  product_name = row['title name'], track_name = row['title name'], isrc = row['title #'], volume = row['perf count'], unit_price = row['current activity amt'], discount = row['withhold'], actual_price = row['current activity amt'], quantity = row['perf count'], total = row['royalty amount'], label_share_net_receipts = row['royalty amount'], vendor="BMI", processed_day=today)
                new_account.save()


        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_bmi_track_output_{today_date}.csv"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = bmi_file
        new_track_document.file_name = track_file_name
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save( track_file_name, File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name = f"processed_bmi_album_output_{today_date}.csv"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = bmi_file
        new_album_document.file_name = album_file_name
        new_album_document.save()

        with open(album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))

        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        new_account_df = new_account_df.drop_duplicates(keep = False, inplace = False)
        account_file_name  = f"processed_bmi_account_output_{today_date}.csv"
        track_account = new_account_df.to_csv(account_file_name, index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = bmi_file
        new_account_document.file_name = account_file_name
        new_account_document.save()

        with open(account_file_name, 'rb') as csv:
            new_account_document.file_doc.save(account_file_name, File(csv))

        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':bmi_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)



        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':bmi_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise




############################
#     WARNER ADA           #
############################

############################
#     ORCHARD              #
############################
@shared_task
def process_orchard(pk):
    orchard_file = ProcessedDocument.objects.get(pk=pk)
    try:
        orchard = pd.read_excel(orchard_file.file_doc, Index=None).fillna('')
        orchard.columns = orchard.columns.str.strip().str.lower()
        print(orchard.columns)
        if not {'isrc', 'digital upc'}.issubset(orchard.columns):

            print("isrc and recording id not present")
            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':orchard_file.file_name})
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)

           
        else:
            print("isrc and recording id present")
            

        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
        }

        album_col = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album_raw_data = {
            'Product UPC':[], 'Total Volumes':[], 'Total Tracks':[], 'Product Title':[],'Product Artist':[], 'Genre':[], 'CLINE':[], 'Release Date':[], 'Imprint Label':[], 'Artist URL':[], 'Catalog Number':[], 'Manufacturer UPC':[], 'Deleted?':[], 'Territories Carveouts':[], 'Master Carveouts':[]
        }

        for index, row in orchard.iterrows():
            dummy_upc = None
            dummy_isrc = None
            if row['digital upc'] == '':
                dummy_upc = str(''.join(random.choices(string.digits, k = 15)))
            else:
                dummy_upc = row['digital upc']
            
            if row['isrc'] == '':
                dummy_isrc = str(''.join(random.choices(string.ascii_uppercase + string.digits, k = 15)))
            else:
                dummy_isrc = row['isrc']


            track_raw_data['ISRC'].append(dummy_isrc)
            track_raw_data['DisplayUPC'].append(dummy_upc)
            track_raw_data['Artist'].append(row['orchard artist'])
            track_raw_data['Title'].append(row['track name'])
            track_raw_data['Release Name'].append(row['release name'])
            track_raw_data['Recording Artist'].append(row['orchard artist'])
            track_raw_data['Publisher'].append(row['publisher(s)'])
            track_raw_data['LabelName'].append(row['imprint'])
            # track_raw_data['VolumeNo'].append(row['volumne'])
            track_raw_data['Genre'].append(row['genre'])
            


            album_raw_data['Product UPC'].append(dummy_upc)
            album_raw_data['Product Title'].append(row['track name'])
            album_raw_data['Product Artist'].append(row['orchard artist'])
            album_raw_data['Genre'].append(row['genre'])
            album_raw_data['Artist URL'].append(row['artist url'])
            album_raw_data['Imprint Label'].append(row['imprint'])
            album_raw_data['Catalog Number'].append(row['folder name / label catalog number'])



            check_track = Track.objects.filter(title=row['track name']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if 'digital upc' in orchard.columns:
                    if not check_track.display_upc:
                        print(f"updating display upc ")
                        check_track.display_upc = dummy_upc
                    elif check_track.display_upc and track_raw_data['DisplayUPC'] == '':
                        track_raw_data['DisplayUPC'].append(dummy_upc)
                    else:
                        pass

                else:
                    print('digital upc  not here')
                if 'isrc' in orchard.columns:
                    if not check_track.isrc:
                        print(f"updating isrc ")
                        check_track.isrc = dummy_isrc
                    elif check_track.isrc and track_raw_data['ISRC'] == '':
                        track_raw_data['ISRC'].append(dummy_isrc)
                    else:
                        pass

                else:
                    print('isrc  not here')
                if 'orchard artist' in orchard.columns:
                    if not check_track.artist or check_track.artist != row['orchard artist']:
                        print(f"updating track artist")
                        check_track.artist = row['orchard artist']
                    elif check_track.artist and track_raw_data['Artist'] == '':
                        track_raw_data['Artist'].append(check_track.artist)
                    else:
                        pass

                    if not check_track.recording_artist or check_track.recording_artist != row['orchard artist']:
                        print(f"updating recording artist ")
                        check_track.recording_artist = row['orchard artist']
                    elif check_track.recording_artist and  track_raw_data['Recording Artist'] == '':
                         track_raw_data['Recording Artist'].append(check_track.recording_artist)
                    else:
                        pass
                else:
                    print('artist_name not here')
                
                   
                if 'publisher(s)' in orchard.columns:
                    if not check_track.publisher or check_track.publisher != row['publisher(s)']:
                        print(f"updating publisher ")
                        check_track.publisher = row['publisher(s)']
                    elif check_track.publisher and track_raw_data['Publisher'] == '':
                        track_raw_data['Publisher'].append(check_track.publisher)
                    else:
                        pass
                else:
                    print('publisher not here')

                if 'imprint' in orchard.columns:
                    if not check_track.label_name or check_track.label_name != row['imprint']:
                        print(f"updating label ")
                        check_track.label_name = row['imprint']
                    elif check_track.label_name and track_raw_data['LabelName'] == '':
                        track_raw_data['LabelName'].append(check_track.label_name)
                    else:
                        pass
                else:
                    print('publisher not here')

                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_title=row['track name']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_title} exists, creating track now! ")
                    if 'digital upc' in orchard.columns:
                        if not check_track_album.product_upc:
                            print(f"updating display upc ")
                            check_track_album.product_upc = dummy_upc
                        elif check_track_album.product_upc and album_raw_data['Product UPC'] == '':
                            album_raw_data['Product UPC'].append(dummy_upc)
                        else:
                            pass

                    else:
                        print('digital upc  not here')
                    if 'orchard artist' in orchard.columns:
                        if not check_track_album.product_artist or check_track_album.product_artist != row['orchard artist']:
                            print(f"updating product artist")
                            check_track_album.product_artist = row['orchard artist']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=dummy_isrc, display_upc= dummy_upc, title=row['track name'], recording_artist=row['orchard artist'] ,artist =row['orchard artist'] , release_name=row['release name'], label_name=row['imprint'], publisher=row['publisher(s)'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                     
                    print(f"track album does not exists, creating track album and track now! ")
                    if {'track name', 'orchard artist'}.issubset(orchard.columns):
                    # if 'recording_title' and  'band_artist_name' and 'member_name' in ppl:
                        new_track_album = Album(product_upc = dummy_upc, product_title = row['track name'] , product_artist = row['orchard artist'],  processed_day=today )
                        new_track_album.save()
                        new_track = Track(album = new_track_album, display_upc = dummy_upc, title  = row['track name'], isrc = dummy_isrc, recording_artist  = row['orchard artist'], artist  = row['orchard artist'], release_name  = row['release name'], label_name = row['imprint'], publisher  = row['publisher(s)'], processed_day=today)
                        new_track.save()
                        new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                        new_master.save()
                    else:
                        print('recording_title, band_artist_name, member_name are not available for a new album')

        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_orchard_track_output_{today_date}.csv"
        # trk_name = f'processed_horus_{today}_track.csv'
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = orchard_file
        new_track_document.file_name = track_file_name
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_orchard_album_output_{today_date}.csv"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = orchard_file
        new_album_document.file_name =  album_file_name
        new_album_document.save()

        with open(album_file_name, 'rb') as csv:
            new_album_document.file_doc.save( album_file_name, File(csv))

        
        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':orchard_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)

        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':orchard_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <helpdesk@imaginariumng.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)


        
    except:
        print("Unexpected error!")
        raise

############################
#     PPL  ACCOUNT          #
############################
# @shared_task
# def process_ppl_account(filename):
#     ppl_file = ProcessedDocument.objects.filter(file_name=filename).first()
#     try:

#         ppl = pd.read_excel(ppl_file.file_doc, Index=None).fillna('')
#         ppl.columns = ppl.columns.str.strip().str.lower()
#         print(ppl.columns)
#         if not {'period', 'isrc'}.issubset(ppl.columns):

#             send_mail(
#                             'Corrupted File',
#                             f''' 
#                             Hello Admin 
#                             The file {ppl_file.file_name} was corrupt !
#                             Period and ISRC missing !
#                             Pls login to run it again
#                             ''',
#                             'Trackeet Recording Panel ',
#                             ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                             fail_silently=False,
#                             )
#         else:
#             pass
#         account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount',' Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
#         account_raw_data = {
#                 'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
#                             }

#         for index, row in ppl.iterrows():
#             account_raw_data['Period'].append(row['period'])
#             account_raw_data['Activity Period'].append(row['activity period'])
#             account_raw_data['Retailer'].append(row['dms'])
#             account_raw_data['Territory'].append(row['territory'])
#             account_raw_data['Product UPC'].append(row['orchard upc'])
#             account_raw_data['Manufacturer UPC'].append(row['orchard upc'])
#             account_raw_data['Imprint Label'].append(row['imprint label'])
#             account_raw_data['Artist Name'].append(row['artist name'])
#             account_raw_data['Product Name'].append(row['track name'])
#             account_raw_data['Track Name'].append(row['track name'])
#             account_raw_data['Track Artist'].append(row['artist name'])
#             account_raw_data['ISRC'].append(row['isrc'])
#             account_raw_data['Total'].append(row['gross'])
#             account_raw_data['Adjusted Total'].append(row['adjusted gross'])
#             account_raw_data['Label Share Net Receipts'].append(row['label share net receipts'])
            
            
#             if  {'period', 'isrc', 'activity period', 'dms', 'territory', 'orchard upc', 'imprint label', 'artist name', 'track name'}.issubset(ppl.columns):
#                 new_account = Accounting( period = row['period'], activity_period = row['activity period'], retailer = row['dms'], territory = row['territory'], orchard_UPC = row['orchard upc'], manufacturer_UPC = row['orchard upc'], imprint_label = row['imprint label'], artist_name = row['artist name'], product_name = row['track name'], track_name = row['track name'], track_artist =  row['artist name'], isrc = row['isrc'], total = row['gross'], adjusted_total = row['adjusted gross'], label_share_net_receipts = row['label share net receipts'], vendor = "PPL", processed_day = today)
#                 new_account.save()
#             else:
#                 print('Some columns missing ')

#         print(account_raw_data) 
#         new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
#         # trk_name = f'processed_horus_{today}_track.csv'
#         track_account = new_account_df.to_csv('processed_ppl_account.csv', index = False, header=True)
#         new_account_document = ProcessedAccountFile()
#         new_account_document.document = ppl_file
#         new_account_document.file_name = 'processed_ppl_account.csv'
#         new_account_document.save()

#         with open('processed_ppl_account.csv', 'rb') as csv:
#             new_account_document.file_doc.save('processed_ppl_account.csv', File(csv))

#         send_mail(
#                         'File Processed',
#                         f''' 
#                         Hello Admin 
#                         The file {ppl_file.file_name} has been successfully processed !
#                         Congratulations
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)
#     except (ValueError, NameError, TypeError) as error:
#         err_msg = str(error)
#         print(err_msg)
#         send_mail(
#                         'Error Processing File',
#                         f''' 
#                         Hello Admin 
#                         The file {ppl_file.file_name} is corrupt!
#                         See the details below:
#                         {err_msg}
#                             ''',
#                         'Trackeet Recording Panel ',
#                         ['digger@wyldpytch.com','shola.albert@gmail.com'],
#                         fail_silently=False,)
#     except:
#         print("Unexpected error!")
#         raise