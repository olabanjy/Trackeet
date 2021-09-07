from datetime import date
from datetime import datetime
import  random, string,  requests,  pandas as pd
from django.core.mail import  send_mail, EmailMessage
from django.core.files import File
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.db.models import Q
from openpyxl import load_workbook
from celery import shared_task
from .utils import * 
from .models import *




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




def process_ppl_label(pk):
    ppl_file = ProcessedDocument.objects.get(pk=pk)
    try:
        ppl = load_workbook(ppl_file.file_doc)
        sheet = ppl.active
        print(sheet['A3'].value)
        print(sheet['A'].value)

        for cell in sheet['A']:
            print(cell.value)
        # ppl = pd.read_excel(ppl_file.file_doc, Index=None).fillna('')
        # ppl.columns = ppl.columns.str.strip().str.lower()
        # print(ppl.columns)

    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)


    except:
        print("Unexpected error!")
        raise



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
            from_email = 'Trackeet Support <hello@pipminds.com>'
            
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
        

        wrong_data_col = ['Orchard UPC',	'Imprint Label', 'Artist Name',	'Release Name',	'Track Name', 'ISRC']

        wrong_data_raw_data = {
                'Orchard UPC':[], 'Imprint Label':[], 'Artist Name':[],	'Release Name':[],	'Track Name':[],	'ISRC':[]
        }


        for index, row in ppl.iterrows():

            check_label = VerifiedLabels.objects.filter(label_name__contains=str(row['imprint label']).upper())

            if not check_label:
                print(f"We have a wrong label name in {row['imprint label']} ")
                wrong_data_raw_data['Orchard UPC'].append(row['orchard upc'])
                wrong_data_raw_data['Imprint Label'].append(row['imprint label'])
                wrong_data_raw_data['Release Name'].append(row['release name'])
                wrong_data_raw_data['Track Name'].append(row['track name'])
                wrong_data_raw_data['ISRC'].append(row['isrc'])
            else:
                
                track_raw_data['ISRC'].append(row['isrc'])
                track_raw_data['DisplayUPC'].append(row['orchard upc'])
                #TRACK CHECKS 
                check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                if check_track_master:
                    print("PPL CHECK TRACK MASTER EXISTS")

                if check_track_master and check_track_master.track_duration is not None:
                    track_raw_data['TrackDuration'].append(check_track_master.track_duration)
                else:
                    track_raw_data['TrackDuration'].append("None")
                
                if check_track_master and check_track_master.download is not None:
                    track_raw_data['Download'].append(check_track_master.download)
                else:
                    track_raw_data['Download'].append("None")
                
                if check_track_master and check_track_master.sales_start_date is not None:
                    track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
                else:
                    track_raw_data['SalesStartDate'].append("None")
                
                if check_track_master and check_track_master.sales_end_date is not None:
                    track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
                else:
                    track_raw_data['SalesEndDate'].append("None")
                
                if check_track_master and check_track_master.error is not None:
                    track_raw_data['Error'].append(check_track_master.error)
                else:
                    track_raw_data['Error'].append("None")

                if check_track_master and check_track_master.parental_advisory is not None:
                    track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
                else:
                    track_raw_data['ParentalAdvisory'].append("None")

                if check_track_master and check_track_master.preview_clip_start_time is not None:
                    track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
                else:
                    track_raw_data['PreviewClipStartTime'].append("None")

                if check_track_master and check_track_master.preview_clip_duration is not None:
                    track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
                else:
                    track_raw_data['PreviewClipDuration'].append("None")

                if check_track_master and check_track_master.song_version is not None:
                    track_raw_data['SongVersion'].append(check_track_master.song_version)
                else:
                    track_raw_data['SongVersion'].append("None")

                if check_track_master and check_track_master.p_line is not None:
                    track_raw_data['P_LINE'].append(check_track_master.p_line)
                else:
                    track_raw_data['P_LINE'].append("None")
                
                if check_track_master and check_track_master.track_no is not None:
                    track_raw_data['TrackNo'].append(check_track_master.track_no)
                else:
                    track_raw_data['TrackNo'].append("None")

                if check_track_master and check_track_master.hidden_track is not None:
                    track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
                else:
                    track_raw_data['HiddenTrack'].append("None")

                if check_track_master and check_track_master.artist is not None:
                    track_raw_data['Artist'].append(check_track_master.artist)
                else:
                    track_raw_data['Artist'].append(row['artist name'])
                
                if check_track_master and check_track_master.title is not None:
                    track_raw_data['Title'].append(check_track_master.title)
                else:
                    track_raw_data['Title'].append(row['track name'])

                if check_track_master and check_track_master.release_name is not None:
                    track_raw_data['Release Name'].append(check_track_master.release_name)
                else:
                    track_raw_data['Release Name'].append(row['release name'])

                if check_track_master and  check_track_master.recording_artist is not None:
                    track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
                else:
                    track_raw_data['Recording Artist'].append(row['artist name'])

                if check_track_master and check_track_master.label_name is not None:
                    track_raw_data['LabelName'].append(check_track_master.label_name)
                else:
                    track_raw_data['LabelName'].append(row['imprint label'])

                if check_track_master and check_track_master.volume_no is not None:
                    track_raw_data['VolumeNo'].append(check_track_master.volume_no)
                else:
                    track_raw_data['VolumeNo'].append(row['volume'])
                
                if check_track_master and check_track_master.genre is not None:
                    track_raw_data['Genre'].append(check_track_master.genre)
                else:
                    track_raw_data['Genre'].append("None")

                if check_track_master and check_track_master.publisher is not None:
                    track_raw_data['Publisher'].append(check_track_master.publisher)
                else:
                    track_raw_data['Publisher'].append("None")

                if check_track_master and check_track_master.producer is not None:
                    track_raw_data['Producer'].append(check_track_master.producer)
                else:
                    track_raw_data['Producer'].append("None")

                if check_track_master and check_track_master.writer is not None:
                    track_raw_data['Writer'].append(check_track_master.writer)
                else:
                    track_raw_data['Writer'].append("None")
                
                if check_track_master and check_track_master.arranger is not None:
                    track_raw_data['Arranger'].append(check_track_master.arranger)
                else:
                    track_raw_data['Arranger'].append("None")

                if check_track_master and check_track_master.territories is not None:
                    track_raw_data['Territories'].append(check_track_master.territories)
                else:
                    track_raw_data['Territories'].append(row['territory'])
                
                if check_track_master and check_track_master.exclusive is not None:
                    track_raw_data['Exclusive'].append(check_track_master.exclusive)
                else:
                    track_raw_data['Exclusive'].append("None")

                if check_track_master and check_track_master.wholesale_price is not None:
                    track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
                else:
                    track_raw_data['WholesalePrice'].append("None")

                

                #ALBUM CHECKS

                album_raw_data['Product UPC'].append(row['orchard upc'])

                check_album_master = Master_DRMSYS.objects.filter(display_upc=row['orchard upc'], data_type="Album").first()
                if check_album_master:
                    print("PPL CHECK MASTER ALBUM EXISTS")

                if check_album_master and check_album_master.c_line is not None:
                    album_raw_data['CLINE'].append(check_album_master.c_line)
                else:
                    album_raw_data['CLINE'].append("None")

                if check_album_master and check_album_master.master_carveouts is not None:
                    album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
                else:
                    album_raw_data['Master Carveouts'].append("None")

                if check_album_master and check_album_master.territories_carveouts is not None:
                    album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
                else:
                    album_raw_data['Territories Carveouts'].append("None")

                if check_album_master and check_album_master.deleted is not None:
                    album_raw_data['Deleted?'].append(check_album_master.deleted)
                else:
                    album_raw_data['Deleted?'].append("None")

                if check_album_master and check_album_master.manufacturer_upc is not None:
                    album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
                else:
                    album_raw_data['Manufacturer UPC'].append("None")

                if check_album_master and check_album_master.release_date is not None:
                    album_raw_data['Release Date'].append(check_album_master.release_date)
                else:
                    album_raw_data['Release Date'].append("None")

                if check_album_master and check_album_master.artist_url is not None:
                    album_raw_data['Artist URL'].append(check_album_master.artist_url)
                else:
                    album_raw_data['Artist URL'].append("None")

                if check_album_master and check_album_master.label_catalogno is not None:
                    album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
                else:
                    album_raw_data['Catalog Number'].append(row['label catalog #'])


                if check_album_master and check_album_master.total_volumes is not None:
                    album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
                else:
                    album_raw_data['Total Volumes'].append(1)

                if check_album_master and check_album_master.total_tracks is not None:
                    album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
                else:
                    album_raw_data['Total Tracks'].append(row['track #'])

                

                if check_album_master and check_album_master.label_name is not None:
                    album_raw_data['Imprint Label'].append(check_album_master.label_name)
                else:
                    album_raw_data['Imprint Label'].append(row['imprint label'])

                if check_album_master and check_album_master.genre is not None:
                    album_raw_data['Genre'].append(check_album_master.genre)
                else:
                    album_raw_data['Genre'].append("None")

                if check_album_master and check_album_master.title is not None:
                    album_raw_data['Product Title'].append(check_album_master.title)
                else:
                    album_raw_data['Product Title'].append(row['track name'])
                
                if check_album_master and check_album_master.artist is not None:
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
                if not check_track:
                    check_track_album = Master_DRMSYS.objects.filter(display_upc=row['orchard upc'], data_type="Album").first()
                    if check_track_album:
                        print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                        new_track, created = Track.objects.get_or_create(album = check_track_album, isrc=row['isrc'], display_upc= row['orchard upc'], title=row['track name'], recording_artist=row['artist name'],artist =row['artist name'] , release_name=row['track name'], label_name=row['imprint label'], publisher=row['imprint label'], processed_day=today)
              
                        new_master, created = Master_DRMSYS.objects.get_or_create( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                        
                    else:
                        print(f"track album does not exists, creating track album and track now! ")
                        if {'track name', 'artist name'}.issubset(ppl.columns):
                        # if 'recording_title' and  'band_artist_name' and 'member_name' in ppl:
                            new_track_album, created = Album.objects.get_or_create(product_upc = row['orchard upc'], product_title = row['track name'] , product_artist = row['artist name'],  processed_day=today )
                      
                            new_track, created = Track.objects.get_or_create(album = new_track_album, display_upc = row['isrc'], title  = row['track name'], isrc = row['isrc'], recording_artist  = row['artist name'], artist  = row['artist name'], release_name  = row['track name'], label_name = row['imprint label'], publisher  = row['imprint label'], processed_day=today)
                      
                            new_master, created = Master_DRMSYS.objects.get_or_create( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                       
                        else:
                            print('track name, artist name are not available for a new album')
                            
                if  {'period', 'isrc', 'activity period', 'orchard upc', 'imprint label', 'artist name', 'track name'}.issubset(ppl.columns):
                    new_account, created  = Accounting.objects.get_or_create( period = row['period'], activity_period = row['activity period'], retailer = row['dms'], territory = row['territory'], orchard_UPC = row['orchard upc'], manufacturer_UPC = row['orchard upc'], imprint_label = row['imprint label'], artist_name = row['artist name'], product_name = row['track name'], track_name = row['track name'], track_artist =  row['artist name'], isrc = row['isrc'], total = row['gross'], adjusted_total = row['adjusted gross'], label_share_net_receipts = row['label share net receipts'], vendor = "PPL", processed_day = today)
                
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


        print(wrong_data_raw_data)
        if len(wrong_data_raw_data) != 0:
            new_wrong_data_df = pd.DataFrame({key:pd.Series(value) for key, value in wrong_data_raw_data.items() }, columns=wrong_data_col)
            new_wrong_data_file_name  = f"new_wrong_ppl_label_{today_date}.csv"
            new_wrong_data = new_wrong_data_df.to_csv(new_wrong_data_file_name, index = False, header=True)
            new_wrong_data_doc = WrongLabelDocs()
            new_wrong_data_doc.file_name = new_wrong_data_file_name
            new_wrong_data_doc.save()

            with open(new_wrong_data_file_name, 'rb') as csv:
                new_wrong_data_doc.file_doc.save( new_wrong_data_file_name, File(csv))


            subject = 'Incorrect/Inconsistent Label Error'
            html_message = render_to_string('mail/error_data.html', {'err_data':new_wrong_data_doc.file_doc.url})
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>'
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

         

        else:
            subject = 'File Processed'
            html_message = render_to_string('mail/processed.html', {'file_name':ppl_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>'
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

        

            

       
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ppl_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise







############################
#     PPL  ACCOUNT  UNDER CONSTRUCTION  #
############################

@shared_task
def process_ppl_artist(pk):
    ppl_file = ProcessedDocument.objects.get(pk=pk)
    try:
        ppl = pd.read_excel(ppl_file.file_doc, Index=None).fillna('')
        ppl.columns = ppl.columns.str.strip().str.lower()
        # print(ppl.columns)
        if not {'artist name'}.issubset(ppl.columns):
            subject = 'CorruptedFile'
            html_message = render_to_string('mail/corrupt.html', {'file_name':ppl_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

            
        else:
            pass

        artists = []
        for index, row in ppl.iterrows():
            if row['artist name'] not in artists:
                artists.append(row['artist name'])
        
        print(artists)

        for val in artists:
            # print(ppl.loc[ppl['artist name'] == val])
            artist_df = ppl.loc[ppl['artist name'] == val]
            artist_refined_name = val.replace(" ","_")
            artist_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
            artist_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }
            
            for index, row in artist_df.iterrows():
                # print(f" {val} in this row has isrc of {row['isrc']}")
                print(f" label name is {row['imprint label']}")
                artist_data['Period'].append(f"{row['period']}")
                artist_data['Activity Period'].append(f"{row['activity period']}")
                artist_data['Retailer'].append(f"{row['dms']}")
                artist_data['Territory'].append(f"{row['territory']}")
                artist_data['Product UPC'].append(f"{row['orchard upc']}")
                artist_data['Manufacturer UPC'].append(f"{row['orchard upc']}")
                artist_data['Imprint Label'].append(f"{row['imprint label']}")
                artist_data['Artist Name'].append(f"{row['artist name']}")
                artist_data['Product Name'].append(f"{row['track name']}")
                artist_data['Track Name'].append(f"{row['track name']}")
                artist_data['Track Artist'].append(f"{row['artist name']}")
                artist_data['ISRC'].append(f"{row['isrc']}")
                artist_data['Total'].append(f"{row['gross']}")
                artist_data['Adjusted Total'].append(f"{row['adjusted gross']}")
                artist_data['Label Share Net Receipts'].append(f"{row['label share net receipts']}")

            print(artist_data) 
            new_artist_df = pd.DataFrame({key:pd.Series(value) for key, value in artist_data.items() }, columns=artist_col)
            account_file_name  = f"processed_{artist_refined_name}_output_{today_date}.csv"
            track_account = new_artist_df.to_csv(account_file_name, index = False, header=True)
            new_account_document = ProcessedArtistFile()
            new_account_document.document = ppl_file
            new_account_document.file_name = account_file_name
            new_account_document.save()

            with open(account_file_name, 'rb') as csv:
                new_account_document.file_doc.save( account_file_name, File(csv))


            print(f"Done with {val}")

        find_all_artist = ProcessedArtistFile.objects.filter(document=ppl_file).all()

        subject, from_email, to = 'Processed Artist Data', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
        # text_content = f" Dear {user.username}. Welcome to Ultra "
        html_content = render_to_string('mail/processed_artists.html', {'file_name':ppl_file.file_name})
        msg = EmailMessage(subject, html_content, from_email, to)
        msg.content_subtype = "html"
        for artist_file in find_all_artist:
            the_file = artist_file.file_doc 
            the_filename = str(artist_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_filename, response.content, mimetype="text/csv")
        msg.send()


    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ppl_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise


@shared_task
def process_ppl_labels(pk):
    ppl_file = ProcessedDocument.objects.get(pk=pk)
    try:
        ppl = pd.read_excel(ppl_file.file_doc, Index=None).fillna('')
        ppl.columns = ppl.columns.str.strip().str.lower()
        # print(ppl.columns)
        if not {'imprint label'}.issubset(ppl.columns):
            subject = 'CorruptedFile'
            html_message = render_to_string('mail/corrupt.html', {'file_name':ppl_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

            
        else:
            pass

        labels = []
        for index, row in ppl.iterrows():
            if row['imprint label'] not in labels:
                labels.append(row['imprint label'])
        
        print(labels)

        for val in labels:
            # print(ppl.loc[ppl['artist name'] == val])
            label_df = ppl.loc[ppl['imprint label'] == val]
            label_refined_name = val.replace(" ","_")
            label_col = ['Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
            label_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }
            
            for index, row in label_df.iterrows():
                print(f" {val} in this row has isrc of {row['isrc']}")


                label_data['Period'].append(row['period'])
                label_data['Activity Period'].append(row['activity period'])
                label_data['Retailer'].append(row['dms'])
                label_data['Territory'].append(row['territory'])
                label_data['Product UPC'].append(row['orchard upc'])
                label_data['Manufacturer UPC'].append(row['orchard upc'])
                label_data['Imprint Label'].append(row['imprint label'])
                label_data['Artist Name'].append(row['artist name'])
                label_data['Product Name'].append(row['track name'])
                label_data['Track Name'].append(row['track name'])
                label_data['Track Artist'].append(row['artist name'])
                label_data['ISRC'].append(row['isrc'])
                label_data['Total'].append(row['gross'])
                label_data['Adjusted Total'].append(row['adjusted gross'])
                label_data['Label Share Net Receipts'].append(row['label share net receipts'])
 
            print(label_data) 
            new_label_df = pd.DataFrame({key:pd.Series(value) for key, value in label_data.items() }, columns=label_col)
            account_file_name  = f"processed_{label_refined_name}_output_{today_date}.csv"
            track_account = new_label_df.to_csv(account_file_name, index = False, header=True)
            new_account_document = ProcessedLabelFile()
            new_account_document.document = ppl_file
            new_account_document.file_name = account_file_name
            new_account_document.save()

            with open(account_file_name, 'rb') as csv:
                new_account_document.file_doc.save( account_file_name, File(csv))

            print(f"Done with {val}")
        find_all_labels = ProcessedLabelFile.objects.filter(document=ppl_file).all()
        subject, from_email, to = 'Processed Label Data', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
        html_content = render_to_string('mail/processed_labels.html', {'file_name':ppl_file.file_name})
        msg = EmailMessage(subject, html_content, from_email, to)
        msg.content_subtype = "html"
        for label_file in find_all_labels:
            the_file = label_file.file_doc 
            the_filename = str(label_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_filename, response.content, mimetype="application/pdf")
        msg.send()
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ppl_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
    except:
        print("Unexpected error!")
        raise



@shared_task
def process_ppl_new_account(pk):
    ppl_file = ProcessedDocument.objects.get(pk=pk)
    try:
        ppl = pd.read_excel(ppl_file.file_doc, Index=None).fillna('')
        ppl.columns = ppl.columns.str.strip().str.lower()
        #print(ppl.columns)
        if not {'period', 'isrc'}.issubset(ppl.columns):
            subject = 'CorruptedFile'
            html_message = render_to_string('mail/corrupt.html', {'file_name':ppl_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)  
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
        
        wrong_data_col = [ 'Period',  'Activity Period', 'DMS', 'Territory', 'Orchard UPC', 'Manufacturer UPC', 'Label Catalog', 'Imprint Label', 'Artist Name', 'Release Name', 'Track Name', 'ISRC', 'Volume', 'Track', 'Quantity','Unit Price', 'Gross', 'Trans Type',  'Adjusted Gross', 'Split Rate', 'Label Share Net Receipts', 'Ringtone Publishing', 'Cloud Publishing', 'Publishing', 'Mech Administrative Fee', 'Preferred Currency' ]

        wrong_data_raw_data = {
            'Period':[],  'Activity Period':[], 'DMS':[], 'Territory':[], 'Orchard UPC':[], 'Manufacturer UPC':[], 'Label Catalog':[], 'Imprint Label':[], 'Artist Name':[], 'Release Name':[], 'Track Name':[], 'ISRC':[], 'Volume':[], 'Track':[], 'Quantity':[],'Unit Price':[], 'Gross':[], 'Trans Type':[],  'Adjusted Gross':[], 'Split Rate':[], 'Label Share Net Receipts':[], 'Ringtone Publishing':[], 'Cloud Publishing':[], 'Publishing':[], 'Mech Administrative Fee':[], 'Preferred Currency':[]
        }

        missing_data_col = [ 'Period',  'Activity Period', 'DMS', 'Territory', 'Orchard UPC', 'Manufacturer UPC', 'Label Catalog', 'Imprint Label', 'Artist Name', 'Release Name', 'Track Name', 'ISRC', 'Volume', 'Track', 'Quantity','Unit Price', 'Gross', 'Trans Type',  'Adjusted Gross', 'Split Rate', 'Label Share Net Receipts', 'Ringtone Publishing', 'Cloud Publishing', 'Publishing', 'Mech Administrative Fee', 'Preferred Currency' ]

        missing_data_raw_data = {
            'Period':[],  'Activity Period':[], 'DMS':[], 'Territory':[], 'Orchard UPC':[], 'Manufacturer UPC':[], 'Label Catalog':[], 'Imprint Label':[], 'Artist Name':[], 'Release Name':[], 'Track Name':[], 'ISRC':[], 'Volume':[], 'Track':[], 'Quantity':[],'Unit Price':[], 'Gross':[], 'Trans Type':[],  'Adjusted Gross':[], 'Split Rate':[], 'Label Share Net Receipts':[], 'Ringtone Publishing':[], 'Cloud Publishing':[], 'Publishing':[], 'Mech Administrative Fee':[], 'Preferred Currency':[]
        }
        
      

        for index, row in ppl.iterrows():
            check_record = Master_DRMSYS.objects.filter(Q(isrc=row['isrc'], data_type='Track')| Q(display_upc=row['orchard upc'], data_type='Track'))
            if check_record.exists():
                check_label = VerifiedLabels.objects.filter(label_name__contains=str(row['imprint label']).upper())
                if check_label.exists():
                    
                    track_raw_data['ISRC'].append(row['isrc'])
                    track_raw_data['DisplayUPC'].append(row['orchard upc'])
                    #TRACK CHECKS 
                    #Process Track 
                    check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                    if check_track_master:
                        print("PPL CHECK TRACK MASTER EXISTS")

                    if check_track_master and check_track_master.track_duration is not None:
                        track_raw_data['TrackDuration'].append(check_track_master.track_duration)
                    else:
                        track_raw_data['TrackDuration'].append("None")
                    
                    if check_track_master and check_track_master.download is not None:
                        track_raw_data['Download'].append(check_track_master.download)
                    else:
                        track_raw_data['Download'].append("None")
                    
                    if check_track_master and check_track_master.sales_start_date is not None:
                        track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
                    else:
                        track_raw_data['SalesStartDate'].append("None")
                    
                    if check_track_master and check_track_master.sales_end_date is not None:
                        track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
                    else:
                        track_raw_data['SalesEndDate'].append("None")
                    
                    if check_track_master and check_track_master.error is not None:
                        track_raw_data['Error'].append(check_track_master.error)
                    else:
                        track_raw_data['Error'].append("None")

                    if check_track_master and check_track_master.parental_advisory is not None:
                        track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
                    else:
                        track_raw_data['ParentalAdvisory'].append("None")

                    if check_track_master and check_track_master.preview_clip_start_time is not None:
                        track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
                    else:
                        track_raw_data['PreviewClipStartTime'].append("None")

                    if check_track_master and check_track_master.preview_clip_duration is not None:
                        track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
                    else:
                        track_raw_data['PreviewClipDuration'].append("None")

                    if check_track_master and check_track_master.song_version is not None:
                        track_raw_data['SongVersion'].append(check_track_master.song_version)
                    else:
                        track_raw_data['SongVersion'].append("None")

                    if check_track_master and check_track_master.p_line is not None:
                        track_raw_data['P_LINE'].append(check_track_master.p_line)
                    else:
                        track_raw_data['P_LINE'].append("None")
                    
                    if check_track_master and check_track_master.track_no is not None:
                        track_raw_data['TrackNo'].append(check_track_master.track_no)
                    else:
                        track_raw_data['TrackNo'].append("None")

                    if check_track_master and check_track_master.hidden_track is not None:
                        track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
                    else:
                        track_raw_data['HiddenTrack'].append("None")

                    if check_track_master and check_track_master.artist is not None:
                        track_raw_data['Artist'].append(check_track_master.artist)
                    else:
                        track_raw_data['Artist'].append(row['artist name'])
                    
                    if check_track_master and check_track_master.title is not None:
                        track_raw_data['Title'].append(check_track_master.title)
                    else:
                        track_raw_data['Title'].append(row['track name'])

                    if check_track_master and check_track_master.release_name is not None:
                        track_raw_data['Release Name'].append(check_track_master.release_name)
                    else:
                        track_raw_data['Release Name'].append(row['release name'])

                    if check_track_master and  check_track_master.recording_artist is not None:
                        track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
                    else:
                        track_raw_data['Recording Artist'].append(row['artist name'])

                    if check_track_master and check_track_master.label_name is not None:
                        track_raw_data['LabelName'].append(check_track_master.label_name)
                    else:
                        track_raw_data['LabelName'].append(row['imprint label'])

                    if check_track_master and check_track_master.volume_no is not None:
                        track_raw_data['VolumeNo'].append(check_track_master.volume_no)
                    else:
                        track_raw_data['VolumeNo'].append(row['volume'])
                    
                    if check_track_master and check_track_master.genre is not None:
                        track_raw_data['Genre'].append(check_track_master.genre)
                    else:
                        track_raw_data['Genre'].append("None")

                    if check_track_master and check_track_master.publisher is not None:
                        track_raw_data['Publisher'].append(check_track_master.publisher)
                    else:
                        track_raw_data['Publisher'].append("None")

                    if check_track_master and check_track_master.producer is not None:
                        track_raw_data['Producer'].append(check_track_master.producer)
                    else:
                        track_raw_data['Producer'].append("None")

                    if check_track_master and check_track_master.writer is not None:
                        track_raw_data['Writer'].append(check_track_master.writer)
                    else:
                        track_raw_data['Writer'].append("None")
                    
                    if check_track_master and check_track_master.arranger is not None:
                        track_raw_data['Arranger'].append(check_track_master.arranger)
                    else:
                        track_raw_data['Arranger'].append("None")

                    if check_track_master and check_track_master.territories is not None:
                        track_raw_data['Territories'].append(check_track_master.territories)
                    else:
                        track_raw_data['Territories'].append(row['territory'])
                    
                    if check_track_master and check_track_master.exclusive is not None:
                        track_raw_data['Exclusive'].append(check_track_master.exclusive)
                    else:
                        track_raw_data['Exclusive'].append("None")

                    if check_track_master and check_track_master.wholesale_price is not None:
                        track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
                    else:
                        track_raw_data['WholesalePrice'].append("None")

                    

                    #ALBUM CHECKS
                    #Process Album 

                    album_raw_data['Product UPC'].append(row['orchard upc'])

                    check_album_master = Master_DRMSYS.objects.filter(display_upc=row['orchard upc'], data_type="Album").first()
                    if check_album_master:
                        print("PPL CHECK MASTER ALBUM EXISTS")

                    if check_album_master and check_album_master.c_line is not None:
                        album_raw_data['CLINE'].append(check_album_master.c_line)
                    else:
                        album_raw_data['CLINE'].append("None")

                    if check_album_master and check_album_master.master_carveouts is not None:
                        album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
                    else:
                        album_raw_data['Master Carveouts'].append("None")

                    if check_album_master and check_album_master.territories_carveouts is not None:
                        album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
                    else:
                        album_raw_data['Territories Carveouts'].append("None")

                    if check_album_master and check_album_master.deleted is not None:
                        album_raw_data['Deleted?'].append(check_album_master.deleted)
                    else:
                        album_raw_data['Deleted?'].append("None")

                    if check_album_master and check_album_master.manufacturer_upc is not None:
                        album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
                    else:
                        album_raw_data['Manufacturer UPC'].append("None")

                    if check_album_master and check_album_master.release_date is not None:
                        album_raw_data['Release Date'].append(check_album_master.release_date)
                    else:
                        album_raw_data['Release Date'].append("None")

                    if check_album_master and check_album_master.artist_url is not None:
                        album_raw_data['Artist URL'].append(check_album_master.artist_url)
                    else:
                        album_raw_data['Artist URL'].append("None")

                    if check_album_master and check_album_master.label_catalogno is not None:
                        album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
                    else:
                        album_raw_data['Catalog Number'].append(row['label catalog #'])


                    if check_album_master and check_album_master.total_volumes is not None:
                        album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
                    else:
                        album_raw_data['Total Volumes'].append(1)

                    if check_album_master and check_album_master.total_tracks is not None:
                        album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
                    else:
                        album_raw_data['Total Tracks'].append(row['track #'])

                    

                    if check_album_master and check_album_master.label_name is not None:
                        album_raw_data['Imprint Label'].append(check_album_master.label_name)
                    else:
                        album_raw_data['Imprint Label'].append(row['imprint label'])

                    if check_album_master and check_album_master.genre is not None:
                        album_raw_data['Genre'].append(check_album_master.genre)
                    else:
                        album_raw_data['Genre'].append("None")

                    if check_album_master and check_album_master.title is not None:
                        album_raw_data['Product Title'].append(check_album_master.title)
                    else:
                        album_raw_data['Product Title'].append(row['track name'])
                    
                    if check_album_master and check_album_master.artist is not None:
                        album_raw_data['Product Artist'].append(check_album_master.artist)
                    else:
                        album_raw_data['Product Artist'].append(row['artist name'])

                    #Process Account 
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


                    if  {'period', 'isrc', 'activity period', 'orchard upc', 'imprint label', 'artist name', 'track name'}.issubset(ppl.columns):
                        new_account, created  = Accounting.objects.get_or_create( period = row['period'], activity_period = row['activity period'], retailer = row['dms'], territory = row['territory'], orchard_UPC = row['orchard upc'], manufacturer_UPC = row['orchard upc'], imprint_label = row['imprint label'], artist_name = row['artist name'], product_name = row['track name'], track_name = row['track name'], track_artist =  row['artist name'], isrc = row['isrc'], total = row['gross'], adjusted_total = row['adjusted gross'], label_share_net_receipts = row['label share net receipts'], vendor = "PPL", processed_day = today)
                
                    else:
                        print('Some columns missing ')
                    
                else:
                    print(f"We have a wrong label name in {row['imprint label']} ")
                    print(f"We have a wrong label name in {row['imprint label']} ")
                    print(f"We have a wrong label name in {row['imprint label']} ")
                    print(f"We have a wrong label name in {row['imprint label']} ")
                    print(f"We have a wrong label name in {row['imprint label']} ")
                    wrong_data_raw_data['Orchard UPC'].append(row['orchard upc'])
                    wrong_data_raw_data['Imprint Label'].append(row['imprint label'])
                    wrong_data_raw_data['Release Name'].append(row['release name'])
                    wrong_data_raw_data['Track Name'].append(row['track name'])
                    wrong_data_raw_data['ISRC'].append(row['isrc'])
                    wrong_data_raw_data['Period'].append(row['period'])
                    wrong_data_raw_data['Activity Period'].append(row['activity period'])
                    wrong_data_raw_data['DMS'].append(row['dms'])
                    wrong_data_raw_data['Territory'].append(row['territory'])                   
                    wrong_data_raw_data['Manufacturer UPC'].append(row["manufacturer's upc"])
                    wrong_data_raw_data['Label Catalog'].append(row['label catalog #'])
                    wrong_data_raw_data['Artist Name'].append(row['artist name'])
                    wrong_data_raw_data['Volume'].append(row['volume'])
                    wrong_data_raw_data['Track'].append(row['track #'])
                    wrong_data_raw_data['Quantity'].append(row['quantity'])
                    wrong_data_raw_data['Unit Price'].append(row['unit price'])
                    wrong_data_raw_data['Gross'].append(row['gross'])
                    wrong_data_raw_data['Trans Type'].append(row['trans type'])
                    wrong_data_raw_data['Adjusted Gross'].append(row['adjusted gross'])
                    wrong_data_raw_data['Split Rate'].append(row['split rate'])
                    wrong_data_raw_data['Label Share Net Receipts'].append(row['label share net receipts'])
                    wrong_data_raw_data['Ringtone Publishing'].append(row['ringtone publishing'])
                    wrong_data_raw_data['Cloud Publishing'].append(row['cloud publishing'])
                    wrong_data_raw_data['Publishing'].append(row['publishing'])
                    wrong_data_raw_data['Mech Administrative Fee'].append(row['mech. administrative fee'])
                    wrong_data_raw_data['Preferred Currency'].append(row['preferred currency'])

            else:
                #print(f"This record does not exist in the Master DRMSYS {row['imprint label']} ")
                missing_data_raw_data['Orchard UPC'].append(row['orchard upc'])
                missing_data_raw_data['Imprint Label'].append(row['imprint label'])
                missing_data_raw_data['Release Name'].append(row['release name'])
                missing_data_raw_data['Track Name'].append(row['track name'])
                missing_data_raw_data['ISRC'].append(row['isrc'])
                missing_data_raw_data['Period'].append(row['period'])
                missing_data_raw_data['Activity Period'].append(row['activity period'])
                missing_data_raw_data['DMS'].append(row['dms'])
                missing_data_raw_data['Territory'].append(row['territory'])                   
                missing_data_raw_data['Manufacturer UPC'].append(row["manufacturer's upc"])
                missing_data_raw_data['Label Catalog'].append(row['label catalog #'])
                missing_data_raw_data['Artist Name'].append(row['artist name'])
                missing_data_raw_data['Volume'].append(row['volume'])
                missing_data_raw_data['Track'].append(row['track #'])
                missing_data_raw_data['Quantity'].append(row['quantity'])
                missing_data_raw_data['Unit Price'].append(row['unit price'])
                missing_data_raw_data['Gross'].append(row['gross'])
                missing_data_raw_data['Trans Type'].append(row['trans type'])
                missing_data_raw_data['Adjusted Gross'].append(row['adjusted gross'])
                missing_data_raw_data['Split Rate'].append(row['split rate'])
                missing_data_raw_data['Label Share Net Receipts'].append(row['label share net receipts'])
                missing_data_raw_data['Ringtone Publishing'].append(row['ringtone publishing'])
                missing_data_raw_data['Cloud Publishing'].append(row['cloud publishing'])
                missing_data_raw_data['Publishing'].append(row['publishing'])
                missing_data_raw_data['Mech Administrative Fee'].append(row['mech. administrative fee'])
                missing_data_raw_data['Preferred Currency'].append(row['preferred currency'])


        #print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_ppl_track_output_{today_date}.csv"
        track_file_name_txt  = f"processed_ppl_track_output_{today_date}.txt"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        track_file_txt = new_track_df.to_csv(track_file_name_txt, index = False, header=True, sep='\t')
        new_track_document = ProcessedTrackFile()
        new_track_document.document = ppl_file
        new_track_document.file_name = track_file_name
        new_track_document.file_name_txt = track_file_name_txt
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        with open(track_file_name_txt, 'rb') as txt:
            new_track_document.file_doc_txt.save(track_file_name_txt, File(txt))
        
        #print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_ppl_album_output_{today_date}.csv"
        album_file_name_txt  = f"processed_ppl_album_output_{today_date}.txt"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        album_file_txt = new_album_df.to_csv(album_file_name_txt, index = False, header=True, sep='\t')
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = ppl_file
        new_album_document.file_name = album_file_name
        new_album_document.file_name_txt = album_file_name_txt
        new_album_document.save()

        with open( album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))

        with open(album_file_name_txt, 'rb') as txt:
            new_album_document.file_doc_txt.save(album_file_name_txt, File(txt))

        #print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        account_file_name  = f"processed_ppl_account_output_{today_date}.csv"
        track_account = new_account_df.to_csv(account_file_name, index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = ppl_file
        new_account_document.file_name = account_file_name
        new_account_document.save()

        with open(account_file_name, 'rb') as csv:
            new_account_document.file_doc.save( account_file_name, File(csv))






        print(len(wrong_data_raw_data))
        print(wrong_data_raw_data)
        if len(wrong_data_raw_data) != 0:
            new_wrong_data_df = pd.DataFrame({key:pd.Series(value) for key, value in wrong_data_raw_data.items() }, columns=wrong_data_col)
            new_wrong_data_file_name  = f"new_wrong_ppl_label_{today_date}.csv"
            new_wrong_data = new_wrong_data_df.to_csv(new_wrong_data_file_name, index = False, header=True)
            new_wrong_data_doc = WrongLabelDocs()
            new_wrong_data_doc.document = ppl_file
            new_wrong_data_doc.file_name = new_wrong_data_file_name
            new_wrong_data_doc.save()

            with open(new_wrong_data_file_name, 'rb') as csv:
                new_wrong_data_doc.file_doc.save( new_wrong_data_file_name, File(csv))


            

            
            the_wrong_label_file = WrongLabelDocs.objects.filter(document=ppl_file).last()

            subject, from_email, to = 'Incorrect/Inconsistent Label Error', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
            
            html_content = render_to_string('mail/error_data.html', {'err_data':new_wrong_data_doc.file_doc.url})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            the_file = the_wrong_label_file.file_doc 
            the_filename = str(the_wrong_label_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_filename, response.content, mimetype="text/csv")
            msg.send()


        print(len(missing_data_raw_data))
        print(missing_data_raw_data)
        if len(missing_data_raw_data) != 0:
            
            new_missing_data_df = pd.DataFrame({key:pd.Series(value) for key, value in missing_data_raw_data.items() }, columns=missing_data_col)
            new_missing_data_file_name  = f"new_missing_data_{today_date}.csv"
            new_missing_data = new_missing_data_df.to_csv(new_missing_data_file_name, index = False, header=True)
            new_missing_data_doc = MissingRecordsDoc()
            new_missing_data_doc.document = ppl_file
            new_missing_data_doc.file_name = new_missing_data_file_name
            new_missing_data_doc.save()

            with open(new_missing_data_file_name, 'rb') as csv:
                new_missing_data_doc.file_doc.save( new_missing_data_file_name, File(csv))


            

            the_missing_file = MissingRecordsDoc.objects.filter(document=ppl_file).last()

            subject, from_email, to = 'Missing Data in DRMSYS', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
            
            html_content = render_to_string('mail/missing_data.html', {'missing_data':new_missing_data_doc.file_doc.url})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            the_file = the_missing_file.file_doc 
            the_filename = str(the_missing_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_filename, response.content, mimetype="text/csv")
            msg.send()




            

            
        get_track_doc = ProcessedTrackFile.objects.filter(document=ppl_file).last()
        get_album_doc = ProcessedAlbumFile.objects.filter(document=ppl_file).last()
        get_account_doc = ProcessedAccountFile.objects.filter(document=ppl_file).last()

        the_track_doc_file = get_track_doc.file_doc 
        the_track_doc_filename = str(get_track_doc.file_name)
        track_response = requests.get(the_track_doc_file.url)

        the_album_doc_file = get_album_doc.file_doc 
        the_album_doc_filename = str(get_album_doc.file_name)
        album_response = requests.get(the_album_doc_file.url)

        the_account_doc_file = get_account_doc.file_doc 
        the_account_doc_filename = str(get_account_doc.file_name)
        account_response = requests.get(the_account_doc_file.url)

        
        subject, from_email, to = 'File Processed', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
            
        html_content = render_to_string('mail/processed.html', {'file_name':ppl_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
        msg = EmailMessage(subject, html_content, from_email, to)
        msg.content_subtype = "html"
        the_file = the_missing_file.file_doc 
        the_filename = str(the_missing_file.file_name)
        response = requests.get(the_file.url)
        msg.attach(the_track_doc_filename, track_response.content, mimetype="text/csv")
        msg.attach(the_album_doc_filename, album_response.content, mimetype="text/csv")
        msg.attach(the_account_doc_filename, account_response.content, mimetype="text/csv")
        msg.send()


    

    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ppl_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
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
#                 if not check_track.display_upc or check_track.display_upc is not row['Manufacturer Number']:
#                     print(f"updating display upc ")
#                     check_track.display_upc = row['Manufacturer Number']
#                 if not check_track.artist or check_track.artist is not row['Interested Party (1)']:
#                     print(f"updating track artist")
#                     check_track.artist = row['Interested Party (1)']
#                 if not check_track.title or check_track.title is not row['Title']:
#                     print(f"updating title ")
#                     check_track.title = row['Title']
#                 if not check_track.release_name or check_track.release_name is not row['Product Title']:
#                     print(f"updating release name ")
#                     check_track.release_name = row['Product Title']
#                 if not check_track.recording_artist or check_track.recording_artist is not row['Interested Party (1)']:
#                     print(f"updating recording artist")
#                     check_track.recording_artist = row['Interested Party (1)']
#                 if not check_track.publisher or check_track.publisher is not  row['Member']:
#                     print(f"updating publisher ")
#                     check_track.publisher = row['Member']
#                 if not check_track.label_name or check_track.label_name is not row['Interested Party (3)']:
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
            from_email = 'Trackeet Support <hello@pipminds.com>'
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

            if check_track_master and check_track_master.track_duration is not None:
                track_raw_data['TrackDuration'].append(check_track_master.track_duration)
            else:
                track_raw_data['TrackDuration'].append("None")

            if check_track_master and check_track_master.display_upc is not None:
                track_raw_data['DisplayUPC'].append(check_track_master.display_upc)
            else:
                track_raw_data['DisplayUPC'].append(row['manufacturer number'])
            
            if check_track_master and check_track_master.download is not None:
                track_raw_data['Download'].append(check_track_master.download)
            else:
                track_raw_data['Download'].append("None")
            
            if check_track_master and check_track_master.sales_start_date is not None:
                track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
            else:
                track_raw_data['SalesStartDate'].append("None")
            
            if check_track_master and check_track_master.sales_end_date is not None:
                track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
            else:
                track_raw_data['SalesEndDate'].append("None")
            
            if check_track_master and check_track_master.error is not None:
                track_raw_data['Error'].append(check_track_master.error)
            else:
                track_raw_data['Error'].append("None")

            if check_track_master and check_track_master.parental_advisory is not None:
                track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
            else:
                track_raw_data['ParentalAdvisory'].append("None")

            if check_track_master and check_track_master.preview_clip_start_time is not None:
                track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
            else:
                track_raw_data['PreviewClipStartTime'].append("None")

            if check_track_master and check_track_master.preview_clip_duration is not None:
                track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
            else:
                track_raw_data['PreviewClipDuration'].append("None")

            if check_track_master and check_track_master.song_version is not None:
                track_raw_data['SongVersion'].append(check_track_master.song_version)
            else:
                track_raw_data['SongVersion'].append("None")

            if check_track_master and check_track_master.p_line is not None:
                track_raw_data['P_LINE'].append(check_track_master.p_line)
            else:
                track_raw_data['P_LINE'].append("None")
            
            if check_track_master and check_track_master.track_no is not None:
                track_raw_data['TrackNo'].append(check_track_master.track_no)
            else:
                track_raw_data['TrackNo'].append("None")

            if check_track_master and check_track_master.hidden_track is not None:
                track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
            else:
                track_raw_data['HiddenTrack'].append("None")

            if check_track_master and check_track_master.artist is not None:
                track_raw_data['Artist'].append(check_track_master.artist)
            else:
                track_raw_data['Artist'].append(row['interested party (1)'])
            
            if check_track_master and check_track_master.title is not None:
                track_raw_data['Title'].append(check_track_master.title)
            else:
                track_raw_data['Title'].append(row['interested party (3)'])

            if check_track_master and check_track_master.release_name is not None:
                track_raw_data['Release Name'].append(check_track_master.release_name)
            else:
                track_raw_data['Release Name'].append(row['product title'])

            if check_track_master and  check_track_master.recording_artist is not None:
                track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
            else:
                track_raw_data['Recording Artist'].append(row['interested party (1)'])

            if check_track_master and check_track_master.label_name is not None:
                track_raw_data['LabelName'].append(check_track_master.label_name)
            else:
                track_raw_data['LabelName'].append(row['interested party (3)'])

            if check_track_master and check_track_master.volume_no is not None:
                track_raw_data['VolumeNo'].append(check_track_master.volume_no)
            else:
                track_raw_data['VolumeNo'].append("None")
            
            if check_track_master and check_track_master.genre is not None:
                track_raw_data['Genre'].append(check_track_master.genre)
            else:
                track_raw_data['Genre'].append("None")

            if check_track_master and check_track_master.publisher is not None:
                track_raw_data['Publisher'].append(check_track_master.publisher)
            else:
                track_raw_data['Publisher'].append(row['member'])

            if check_track_master and check_track_master.producer is not None:
                track_raw_data['Producer'].append(check_track_master.producer)
            else:
                track_raw_data['Producer'].append("None")

            if check_track_master and check_track_master.writer is not None:
                track_raw_data['Writer'].append(check_track_master.writer)
            else:
                track_raw_data['Writer'].append("None")
            
            if check_track_master and check_track_master.arranger is not None:
                track_raw_data['Arranger'].append(check_track_master.arranger)
            else:
                track_raw_data['Arranger'].append("None")

            if check_track_master and check_track_master.territories is not None:
                track_raw_data['Territories'].append(check_track_master.territories)
            else:
                track_raw_data['Territories'].append("None")
            
            if check_track_master and check_track_master.exclusive is not None:
                track_raw_data['Exclusive'].append(check_track_master.exclusive)
            else:
                track_raw_data['Exclusive'].append("None")

            if check_track_master and check_track_master.wholesale_price is not None:
                track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
            else:
                track_raw_data['WholesalePrice'].append("None")

            
            #ALBUM CHECKS

            album_raw_data['Product UPC'].append(row['manufacturer number'])
            check_album_master = Master_DRMSYS.objects.filter(display_upc=row['manufacturer number'], data_type="Album").first()
            if check_album_master:
                print("MCPS CHECK MASTER ALBUM EXISTS")

            if check_album_master and check_album_master.c_line is not None:
                album_raw_data['CLINE'].append(check_album_master.c_line)
            else:
                album_raw_data['CLINE'].append("None")

            if check_album_master and check_album_master.master_carveouts is not None:
                album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
            else:
                album_raw_data['Master Carveouts'].append("None")

            if check_album_master and check_album_master.territories_carveouts is not None:
                album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
            else:
                album_raw_data['Territories Carveouts'].append("None")

            if check_album_master and check_album_master.deleted is not None:
                album_raw_data['Deleted?'].append(check_album_master.deleted)
            else:
                album_raw_data['Deleted?'].append("None")

            if check_album_master and check_album_master.manufacturer_upc is not None:
                album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
            else:
                album_raw_data['Manufacturer UPC'].append("None")

            if check_album_master and check_album_master.release_date is not None:
                album_raw_data['Release Date'].append(check_album_master.release_date)
            else:
                album_raw_data['Release Date'].append(row['period'])

            if check_album_master and check_album_master.artist_url is not None:
                album_raw_data['Artist URL'].append(check_album_master.artist_url)
            else:
                album_raw_data['Artist URL'].append("None")

            if check_album_master and check_album_master.label_catalogno is not None:
                album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
            else:
                album_raw_data['Catalog Number'].append(row['catalogue number'])


            check_master_for_cat = Master_DRMSYS.objects.filter(label_catalogno=row['catalogue number'])
            if check_master_for_cat:
                total_vol = check_master_for_cat.count()
                album_raw_data['Total Volumes'].append(str(total_vol))
            else:
                album_raw_data['Total Volumes'].append('1')
       
            if check_album_master and check_album_master.total_tracks is not None:
                album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
            else:
                album_raw_data['Total Tracks'].append(1)

            if check_album_master and check_album_master.label_name is not None:
                album_raw_data['Imprint Label'].append(check_album_master.label_name)
            else:
                album_raw_data['Imprint Label'].append(row['interested party (3)'])

            if check_album_master and check_album_master.genre is not None:
                album_raw_data['Genre'].append(check_album_master.genre)
            else:
                album_raw_data['Genre'].append("None")

            if check_album_master and check_album_master.title is not None:
                album_raw_data['Product Title'].append(check_album_master.title)
            else:
                album_raw_data['Product Title'].append("None")
            
            if check_album_master and check_album_master.artist is not None:
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
                if not check_track.display_upc or check_track.display_upc is not row['manufacturer number']:
                    print(f"updating display upc ")
                    check_track.display_upc = row['manufacturer number']
                if not check_track.artist or check_track.artist is not row['interested party (1)']:
                    print(f"updating track artist")
                    check_track.artist = row['interested party (1)']
                if not check_track.title or check_track.title is not row['title']:
                    print(f"updating title ")
                    check_track.title = row['title']
                if not check_track.release_name or check_track.release_name is not row['product title']:
                    print(f"updating release name ")
                    check_track.release_name = row['product title']
                if not check_track.recording_artist or check_track.recording_artist is not row['interested party (1)']:
                    print(f"updating recording artist")
                    check_track.recording_artist = row['interested party (1)']
                if not check_track.publisher or check_track.publisher is not  row['member']:
                    print(f"updating publisher ")
                    check_track.publisher = row['member']
                if not check_track.label_name or check_track.label_name is not row['interested party (3)']:
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
        from_email = 'Trackeet Support <hello@pipminds.com>'
        
        #send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)


    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg) 

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':mcps_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
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
            from_email = 'Trackeet Support <hello@pipminds.com>'
            
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
        from_email = 'Trackeet Support <hello@pipminds.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)


        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ditto_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        
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
            from_email = 'Trackeet Support <hello@pipminds.com>'
            
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

                if check_track_master and check_track_master.track_duration is not None:
                    track_raw_data['TrackDuration'].append(check_track_master.track_duration)
                else:
                    track_raw_data['TrackDuration'].append("None")

                if check_track_master and check_track_master.display_upc is not None:
                    track_raw_data['DisplayUPC'].append(check_track_master.display_upc)
                else:
                    track_raw_data['DisplayUPC'].append(row['manual barcode'])
                
                if check_track_master and check_track_master.download is not None:
                    track_raw_data['Download'].append(check_track_master.download)
                else:
                    track_raw_data['Download'].append("None")
                
                if check_track_master and check_track_master.sales_start_date is not None:
                    track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
                else:
                    track_raw_data['SalesStartDate'].append("None")
                
                if check_track_master and check_track_master.sales_end_date is not None:
                    track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
                else:
                    track_raw_data['SalesEndDate'].append("None")
                
                if check_track_master and check_track_master.error is not None:
                    track_raw_data['Error'].append(check_track_master.error)
                else:
                    track_raw_data['Error'].append("None")

                if check_track_master and check_track_master.parental_advisory is not None:
                    track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
                else:
                    track_raw_data['ParentalAdvisory'].append("None")

                if check_track_master and check_track_master.preview_clip_start_time is not None:
                    track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
                else:
                    track_raw_data['PreviewClipStartTime'].append("None")

                if check_track_master and check_track_master.preview_clip_duration is not None:
                    track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
                else:
                    track_raw_data['PreviewClipDuration'].append("None")

                if check_track_master and check_track_master.song_version is not None:
                    track_raw_data['SongVersion'].append(check_track_master.song_version)
                else:
                    track_raw_data['SongVersion'].append("None")

                if check_track_master and check_track_master.p_line is not None:
                    track_raw_data['P_LINE'].append(check_track_master.p_line)
                else:
                    track_raw_data['P_LINE'].append("None")
                
                if check_track_master and check_track_master.track_no is not None:
                    track_raw_data['TrackNo'].append(check_track_master.track_no)
                else:
                    track_raw_data['TrackNo'].append("None")

                if check_track_master and check_track_master.hidden_track is not None:
                    track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
                else:
                    track_raw_data['HiddenTrack'].append("None")

                if check_track_master and check_track_master.artist is not None:
                    track_raw_data['Artist'].append(check_track_master.artist)
                else:
                    track_raw_data['Artist'].append(row['artist display name'])
                
                if check_track_master and check_track_master.title is not None:
                    track_raw_data['Title'].append(check_track_master.title)
                else:
                    track_raw_data['Title'].append(row['track title'])

                if check_track_master and check_track_master.release_name is not None:
                    track_raw_data['Release Name'].append(check_track_master.release_name)
                else:
                    track_raw_data['Release Name'].append(row['track title'])

                if check_track_master and  check_track_master.recording_artist is not None:
                    track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
                else:
                    track_raw_data['Recording Artist'].append(row['artist display name'])

                if check_track_master and check_track_master.label_name is not None:
                    track_raw_data['LabelName'].append(check_track_master.label_name)
                else:
                    track_raw_data['LabelName'].append("None")

                if check_track_master and check_track_master.volume_no is not None:
                    track_raw_data['VolumeNo'].append(check_track_master.volume_no)
                else:
                    track_raw_data['VolumeNo'].append("1")
                
                if check_track_master and check_track_master.genre is not None:
                    track_raw_data['Genre'].append(check_track_master.genre)
                else:
                    track_raw_data['Genre'].append("None")

                if check_track_master and check_track_master.publisher is not None:
                    track_raw_data['Publisher'].append(check_track_master.publisher)
                else:
                    track_raw_data['Publisher'].append("None")

                if check_track_master and check_track_master.producer is not None:
                    track_raw_data['Producer'].append(check_track_master.producer)
                else:
                    track_raw_data['Producer'].append("None")

                if check_track_master and check_track_master.writer is not None:
                    track_raw_data['Writer'].append(check_track_master.writer)
                else:
                    track_raw_data['Writer'].append("None")
                
                if check_track_master and check_track_master.arranger is not None:
                    track_raw_data['Arranger'].append(check_track_master.arranger)
                else:
                    track_raw_data['Arranger'].append("None")

                if check_track_master and check_track_master.territories is not None:
                    track_raw_data['Territories'].append(check_track_master.territories)
                else:
                    track_raw_data['Territories'].append(row['country'])
                
                if check_track_master and check_track_master.exclusive is not None:
                    track_raw_data['Exclusive'].append(check_track_master.exclusive)
                else:
                    track_raw_data['Exclusive'].append("None")

                if check_track_master and check_track_master.wholesale_price is not None:
                    track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
                else:
                    track_raw_data['WholesalePrice'].append("None")



                
                #ALBUM CHECKS

                album_raw_data['Product UPC'].append(row['manual barcode'])
                check_album_master = Master_DRMSYS.objects.filter(display_upc=row['manual barcode'], data_type="Album").first()
                if check_album_master:
                    print("DITTO CHECK MASTER ALBUM EXISTS")

                if check_album_master and check_album_master.c_line is not None:
                    album_raw_data['CLINE'].append(check_album_master.c_line)
                else:
                    album_raw_data['CLINE'].append("None")

                if check_album_master and check_album_master.master_carveouts is not None:
                    album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
                else:
                    album_raw_data['Master Carveouts'].append("None")

                if check_album_master and check_album_master.territories_carveouts is not None:
                    album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
                else:
                    album_raw_data['Territories Carveouts'].append("None")

                if check_album_master and check_album_master.deleted is not None:
                    album_raw_data['Deleted?'].append(check_album_master.deleted)
                else:
                    album_raw_data['Deleted?'].append("None")

                if check_album_master and check_album_master.manufacturer_upc is not None:
                    album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
                else:
                    album_raw_data['Manufacturer UPC'].append("None")

                if check_album_master and check_album_master.release_date is not None:
                    album_raw_data['Release Date'].append(check_album_master.release_date)
                else:
                    album_raw_data['Release Date'].append("None")

                if check_album_master and check_album_master.artist_url is not None:
                    album_raw_data['Artist URL'].append(check_album_master.artist_url)
                else:
                    album_raw_data['Artist URL'].append("None")

                if check_album_master and check_album_master.label_catalogno is not None:
                    album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
                else:
                    album_raw_data['Catalog Number'].append("None")


                # check_master_for_cat = Master_DRMSYS.objects.filter(label_catalogno=row['catalogue number'])
                # if check_master_for_cat:
                #     total_vol = check_master_for_cat.count()
                #     album_raw_data['Total Volumes'].append(str(total_vol))
                # else:
                #     album_raw_data['Total Volumes'].append('1')
                
                if check_album_master and check_album_master.total_volumes is not None:
                    album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
                else:
                    album_raw_data['Total Volumes'].append('1')

                if check_album_master and check_album_master.total_tracks is not None:
                    album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
                else:
                    album_raw_data['Total Tracks'].append('1')

                if check_album_master and check_album_master.label_name is not None:
                    album_raw_data['Imprint Label'].append(check_album_master.label_name)
                else:
                    album_raw_data['Imprint Label'].append("None")

                if check_album_master and check_album_master.genre is not None:
                    album_raw_data['Genre'].append(check_album_master.genre)
                else:
                    album_raw_data['Genre'].append("None")

                if check_album_master and check_album_master.title is not None:
                    album_raw_data['Product Title'].append(check_album_master.title)
                else:
                    album_raw_data['Product Title'].append(row['track title'])
                
                if check_album_master and check_album_master.artist is not None:
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
        from_email = 'Trackeet Support <hello@pipminds.com>'

        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com' , 'digger@wyldpytch.com'], html_message=html_message)

        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':ditto_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
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
            from_email = 'Trackeet Support <hello@pipminds.com>'
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
        from_email = 'Trackeet Support <hello@pipminds.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)


        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':horus_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise



############################
#     HORUS ACCOUNT         #
############################

@shared_task
def process_horus_artist(pk):
    horus_file = ProcessedDocument.objects.get(pk=pk)
    try:
        horus = pd.read_excel(horus_file.file_doc, Index=None).fillna('')
        horus.columns = horus.columns.str.strip().str.lower()
        # print(ppl.columns)
        if not {'artist'}.issubset(horus.columns):
            subject = 'CorruptedFile'
            html_message = render_to_string('mail/corrupt.html', {'file_name':horus_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
 
        else:
            pass

        artists = []
        for index, row in horus.iterrows():
            if row['artist'] not in artists:
                artists.append(row['artist'])
        
        print(artists)

        for val in artists:
            # print(ppl.loc[ppl['artist name'] == val])
            artist_df = horus.loc[horus['artist'] == val]
            artist_refined_name = val.replace(" ","_")
            artist_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
            artist_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }
            
            for index, row in artist_df.iterrows():
                # print(f" {val} in this row has isrc of {row['isrc']}")
                print(f"label name is {row['label']}")
                artist_data['Period'].append(f"{row['sales period']}")
                artist_data['Activity Period'].append(f"{row['date of sale']} - {row['date entered']} ")
                artist_data['Retailer'].append(f"{row['store']}")
                artist_data['Territory'].append(f"{row['country of sale']}")
                artist_data['Product UPC'].append(f"{row['barcode']}")
                artist_data['Manufacturer UPC'].append(" ")
                artist_data['Imprint Label'].append(f"{row['label']}")
                artist_data['Artist Name'].append(f"{row['artist']}")
                artist_data['Product Name'].append(f"{row['track title']}")
                artist_data['Track Name'].append(f"{row['track title']}")
                artist_data['Track Artist'].append(f"{row['artist']}")
                artist_data['ISRC'].append(f"{row['isrc']}")
                artist_data['Total'].append(f"{row['amount remaining']}")
                artist_data['Adjusted Total'].append(" ")
                artist_data['Label Share Net Receipts'].append(f"{row['line total to be paid to you']}")

            print(artist_data) 
            new_artist_df = pd.DataFrame({key:pd.Series(value) for key, value in artist_data.items() }, columns=artist_col)
            account_file_name  = f"processed_{artist_refined_name}_output_{today_date}.csv"
            track_account = new_artist_df.to_csv(account_file_name, index = False, header=True)
            new_account_document = ProcessedArtistFile()
            new_account_document.document = horus_file
            new_account_document.file_name = account_file_name
            new_account_document.save()

            with open(account_file_name, 'rb') as csv:
                new_account_document.file_doc.save( account_file_name, File(csv))


            print(f"Done with {val}")

        find_all_artist = ProcessedArtistFile.objects.filter(document=horus_file).all()

        subject, from_email, to = 'Processed Artist Data', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
        # text_content = f" Dear {user.username}. Welcome to Ultra "
        html_content = render_to_string('mail/processed_artists.html', {'file_name':horus_file.file_name})
        msg = EmailMessage(subject, html_content, from_email, to)
        msg.content_subtype = "html"
        for artist_file in find_all_artist:
            the_file = artist_file.file_doc 
            the_filename = str(artist_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_filename, response.content, mimetype="text/csv")
        msg.send()


    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':horus_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise




@shared_task
def process_horus_labels(pk):
    horus_file = ProcessedDocument.objects.get(pk=pk)
    try:
        horus = pd.read_excel(horus_file.file_doc, Index=None).fillna('')
        horus.columns = horus.columns.str.strip().str.lower()
      
        if not {'label'}.issubset(horus.columns):
            subject = 'CorruptedFile'
            html_message = render_to_string('mail/corrupt.html', {'file_name':horus_file.file_name, })
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>>'
            
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

            
        else:
            pass

        labels = []
        for index, row in horus.iterrows():
            if row['label'] not in labels:
                labels.append(row['label'])
        
        print(labels)

        for val in labels:
            # print(ppl.loc[ppl['artist name'] == val])
            label_df = horus.loc[horus['label'] == val]
            label_refined_name = val.replace(" ","_")
            label_col = ['Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
            label_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }
            
            for index, row in label_df.iterrows():
                print(f" {val} in this row has isrc of {row['isrc']}")


                print(f"label name is {row['label']}")
                label_data['Period'].append(f"{row['sales period']}")
                label_data['Activity Period'].append(f"{row['date of sale']} - {row['date entered']} ")
                label_data['Retailer'].append(f"{row['store']}")
                label_data['Territory'].append(f"{row['country of sale']}")
                label_data['Product UPC'].append(f"{row['barcode']}")
                label_data['Manufacturer UPC'].append(" ")
                label_data['Imprint Label'].append(f"{row['label']}")
                label_data['Artist Name'].append(f"{row['artist']}")
                label_data['Product Name'].append(f"{row['track title']}")
                label_data['Track Name'].append(f"{row['track title']}")
                label_data['Track Artist'].append(f"{row['artist']}")
                label_data['ISRC'].append(f"{row['isrc']}")
                label_data['Total'].append(f"{row['amount remaining']}")
                label_data['Adjusted Total'].append(" ")
                label_data['Label Share Net Receipts'].append(f"{row['line total to be paid to you']}")
 
            print(label_data) 
            new_label_df = pd.DataFrame({key:pd.Series(value) for key, value in label_data.items() }, columns=label_col)
            account_file_name  = f"processed_{label_refined_name}_output_{today_date}.csv"
            track_account = new_label_df.to_csv(account_file_name, index = False, header=True)
            new_account_document = ProcessedLabelFile()
            new_account_document.document = horus_file
            new_account_document.file_name = account_file_name
            new_account_document.save()

            with open(account_file_name, 'rb') as csv:
                new_account_document.file_doc.save( account_file_name, File(csv))

            print(f"Done with {val}")
        find_all_labels = ProcessedLabelFile.objects.filter(document=horus_file).all()
        subject, from_email, to = 'Processed Label Data', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
        html_content = render_to_string('mail/processed_labels.html', {'file_name':horus_file.file_name})
        msg = EmailMessage(subject, html_content, from_email, to)
        msg.content_subtype = "html"
        for label_file in find_all_labels:
            the_file = label_file.file_doc 
            the_filename = str(label_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_filename, response.content, mimetype="application/pdf")
        msg.send()
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':horus_file.file_name })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
    except:
        print("Unexpected error!")
        raise


@shared_task
def process_horus_account(pk):
    horus_file = ProcessedDocument.objects.get(pk=pk)
    statement_type = None
    p = lambda x: x/100
    try:
        horus = pd.read_excel(horus_file.file_doc, Index=None).fillna('')
      
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
                from_email = 'Trackeet Support <hello@pipminds.com>'
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


            #wrong_data_col = [ 'Period',  'Activity Period', 'DMS', 'Territory', 'Orchard UPC', 'Manufacturer UPC', 'Label Catalog', 'Imprint Label', 'Artist Name', 'Release Name', 'Track Name', 'ISRC', 'Volume', 'Track', 'Quantity','Unit Price', 'Gross', 'Trans Type',  'Adjusted Gross', 'Split Rate', 'Label Share Net Receipts', 'Ringtone Publishing', 'Cloud Publishing', 'Publishing', 'Mech Administrative Fee', 'Preferred Currency' ]
            wrong_data_col = [ 'Date Of Sale', 	'Date Entered',	'Sales Period',	'Store', 'Store Type',	'Country Of Sale',	'Label', 'Release Title', 'Artist', 'Track Title', 'Track Artist', 'Barcode', 'Catalogue Number', 'ISRC',	'Quantity',	'Store Price',	'Mechanical Deductions', 	'Amount Paid by Store',	'Distributor Share (%)', 	'Distributor Amount (£)',	'Amount Remaining', 'Label Share (%)', 	'Label Amount (£)',	'Artists Share (%)', 'Artists Amount (£)',	'Line Total to be paid to you',	'Tax:  Country',	'Tax:  % Deducted',	'Tax:  Amount Deducted', 'Your Contract Number' ]
            wrong_data_raw_data = {
                'Date Of Sale':[], 'Date Entered':[],	'Sales Period':[],	'Store':[], 'Store Type':[],	'Country Of Sale':[],	'Label':[], 'Release Title':[],'Artist':[], 'Track Title':[], 'Track Artist':[], 'Barcode':[], 'Catalogue Number':[], 'ISRC':[],	'Quantity':[],	'Store Price':[],	'Mechanical Deductions':[], 	'Amount Paid by Store':[],	'Distributor Share (%)':[], 	'Distributor Amount (£)':[],	'Amount Remaining':[], 'Label Share (%)':[], 	'Label Amount (£)':[],	'Artists Share (%)':[], 'Artists Amount (£)':[],	'Line Total to be paid to you':[],	'Tax:  Country':[],	'Tax:  % Deducted':[],	'Tax:  Amount Deducted':[], 'Your Contract Number':[]
            }

            missing_data_col = [ 'Date Of Sale','Date Entered','Sales Period','Store','Store Type','Country Of Sale','Label','Release Title','Artist','Track Title','Track Artist','Barcode','Catalogue Number','ISRC','Quantity','Store Price','Mechanical Deductions', 'Amount Paid by Store','Distributor Share (%)', 'Distributor Amount (£)','Amount Remaining','Label Share (%)', 'Label Amount (£)','Artists Share (%)','Artists Amount (£)','Line Total to be paid to you','Tax: Country',	'Tax: % Deducted',	'Tax: Amount Deducted','Your Contract Number' ]
            missing_data_raw_data = {
                'Date Of Sale':[], 'Date Entered':[],	'Sales Period':[],	'Store':[], 'Store Type':[],	'Country Of Sale':[],	'Label':[], 'Release Title':[],'Artist':[], 'Track Title':[], 'Track Artist':[], 'Barcode':[], 'Catalogue Number':[], 'ISRC':[],	'Quantity':[],	'Store Price':[],	'Mechanical Deductions':[], 	'Amount Paid by Store':[],	'Distributor Share (%)':[], 	'Distributor Amount (£)':[],	'Amount Remaining':[], 'Label Share (%)':[], 	'Label Amount (£)':[],	'Artists Share (%)':[], 'Artists Amount (£)':[],	'Line Total to be paid to you':[],	'Tax: Country':[],	'Tax: % Deducted':[],	'Tax: Amount Deducted':[], 'Your Contract Number':[]
            }

            for index, row in horus.iterrows():

                check_record = Master_DRMSYS.objects.filter(
                     Q(title=f"{row['track title']}", data_type='Track') | Q(isrc=f"{row['isrc']}", data_type='Track')
                )
                #check_record = Master_DRMSYS.objects.filter(title=f"{row['track title']}", data_type='Track')
                if check_record.exists():
                    print(check_record.first())
                    # check_label = VerifiedLabels.objects.filter(label_name__contains=str(row['label']).upper())
                    # if check_label.exists():

                    track_raw_data['ISRC'].append(f"{row['isrc']}")
                    track_raw_data['DisplayUPC'].append(f"{row['barcode']}")


            
                    check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                    if check_track_master:
                        print("HORUS CHECK TRACK MASTER EXISTS")

                    if check_track_master and check_track_master.track_duration is not None:
                        track_raw_data['TrackDuration'].append(f"{check_track_master.track_duration}")
                    else:
                        track_raw_data['TrackDuration'].append("None")

                    
                    
                    if check_track_master and check_track_master.download is not None:
                        track_raw_data['Download'].append(f"{check_track_master.download}")
                    else:
                        track_raw_data['Download'].append("None")
                    
                    if check_track_master and check_track_master.sales_start_date is not None:
                        track_raw_data['SalesStartDate'].append(f"{check_track_master.sales_start_date}")
                    else:
                        track_raw_data['SalesStartDate'].append(f"{row['sales period']}")
                    
                    if check_track_master and check_track_master.sales_end_date is not None:
                        track_raw_data['SalesEndDate'].append(f"{check_track_master.sales_end_date}")
                    else:
                        track_raw_data['SalesEndDate'].append(f"{row['sales period']}")
                    
                    if check_track_master and check_track_master.error is not None:
                        track_raw_data['Error'].append(f"{check_track_master.error}")
                    else:
                        track_raw_data['Error'].append("None")

                    if check_track_master and check_track_master.parental_advisory is not None:
                        track_raw_data['ParentalAdvisory'].append(f"{check_track_master.parental_advisory}")
                    else:
                        track_raw_data['ParentalAdvisory'].append("None")

                    if check_track_master and check_track_master.preview_clip_start_time is not None:
                        track_raw_data['PreviewClipStartTime'].append(f"{check_track_master.preview_clip_start_time}")
                    else:
                        track_raw_data['PreviewClipStartTime'].append("None")

                    if check_track_master and check_track_master.preview_clip_duration is not None:
                        track_raw_data['PreviewClipDuration'].append(f"{check_track_master.preview_clip_duration}")
                    else:
                        track_raw_data['PreviewClipDuration'].append("None")

                    if check_track_master and check_track_master.song_version is not None:
                        track_raw_data['SongVersion'].append(f"{check_track_master.song_version}")
                    else:
                        track_raw_data['SongVersion'].append("None")

                    if check_track_master and check_track_master.p_line is not None:
                        track_raw_data['P_LINE'].append(f"{check_track_master.p_line}")
                    else:
                        track_raw_data['P_LINE'].append("None")
                    
                    if check_track_master and check_track_master.track_no is not None:
                        track_raw_data['TrackNo'].append(f"{check_track_master.track_no}")
                    else:
                        track_raw_data['TrackNo'].append("None")

                    if check_track_master and check_track_master.hidden_track is not None:
                        track_raw_data['HiddenTrack'].append(f"{check_track_master.hidden_track}")
                    else:
                        track_raw_data['HiddenTrack'].append("None")

                    if check_track_master and check_track_master.artist is not None:
                        track_raw_data['Artist'].append(f"{check_track_master.artist}")
                    else:
                        track_raw_data['Artist'].append(f"{row['artist']}")
                    
                    if check_track_master and check_track_master.title is not None:
                        track_raw_data['Title'].append(f"{check_track_master.title}")
                    else:
                        track_raw_data['Title'].append(f"{row['track title']}")

                    if check_track_master and check_track_master.release_name is not None:
                        track_raw_data['Release Name'].append(f"{check_track_master.release_name}")
                    else:
                        track_raw_data['Release Name'].append(f"{row['release title']}")

                    if check_track_master and  check_track_master.recording_artist is not None:
                        track_raw_data['Recording Artist'].append(f"{check_track_master.recording_artist}")
                    else:
                        track_raw_data['Recording Artist'].append(f"{row['artist']}")

                    if check_track_master and check_track_master.label_name is not None:
                        track_raw_data['LabelName'].append(f"{check_track_master.label_name}")
                    else:
                        track_raw_data['LabelName'].append(f"{row['label']}")

                    if check_track_master and check_track_master.volume_no is not None:
                        track_raw_data['VolumeNo'].append(f"{check_track_master.volume_no}")
                    else:
                        track_raw_data['VolumeNo'].append(f"{row['quantity']}")
                    
                    if check_track_master and check_track_master.genre is not None:
                        track_raw_data['Genre'].append(f"{check_track_master.genre}")
                    else:
                        track_raw_data['Genre'].append("None")

                    if check_track_master and check_track_master.publisher is not None:
                        track_raw_data['Publisher'].append(f"{check_track_master.publisher}")
                    else:
                        track_raw_data['Publisher'].append("None")

                    if check_track_master and check_track_master.producer is not None:
                        track_raw_data['Producer'].append(f"{check_track_master.producer}")
                    else:
                        track_raw_data['Producer'].append("None")

                    if check_track_master and check_track_master.writer is not None:
                        track_raw_data['Writer'].append(f"{check_track_master.writer}")
                    else:
                        track_raw_data['Writer'].append("None")
                    
                    if check_track_master and check_track_master.arranger is not None:
                        track_raw_data['Arranger'].append(f"{check_track_master.arranger}")
                    else:
                        track_raw_data['Arranger'].append("None")

                    if check_track_master and check_track_master.territories is not None:
                        track_raw_data['Territories'].append(f"{check_track_master.territories}")
                    else:
                        track_raw_data['Territories'].append(f"{row['country of sale']}")
                    
                    if check_track_master and check_track_master.exclusive is not None:
                        track_raw_data['Exclusive'].append(f"{check_track_master.exclusive}")
                    else:
                        track_raw_data['Exclusive'].append("None")

                    if check_track_master and check_track_master.wholesale_price is not None:
                        track_raw_data['WholesalePrice'].append(f"{check_track_master.wholesale_price}")
                    else:
                        track_raw_data['WholesalePrice'].append("None")

                

                    #ALBUM CHECKS

                    album_raw_data['Product UPC'].append(f"{row['barcode']}")
                    check_album_master = Master_DRMSYS.objects.filter(display_upc=f"{row['barcode']}", data_type="Album").first()
                    if check_album_master:
                        print("PPL CHECK MASTER ALBUM EXISTS")

                    if check_album_master and check_album_master.c_line is not None:
                        album_raw_data['CLINE'].append(f"{check_album_master.c_line}")
                    else:
                        album_raw_data['CLINE'].append("None")

                    if check_album_master and check_album_master.master_carveouts is not None:
                        album_raw_data['Master Carveouts'].append(f"{check_album_master.master_carveouts}")
                    else:
                        album_raw_data['Master Carveouts'].append("None")

                    if check_album_master and check_album_master.territories_carveouts is not None:
                        album_raw_data['Territories Carveouts'].append(f"{check_album_master.territories_carveouts}")
                    else:
                        album_raw_data['Territories Carveouts'].append(f"{row['country of sale']}")

                    if check_album_master and check_album_master.deleted is not None:
                        album_raw_data['Deleted?'].append(f"{check_album_master.deleted}")
                    else:
                        album_raw_data['Deleted?'].append("None")

                    if check_album_master and check_album_master.manufacturer_upc is not None:
                        album_raw_data['Manufacturer UPC'].append(f"{check_album_master.manufacturer_upc}")
                    else:
                        album_raw_data['Manufacturer UPC'].append("None")

                    if check_album_master and check_album_master.release_date is not None:
                        album_raw_data['Release Date'].append(f"{check_album_master.release_date}")
                    else:
                        album_raw_data['Release Date'].append("None")

                    if check_album_master and check_album_master.artist_url is not None:
                        album_raw_data['Artist URL'].append(f"{check_album_master.artist_url}")
                    else:
                        album_raw_data['Artist URL'].append("None")

                    if check_album_master and check_album_master.label_catalogno is not None:
                        album_raw_data['Catalog Number'].append(f"{check_album_master.label_catalogno}")
                    else:
                        album_raw_data['Catalog Number'].append(f"{row['catalogue number']}")


                    check_master_for_cat = Master_DRMSYS.objects.filter(label_catalogno=row['catalogue number'])
                    if check_master_for_cat:
                        total_vol = check_master_for_cat.count()
                        album_raw_data['Total Volumes'].append(str(total_vol))
                    else:
                        album_raw_data['Total Volumes'].append('1')
                
                    
                    if check_album_master and check_album_master.total_tracks is not None:
                        album_raw_data['Total Tracks'].append(f"{check_album_master.total_tracks}")
                    else:
                        album_raw_data['Total Tracks'].append('1')

                    if check_album_master and check_album_master.label_name is not None:
                        album_raw_data['Imprint Label'].append(f"{check_album_master.label_name}")
                    else:
                        album_raw_data['Imprint Label'].append(f"{row['label']}")

                    if check_album_master and check_album_master.genre is not None:
                        album_raw_data['Genre'].append(f"{check_album_master.genre}")
                    else:
                        album_raw_data['Genre'].append("None")

                    if check_album_master and check_album_master.title is not None:
                        album_raw_data['Product Title'].append(f"{check_album_master.title}")
                    else:
                        album_raw_data['Product Title'].append(f"{row['track title']}")
                    
                    if check_album_master and check_album_master.artist is not None:
                        album_raw_data['Product Artist'].append(f"{check_album_master.artist}")
                    else:
                        album_raw_data['Product Artist'].append(f"{row['artist']}")


                    account_raw_data['Period'].append(f"{row['sales period']}")
                    account_raw_data['Activity Period'].append(f"{row['date of sale']} - {row['date entered']} ")
                    account_raw_data['Retailer'].append(f"{row['store']}")
                    account_raw_data['Product UPC'].append(f"{row['barcode']}")
                    account_raw_data['Territory'].append(f"{row['country of sale']}")
                    account_raw_data['Project Code'].append(f"{row['track title']}")
                    account_raw_data['Product Code'].append(f"{row['barcode']}")
                    account_raw_data['Imprint Label'].append(f"{row['label']}")
                    account_raw_data['Artist Name'].append(f"{row['artist']}")
                    account_raw_data['Product Name'].append(f"{row['track title']}")
                    account_raw_data['ISRC'].append(f"{row['isrc']}")
                    account_raw_data['Volume'].append(f"{row['quantity']}")
                    account_raw_data['Unit Price'].append(f"{row['amount paid by store']}")
                    # account_raw_data['Discount'].append(row['withhold'])
                    account_raw_data['Price'].append(f"{row['amount remaining']}")
                    account_raw_data['Quantity'].append(f"{row['quantity']}")
                    account_raw_data['Total'].append(f"{row['amount remaining']}")
                    account_raw_data['Label Share Net Receipts'].append(f"{row['line total to be paid to you']}")
                    new_account, created = Accounting.objects.get_or_create( period = f"{row['sales period']}", activity_period =f"{row['date of sale']} - {row['date entered']} ", retailer = f"{row['store']}", territory = f"{row['country of sale']}", orchard_UPC = f"{row['barcode']}", manufacturer_UPC = f"{row['barcode']}", imprint_label = f"{row['label']}", artist_name = f"{row['artist']}", product_name = f"{row['track title']}", track_name = f"{row['track title']}", track_artist = f"{row['artist']}", isrc = f"{row['isrc']}", volume = f"{row['quantity']}", unit_price = f"{row['amount paid by store']}", actual_price = f"{row['amount remaining']}", quantity = f"{row['amount remaining']}", total = f"{row['amount remaining']}", label_share_net_receipts = f"{row['line total to be paid to you']}", vendor="HORUS", processed_day=f"{today}")
                else:
                    print(f"This track record is missing from the drmsys ")
                    missing_data_raw_data['Date Of Sale'].append(f"{row['date of sale']}")
                    missing_data_raw_data['Date Entered'].append(f"{row['date entered']}")
                    missing_data_raw_data['Sales Period'].append(f"{row['sales period']}")
                    missing_data_raw_data['Store'].append(f"{row['store']}")
                    missing_data_raw_data['Store Type'].append(f"{row['store type']}")
                    missing_data_raw_data['Country Of Sale'].append(f"{row['country of sale']}")
                    missing_data_raw_data['Label'].append(f"{row['label']}")
                    missing_data_raw_data['Release Title'].append(f"{row['release title']}")
                    missing_data_raw_data['Artist'].append(f"{row['artist']}")                   
                    missing_data_raw_data['Track Title'].append(f"{row['track title']}")
                    missing_data_raw_data['Track Artist'].append(f"{row['track artist']}")
                    missing_data_raw_data['Barcode'].append(f"{row['barcode']}")
                    missing_data_raw_data['Catalogue Number'].append(f"{row['catalogue number']}")
                    missing_data_raw_data['ISRC'].append(f"{row['isrc']}")
                    missing_data_raw_data['Quantity'].append(f"{row['quantity']}")
                    missing_data_raw_data['Store Price'].append(f"{row['store price']}")
                    missing_data_raw_data['Mechanical Deductions'].append(f"{row['mechanical deductions']}")
                    missing_data_raw_data['Amount Paid by Store'].append(f"{row['amount paid by store']}")
                    missing_data_raw_data['Distributor Share (%)'].append(f"{row['distributor share (%)']}")
                    missing_data_raw_data['Distributor Amount (£)'].append(f"{row['distributor amount (£)']}")
                    missing_data_raw_data['Amount Remaining'].append(f"{row['amount remaining']}")
                    missing_data_raw_data['Label Share (%)'].append(f"{row['label share (%)']}")
                    missing_data_raw_data['Label Amount (£)'].append(f"{row['label amount (£)']}")
                    missing_data_raw_data['Artists Share (%)'].append(f"{row['artists share (%)']}")
                    missing_data_raw_data['Artists Amount (£)'].append(f"{row['artists amount (£)']}")
                    missing_data_raw_data['Line Total to be paid to you'].append(f"{row['line total to be paid to you']}")

            print("printing track data ")
            print(track_raw_data) 
            new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
            # new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
            track_file_name  = f"processed_horus_track_output_{today_date}.csv"
            track_file_name_txt  = f"processed_horus_track_output_{today_date}.txt"
            track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
            track_file_txt = new_track_df.to_csv(track_file_name_txt, index = False, header=True, sep='\t')
            new_track_document = ProcessedTrackFile()
            new_track_document.document = horus_file
            new_track_document.file_name = track_file_name
            new_track_document.file_name_txt = track_file_name_txt
            new_track_document.save()

            with open(track_file_name, 'rb') as csv:
                new_track_document.file_doc.save(track_file_name, File(csv))

            with open(track_file_name_txt, 'rb') as txt:
                new_track_document.file_doc_txt.save(track_file_name_txt, File(txt))
            
            print("printing album data ")
            print(album_raw_data)
            new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
            # new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
            album_file_name  = f"processed_horus_album_output_{today_date}.csv"
            album_file_name_txt  = f"processed_horus_album_output_{today_date}.txt"
            album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
            album_file_txt = new_album_df.to_csv(album_file_name_txt, index = False, header=True, sep='\t')
            new_album_document = ProcessedAlbumFile()
            new_album_document.document = horus_file
            new_album_document.file_name = album_file_name
            new_album_document.file_name_txt = album_file_name_txt
            new_album_document.save()

            with open( album_file_name, 'rb') as csv:
                new_album_document.file_doc.save(album_file_name, File(csv))

            with open(album_file_name_txt, 'rb') as txt:
                new_album_document.file_doc_txt.save(album_file_name_txt, File(txt))

            print("printing accounting data")
            print(account_raw_data)
            new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
            account_file_name  = f"processed_horus_account_output_{today_date}.csv"
            track_account = new_account_df.to_csv(account_file_name, index = False, header=True)
            new_account_document = ProcessedAccountFile()
            new_account_document.document = horus_file
            new_account_document.file_name = account_file_name
            new_account_document.save()

            with open(account_file_name, 'rb') as csv:
                new_account_document.file_doc.save( account_file_name, File(csv))

            
            print(len(missing_data_raw_data))
            print(missing_data_raw_data)
            if len(missing_data_raw_data) != 0:
                new_missing_data_df = pd.DataFrame({key:pd.Series(value) for key, value in missing_data_raw_data.items() }, columns=missing_data_col)
                new_missing_data_file_name  = f"new_missing_data_{today_date}.csv"
                new_missing_data = new_missing_data_df.to_csv(new_missing_data_file_name, index = False, header=True)
                new_missing_data_doc = MissingRecordsDoc()
                new_missing_data_doc.document = horus_file
                new_missing_data_doc.file_name = new_missing_data_file_name
                new_missing_data_doc.save()

                with open(new_missing_data_file_name, 'rb') as csv:
                    new_missing_data_doc.file_doc.save( new_missing_data_file_name, File(csv))

            
                the_missing_file = MissingRecordsDoc.objects.filter(document=horus_file).last()

                subject, from_email, to = 'Missing Data in DRMSYS', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
                
                html_content = render_to_string('mail/missing_data.html', {'missing_data':new_missing_data_doc.file_doc.url})
                msg = EmailMessage(subject, html_content, from_email, to)
                msg.content_subtype = "html"
                the_file = the_missing_file.file_doc 
                the_filename = str(the_missing_file.file_name)
                response = requests.get(the_file.url)
                msg.attach(the_filename, response.content, mimetype="text/csv")
                msg.send()

            get_track_doc = ProcessedTrackFile.objects.filter(document=horus_file).last()
            get_album_doc = ProcessedAlbumFile.objects.filter(document=horus_file).last()
            get_account_doc = ProcessedAccountFile.objects.filter(document=horus_file).last()

            the_track_doc_file = get_track_doc.file_doc 
            the_track_doc_filename = str(get_track_doc.file_name)
            track_response = requests.get(the_track_doc_file.url)

            the_album_doc_file = get_album_doc.file_doc 
            the_album_doc_filename = str(get_album_doc.file_name)
            album_response = requests.get(the_album_doc_file.url)

            the_account_doc_file = get_account_doc.file_doc 
            the_account_doc_filename = str(get_account_doc.file_name)
            account_response = requests.get(the_account_doc_file.url)

            subject, from_email, to = 'File Processed', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
            
            html_content = render_to_string('mail/processed.html', {'file_name':horus_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            # the_file = the_missing_file.file_doc 
            # the_filename = str(the_missing_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_track_doc_filename, track_response.content, mimetype="text/csv")
            msg.attach(the_album_doc_filename, album_response.content, mimetype="text/csv")
            msg.attach(the_account_doc_filename, account_response.content, mimetype="text/csv")
            msg.send()




            
        # YOUTUBE STATEMENT TYPE VARIATION  
        elif statement_type == 'yt':
            if not {'upc', 'isrc'}.issubset(horus.columns):

                subject = 'Corrupted File'
                html_message = render_to_string('mail/corrupt.html', {'file_name':horus_file.file_name, })
                plain_message = strip_tags(html_message)
                from_email = 'Trackeet Support <hello@pipminds.com>'
                
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

            missing_data_col = [ 'Adjustment Type', 'Day', 'Country', 'Asset ID', 'Asset Title', 'Asset Labels', 'Asset Channel ID', 'Asset Type', 'Custom ID', 'ISRC', 'UPC', 'GRid', 'Artist', 'Album', 'Label', 'Administer Publish Rights', 'Owned Views', 'YouTube Revenue Split : Auction', 'YouTube Revenue Split : Reserved', 'YouTube Revenue Split : Partner Sold YouTube Served', 'YouTube Revenue Split : Partner Sold Partner Served', 'YouTube Revenue Split', 'Partner Revenue : Auction', 'Partner Revenue : Reserved', 'Partner Revenue : Partner Sold YouTube Served', 'Partner Revenue : Partner Sold Partner Served', 'Partner Revenue']

            missing_data_raw_data = {
                        'Adjustment Type':[], 'Day':[], 'Country':[], 'Asset ID':[], 'Asset Title':[], 'Asset Labels':[], 'Asset Channel ID':[], 'Asset Type':[], 'Custom ID':[], 'ISRC':[], 'UPC':[], 'GRid':[], 'Artist':[], 'Album':[], 'Label':[], 'Administer Publish Rights':[], 'Owned Views':[], 'YouTube Revenue Split : Auction':[], 'YouTube Revenue Split : Reserved':[], 'YouTube Revenue Split : Partner Sold YouTube Served':[], 'YouTube Revenue Split : Partner Sold Partner Served':[], 'YouTube Revenue Split':[], 'Partner Revenue : Auction':[], 'Partner Revenue : Reserved':[], 'Partner Revenue : Partner Sold YouTube Served':[], 'Partner Revenue : Partner Sold Partner Served':[], 'Partner Revenue':[]
            }


            for index, row in horus.iterrows():

                check_record = Master_DRMSYS.objects.filter(
                     Q(title=f"{row['track title']}", data_type='Track') | Q(isrc=f"{row['isrc']}", data_type='Track')
                )
                if check_record.exists():
                    print(check_record.first())

                # if  {'upc', 'isrc'}.issubset(horus.columns):

                    track_raw_data['ISRC'].append(f"{row['isrc']}")

                    check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                    if check_track_master:
                        print("HORUS CHECK TRACK MASTER EXISTS")

                    if check_track_master and check_track_master.track_duration is not None:
                        track_raw_data['TrackDuration'].append(f"{check_track_master.track_duration}")
                    else:
                        track_raw_data['TrackDuration'].append("None")

                    if check_track_master and check_track_master.display_upc is not None:
                        track_raw_data['DisplayUPC'].append(f"{check_track_master.display_upc}")
                    else:
                        track_raw_data['DisplayUPC'].append(f"{row['upc']}")
                    
                    if check_track_master and check_track_master.download is not None:
                        track_raw_data['Download'].append(f"{check_track_master.download}")
                    else:
                        track_raw_data['Download'].append("None")
                    
                    if check_track_master and check_track_master.sales_start_date is not None:
                        track_raw_data['SalesStartDate'].append(f"{check_track_master.sales_start_date}")
                    else:
                        track_raw_data['SalesStartDate'].append("None")
                    
                    if check_track_master and check_track_master.sales_end_date is not None:
                        track_raw_data['SalesEndDate'].append(f"{check_track_master.sales_end_date}")
                    else:
                        track_raw_data['SalesEndDate'].append("None")
                    
                    if check_track_master and check_track_master.error is not None:
                        track_raw_data['Error'].append(f"{check_track_master.error}")
                    else:
                        track_raw_data['Error'].append("None")

                    if check_track_master and check_track_master.parental_advisory is not None:
                        track_raw_data['ParentalAdvisory'].append(f"{check_track_master.parental_advisory}")
                    else:
                        track_raw_data['ParentalAdvisory'].append("None")

                    if check_track_master and check_track_master.preview_clip_start_time is not None:
                        track_raw_data['PreviewClipStartTime'].append(f"{check_track_master.preview_clip_start_time}")
                    else:
                        track_raw_data['PreviewClipStartTime'].append("None")

                    if check_track_master and check_track_master.preview_clip_duration is not None:
                        track_raw_data['PreviewClipDuration'].append(f"{check_track_master.preview_clip_duration}")
                    else:
                        track_raw_data['PreviewClipDuration'].append("None")

                    if check_track_master and check_track_master.song_version is not None:
                        track_raw_data['SongVersion'].append(f"{check_track_master.song_version}")
                    else:
                        track_raw_data['SongVersion'].append("None")

                    if check_track_master and check_track_master.p_line is not None:
                        track_raw_data['P_LINE'].append(f"{check_track_master.p_line}")
                    else:
                        track_raw_data['P_LINE'].append("None")
                    
                    if check_track_master and check_track_master.track_no is not None:
                        track_raw_data['TrackNo'].append(f"{check_track_master.track_no}")
                    else:
                        track_raw_data['TrackNo'].append("None")

                    if check_track_master and check_track_master.hidden_track is not None:
                        track_raw_data['HiddenTrack'].append(f"{check_track_master.hidden_track}")
                    else:
                        track_raw_data['HiddenTrack'].append("None")

                    if check_track_master and check_track_master.artist is not None:
                        track_raw_data['Artist'].append(f"{check_track_master.artist}")
                    else:
                        track_raw_data['Artist'].append(f"{row['artist']}")
                    
                    if check_track_master and check_track_master.title is not None:
                        track_raw_data['Title'].append(f"{check_track_master.title}")
                    else:
                        track_raw_data['Title'].append(f"{row['asset title']}")

                    if check_track_master and check_track_master.release_name is not None:
                        track_raw_data['Release Name'].append(f"{check_track_master.release_name}")
                    else:
                        track_raw_data['Release Name'].append(f"{row['asset title']}")

                    if check_track_master and  check_track_master.recording_artist is not None:
                        track_raw_data['Recording Artist'].append(f"{check_track_master.recording_artist}")
                    else:
                        track_raw_data['Recording Artist'].append(f"{row['artist']}")

                    if check_track_master and check_track_master.label_name is not None:
                        track_raw_data['LabelName'].append(f"{check_track_master.label_name}")
                    else:
                        track_raw_data['LabelName'].append(row['label'])

                    if check_track_master and check_track_master.volume_no is not None:
                        track_raw_data['VolumeNo'].append(f"{check_track_master.volume_no}")
                    else:
                        track_raw_data['VolumeNo'].append('1')
                    
                    if check_track_master and check_track_master.genre is not None:
                        track_raw_data['Genre'].append(f"{check_track_master.genre}")
                    else:
                        track_raw_data['Genre'].append("None")

                    if check_track_master and check_track_master.publisher is not None:
                        track_raw_data['Publisher'].append(f"{check_track_master.publisher}")
                    else:
                        track_raw_data['Publisher'].append("None")

                    if check_track_master and check_track_master.producer is not None:
                        track_raw_data['Producer'].append(f"{check_track_master.producer}")
                    else:
                        track_raw_data['Producer'].append("None")

                    if check_track_master and check_track_master.writer is not None:
                        track_raw_data['Writer'].append(f"{check_track_master.writer}")
                    else:
                        track_raw_data['Writer'].append("None")
                    
                    if check_track_master and check_track_master.arranger is not None:
                        track_raw_data['Arranger'].append(f"{check_track_master.arranger}")
                    else:
                        track_raw_data['Arranger'].append("None")

                    if check_track_master and check_track_master.territories is not None:
                        track_raw_data['Territories'].append(f"{check_track_master.territories}")
                    else:
                        track_raw_data['Territories'].append(f"{row['country']}")
                    
                    if check_track_master and check_track_master.exclusive is not None:
                        track_raw_data['Exclusive'].append(f"{check_track_master.exclusive}")
                    else:
                        track_raw_data['Exclusive'].append("None")

                    if check_track_master and check_track_master.wholesale_price is not None:
                        track_raw_data['WholesalePrice'].append(f"{check_track_master.wholesale_price}")
                    else:
                        track_raw_data['WholesalePrice'].append("None")

                    

                    #ALBUM CHECKS

                    album_raw_data['Product UPC'].append(row['upc'])
                    check_album_master = Master_DRMSYS.objects.filter(display_upc=f"{row['upc']}", data_type="Album").first()
                    if check_album_master:
                        print("PPL CHECK MASTER ALBUM EXISTS")

                    if check_album_master and check_album_master.c_line is not None:
                        album_raw_data['CLINE'].append(f"{check_album_master.c_line}")
                    else:
                        album_raw_data['CLINE'].append("None")

                    if check_album_master and check_album_master.master_carveouts is not None:
                        album_raw_data['Master Carveouts'].append(f"{check_album_master.master_carveouts}")
                    else:
                        album_raw_data['Master Carveouts'].append("None")

                    if check_album_master and check_album_master.territories_carveouts is not None:
                        album_raw_data['Territories Carveouts'].append(f"{check_album_master.territories_carveouts}")
                    else:
                        album_raw_data['Territories Carveouts'].append("None")

                    if check_album_master and check_album_master.deleted is not None:
                        album_raw_data['Deleted?'].append(f"{check_album_master.deleted}")
                    else:
                        album_raw_data['Deleted?'].append("None")

                    if check_album_master and check_album_master.manufacturer_upc is not None:
                        album_raw_data['Manufacturer UPC'].append(f"{check_album_master.manufacturer_upc}")
                    else:
                        album_raw_data['Manufacturer UPC'].append("None")

                    if check_album_master and check_album_master.release_date is not None:
                        album_raw_data['Release Date'].append(f"{check_album_master.release_date}")
                    else:
                        album_raw_data['Release Date'].append("None")

                    if check_album_master and check_album_master.artist_url is not None:
                        album_raw_data['Artist URL'].append(f"{check_album_master.artist_url}")
                    else:
                        album_raw_data['Artist URL'].append("None")

                    if check_album_master and check_album_master.label_catalogno is not None:
                        album_raw_data['Catalog Number'].append(f"{check_album_master.label_catalogno}")
                    else:
                        album_raw_data['Catalog Number'].append(f"{row['asset id']}")


                    if check_album_master and check_album_master.total_volumes is not None:
                        album_raw_data['Total Volumes'].append(f"{check_album_master.total_volumes}")
                    else:
                        album_raw_data['Total Volumes'].append(f"{row['owned views']}")

                    if check_album_master and check_album_master.total_tracks is not None:
                        album_raw_data['Total Tracks'].append(f"{check_album_master.total_tracks}")
                    else:
                        album_raw_data['Total Tracks'].append('1')

                    if check_album_master and check_album_master.label_name is not None:
                        album_raw_data['Imprint Label'].append(f"{check_album_master.label_name}")
                    else:
                        album_raw_data['Imprint Label'].append(f"{row['label']}")

                    if check_album_master and check_album_master.genre is not None:
                        album_raw_data['Genre'].append(f"{check_album_master.genre}")
                    else:
                        album_raw_data['Genre'].append("None")

                    if check_album_master and check_album_master.title is not None:
                        album_raw_data['Product Title'].append(f"{check_album_master.title}")
                    else:
                        album_raw_data['Product Title'].append(f"{row['asset title']}")
                    
                    if check_album_master and check_album_master.artist is not None:
                        album_raw_data['Product Artist'].append(f"{check_album_master.artist}")
                    else:
                        album_raw_data['Product Artist'].append(f"{row['artist']}")


                    account_raw_data['Period'].append(f"{row['day']}")
                    # account_raw_data['Activity Period'].append(f"{row['date of sale']} - {row['date entered']} ")
                    account_raw_data['Retailer'].append('youtube')
                    account_raw_data['Product UPC'].append(f"{row['upc']}")
                    account_raw_data['Territory'].append(f"{row['country']}")
                    account_raw_data['Project Code'].append(f"{row['asset title']}")
                    account_raw_data['Product Code'].append(f"{row['asset id']}")
                    account_raw_data['Imprint Label'].append(f"{row['label']}")
                    account_raw_data['Artist Name'].append(f"{row['artist']}")
                    account_raw_data['Product Name'].append(f"{row['asset title']}")
                    account_raw_data['ISRC'].append(f"{row['isrc']}")
                    account_raw_data['Volume'].append(f"{row['owned views']}")
                    #account_raw_data['Unit Price'].append(row['amount paid by store'])
                    # account_raw_data['Discount'].append(row['withhold'])
                    account_raw_data['Price'].append(f"{row['youtube revenue split']}")
                    account_raw_data['Quantity'].append(f"{row['owned views']}")
                    
                    revenue = row['partner revenue'] - row['partner revenue'] * p(20)
                    account_raw_data['Total'].append(f"{revenue}")
                    account_raw_data['Label Share Net Receipts'].append(f"{revenue}")


                    new_account = Accounting( period = row['day'], retailer = 'youtube', territory = f"{row['country']}", orchard_UPC = f"{row['upc']}", manufacturer_UPC = f"{row['upc']}", imprint_label = f"{row['label']}", artist_name = f"{row['artist']}", product_name = f"{row['asset title']}", track_name = f"{row['asset title']}", track_artist = f"{row['artist']}", isrc = f"{row['isrc']}", volume = f"{row['owned views']}",  quantity = f"{row['owned views']}", total = f"{revenue}", label_share_net_receipts = f"{revenue}", vendor="HORUS", processed_day=today)
                    new_account.save()
                
                else:
                    print(f"This track record is missing from the drmsys ")


                    missing_data_raw_data['Adjustment Type'].append(f"{row['adjustment type']}")
                    missing_data_raw_data['Day'].append(f"{row['day']}")
                    missing_data_raw_data['Country'].append(f"{row['country']}")
                    missing_data_raw_data['Asset ID'].append(f"{row['asset id']}")
                    missing_data_raw_data['Asset Title'].append(f"{row['asset title']}")
                    missing_data_raw_data['Asset Labels'].append(f"{row['asset labels']}")
                    missing_data_raw_data['Asset Channel ID'].append(f"{row['asset channel id']}")
                    missing_data_raw_data['Asset Type'].append(f"{row['asset type']}")
                    missing_data_raw_data['Custom ID'].append(f"{row['custom id']}")
                    missing_data_raw_data['ISRC'].append(f"{row['isrc']}")
                    missing_data_raw_data['UPC'].append(f"{row['upc']}")
                    missing_data_raw_data['GRid'].append(f"{row['grid']}")
                    missing_data_raw_data['Artist'].append(f"{row['artist']}")
                    missing_data_raw_data['Album'].append(f"{row['album']}")
                    missing_data_raw_data['Label'].append(f"{row['label']}")
                    missing_data_raw_data['Administer Publish Rights'].append(f"{row['administer publish rights']}")
                    missing_data_raw_data['Owned Views'].append(f"{row['owned views']}")
                    missing_data_raw_data['YouTube Revenue Split : Auction'].append(f"{row['youtube revenue split : auction']}")
                    missing_data_raw_data['YouTube Revenue Split : Reserved'].append(f"{row['youtube revenue split : reserved']}")
                    missing_data_raw_data['YouTube Revenue Split : Partner Sold YouTube Served'].append(f"{row['youtube revenue split : partner sold youtube served']}")
                    missing_data_raw_data['YouTube Revenue Split : Partner Sold Partner Served'].append(f"{row['youtube revenue split : partner sold partner served']}")
                    missing_data_raw_data['YouTube Revenue Split'].append(f"{row['youtube revenue split']}")
                    missing_data_raw_data['Partner Revenue : Auction'].append(f"{row['partner revenue : auction']}")
                    missing_data_raw_data['Partner Revenue : Reserved'].append(f"{row['partner revenue : reserved']}")
                    missing_data_raw_data['Partner Revenue : Partner Sold YouTube Served'].append(f"{row['partner revenue : partner sold youtube served']}")
                    missing_data_raw_data['Partner Revenue : Partner Sold Partner Served'].append(f"{row['partner revenue : partner sold partner served']}")
                    missing_data_raw_data['Partner Revenue'].append(f"{row['partner revenue']}")
                    

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
            

            print(len(missing_data_raw_data))
            print(missing_data_raw_data)
            if len(missing_data_raw_data) != 0:
                new_missing_data_df = pd.DataFrame({key:pd.Series(value) for key, value in missing_data_raw_data.items() }, columns=missing_data_col)
                new_missing_data_file_name  = f"new_missing_data_{today_date}.csv"
                new_missing_data = new_missing_data_df.to_csv(new_missing_data_file_name, index = False, header=True)
                new_missing_data_doc = MissingRecordsDoc()
                new_missing_data_doc.document = horus_file
                new_missing_data_doc.file_name = new_missing_data_file_name
                new_missing_data_doc.save()

                with open(new_missing_data_file_name, 'rb') as csv:
                    new_missing_data_doc.file_doc.save( new_missing_data_file_name, File(csv))

            
                the_missing_file = MissingRecordsDoc.objects.filter(document=horus_file).last()

                subject, from_email, to = 'Missing Data in DRMSYS', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
                
                html_content = render_to_string('mail/missing_data.html', {'missing_data':new_missing_data_doc.file_doc.url})
                msg = EmailMessage(subject, html_content, from_email, to)
                msg.content_subtype = "html"
                the_file = the_missing_file.file_doc 
                the_filename = str(the_missing_file.file_name)
                response = requests.get(the_file.url)
                msg.attach(the_filename, response.content, mimetype="text/csv")
                msg.send() 


            get_track_doc = ProcessedTrackFile.objects.filter(document=horus_file).last()
            get_album_doc = ProcessedAlbumFile.objects.filter(document=horus_file).last()
            get_account_doc = ProcessedAccountFile.objects.filter(document=horus_file).last()

            the_track_doc_file = get_track_doc.file_doc 
            the_track_doc_filename = str(get_track_doc.file_name)
            track_response = requests.get(the_track_doc_file.url)

            the_album_doc_file = get_album_doc.file_doc 
            the_album_doc_filename = str(get_album_doc.file_name)
            album_response = requests.get(the_album_doc_file.url)

            the_account_doc_file = get_account_doc.file_doc 
            the_account_doc_filename = str(get_account_doc.file_name)
            account_response = requests.get(the_account_doc_file.url)

            subject, from_email, to = 'File Processed', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
            
            html_content = render_to_string('mail/processed.html', {'file_name':horus_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            # the_file = the_missing_file.file_doc 
            # the_filename = str(the_missing_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_track_doc_filename, track_response.content, mimetype="text/csv")
            msg.attach(the_album_doc_filename, album_response.content, mimetype="text/csv")
            msg.attach(the_account_doc_filename, account_response.content, mimetype="text/csv")
            msg.send()

           

    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':horus_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

       
    except:
        print("Unexpected error!")
        raise





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
            from_email = 'Trackeet Support <hello@pipminds.com>'
            
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
        missing_data_col = [ 'PERIOD', 'W OR P', 'PARTICIPANT NAME', 'PARTICIPANT #', 'IP #', 'TITLE NAME', 'TITLE #', 'PERF SOURCE', 'COUNTRY OF PERFORMANCE', 'SHOW NAME', 'EPISODE NAME', 'SHOW #', 'USE CODE', 'TIMING', 'PARTICIPANT %', 'PERF COUNT', 'BONUS LEVEL', 'ROYALTY AMOUNT', 'WITHHOLD', 'PERF PERIOD', 'CURRENT ACTIVITY AMT', 'HITS SONG OR TV NET SUPER USAGE BONUS', 'STANDARDS OR TV NET THEME BONUS', 'FOREIGN SOCIETY ADJUSTMENT', 'COMPANY CODE', 'COMPANY NAME' ]
        missing_data_raw_data = {
            'PERIOD':[], 'W OR P':[], 'PARTICIPANT NAME':[], 'PARTICIPANT #':[], 'IP #':[], 'TITLE NAME':[], 'TITLE #':[], 'PERF SOURCE':[], 'COUNTRY OF PERFORMANCE':[], 'SHOW NAME':[], 'EPISODE NAME':[], 'SHOW #':[], 'USE CODE':[], 'TIMING':[], 'PARTICIPANT %':[], 'PERF COUNT':[], 'BONUS LEVEL':[], 'ROYALTY AMOUNT':[], 'WITHHOLD':[], 'PERF PERIOD':[], 'CURRENT ACTIVITY AMT':[], 'HITS SONG OR TV NET SUPER USAGE BONUS':[], 'STANDARDS OR TV NET THEME BONUS':[], 'FOREIGN SOCIETY ADJUSTMENT':[], 'COMPANY CODE':[], 'COMPANY NAME':[]
        }
      


        for index, row in bmi.iterrows():

            if  {'title #', 'ip #', 'period'}.issubset(bmi.columns):

                #check_master = Master_DRMSYS.objects.filter(isrc=row['title #']).first()
                check_record = Master_DRMSYS.objects.filter(Q(isrc=row['title #'], data_type='Track')| Q(display_upc=row['ip #'], data_type='Track'))
                if check_record:
                    check_label = VerifiedLabels.objects.filter(label_name__contains=str(row['imprint label']).upper())
                    if check_label.exists():

                        track_raw_data['ISRC'].append(str(row['title #']))
                        track_raw_data['DisplayUPC'].append(str(row['ip #']))
                        #TRACK CHECKS 
                        #Process Track 
                        check_track_master = Master_DRMSYS.objects.filter(isrc=str(row['title #']), data_type='Track').first()
                        if check_track_master:
                            print("PPL CHECK TRACK MASTER EXISTS")

                        if check_track_master and check_track_master.track_duration is not None:
                            track_raw_data['TrackDuration'].append(check_track_master.track_duration)
                        else:
                            track_raw_data['TrackDuration'].append("None")
                        
                        if check_track_master and check_track_master.download is not None:
                            track_raw_data['Download'].append(check_track_master.download)
                        else:
                            track_raw_data['Download'].append("None")
                        
                        if check_track_master and check_track_master.sales_start_date is not None:
                            track_raw_data['SalesStartDate'].append(check_track_master.sales_start_date)
                        else:
                            track_raw_data['SalesStartDate'].append(str(row['period']))
                        
                        if check_track_master and check_track_master.sales_end_date is not None:
                            track_raw_data['SalesEndDate'].append(check_track_master.sales_end_date)
                        else:
                            track_raw_data['SalesEndDate'].append(str(row['period']))
                        
                        if check_track_master and check_track_master.error is not None:
                            track_raw_data['Error'].append(check_track_master.error)
                        else:
                            track_raw_data['Error'].append("None")

                        if check_track_master and check_track_master.parental_advisory is not None:
                            track_raw_data['ParentalAdvisory'].append(check_track_master.parental_advisory)
                        else:
                            track_raw_data['ParentalAdvisory'].append("None")

                        if check_track_master and check_track_master.preview_clip_start_time is not None:
                            track_raw_data['PreviewClipStartTime'].append(check_track_master.preview_clip_start_time)
                        else:
                            track_raw_data['PreviewClipStartTime'].append("None")

                        if check_track_master and check_track_master.preview_clip_duration is not None:
                            track_raw_data['PreviewClipDuration'].append(check_track_master.preview_clip_duration)
                        else:
                            track_raw_data['PreviewClipDuration'].append("None")

                        if check_track_master and check_track_master.song_version is not None:
                            track_raw_data['SongVersion'].append(check_track_master.song_version)
                        else:
                            track_raw_data['SongVersion'].append("None")

                        if check_track_master and check_track_master.p_line is not None:
                            track_raw_data['P_LINE'].append(check_track_master.p_line)
                        else:
                            track_raw_data['P_LINE'].append("None")
                        
                        if check_track_master and check_track_master.track_no is not None:
                            track_raw_data['TrackNo'].append(check_track_master.track_no)
                        else:
                            track_raw_data['TrackNo'].append("None")

                        if check_track_master and check_track_master.hidden_track is not None:
                            track_raw_data['HiddenTrack'].append(check_track_master.hidden_track)
                        else:
                            track_raw_data['HiddenTrack'].append("None")

                        if check_track_master and check_track_master.artist is not None:
                            track_raw_data['Artist'].append(check_track_master.artist)
                        else:
                            track_raw_data['Artist'].append(str(row['show name']))
                        
                        if check_track_master and check_track_master.title is not None:
                            track_raw_data['Title'].append(check_track_master.title)
                        else:
                            track_raw_data['Title'].append(str(row['show name']))

                        if check_track_master and check_track_master.release_name is not None:
                            track_raw_data['Release Name'].append(check_track_master.release_name)
                        else:
                            track_raw_data['Release Name'].append(row['show name'])

                        if check_track_master and  check_track_master.recording_artist is not None:
                            track_raw_data['Recording Artist'].append(check_track_master.recording_artist)
                        else:
                            track_raw_data['Recording Artist'].append(row['title name'])

                        if check_track_master and check_track_master.label_name is not None:
                            track_raw_data['LabelName'].append(check_track_master.label_name)
                        else:
                            track_raw_data['LabelName'].append(row['perf source'])

                        if check_track_master and check_track_master.volume_no is not None:
                            track_raw_data['VolumeNo'].append(check_track_master.volume_no)
                        else:
                            track_raw_data['VolumeNo'].append(row['perf count'])
                        
                        if check_track_master and check_track_master.genre is not None:
                            track_raw_data['Genre'].append(check_track_master.genre)
                        else:
                            track_raw_data['Genre'].append("None")

                        if check_track_master and check_track_master.publisher is not None:
                            track_raw_data['Publisher'].append(check_track_master.publisher)
                        else:
                            track_raw_data['Publisher'].append("None")

                        if check_track_master and check_track_master.producer is not None:
                            track_raw_data['Producer'].append(check_track_master.producer)
                        else:
                            track_raw_data['Producer'].append("None")

                        if check_track_master and check_track_master.writer is not None:
                            track_raw_data['Writer'].append(check_track_master.writer)
                        else:
                            track_raw_data['Writer'].append("None")
                        
                        if check_track_master and check_track_master.arranger is not None:
                            track_raw_data['Arranger'].append(check_track_master.arranger)
                        else:
                            track_raw_data['Arranger'].append("None")

                        if check_track_master and check_track_master.territories is not None:
                            track_raw_data['Territories'].append(check_track_master.territories)
                        else:
                            track_raw_data['Territories'].append(row['country of performance'])
                        
                        if check_track_master and check_track_master.exclusive is not None:
                            track_raw_data['Exclusive'].append(check_track_master.exclusive)
                        else:
                            track_raw_data['Exclusive'].append("None")

                        if check_track_master and check_track_master.wholesale_price is not None:
                            track_raw_data['WholesalePrice'].append(check_track_master.wholesale_price)
                        else:
                            track_raw_data['WholesalePrice'].append("None")

                        

                        #ALBUM CHECKS
                        #Process Album 

                        album_raw_data['Product UPC'].append(str(row['ip #']))

                        check_album_master = Master_DRMSYS.objects.filter(display_upc=str(row['ip #']), data_type="Album").first()
                        if check_album_master:
                            print("PPL CHECK MASTER ALBUM EXISTS")

                        if check_album_master and check_album_master.c_line is not None:
                            album_raw_data['CLINE'].append(check_album_master.c_line)
                        else:
                            album_raw_data['CLINE'].append("None")

                        if check_album_master and check_album_master.master_carveouts is not None:
                            album_raw_data['Master Carveouts'].append(check_album_master.master_carveouts)
                        else:
                            album_raw_data['Master Carveouts'].append("None")

                        if check_album_master and check_album_master.territories_carveouts is not None:
                            album_raw_data['Territories Carveouts'].append(check_album_master.territories_carveouts)
                        else:
                            album_raw_data['Territories Carveouts'].append("None")

                        if check_album_master and check_album_master.deleted is not None:
                            album_raw_data['Deleted?'].append(check_album_master.deleted)
                        else:
                            album_raw_data['Deleted?'].append("None")

                        if check_album_master and check_album_master.manufacturer_upc is not None:
                            album_raw_data['Manufacturer UPC'].append(check_album_master.manufacturer_upc)
                        else:
                            album_raw_data['Manufacturer UPC'].append("None")

                        if check_album_master and check_album_master.release_date is not None:
                            album_raw_data['Release Date'].append(check_album_master.release_date)
                        else:
                            album_raw_data['Release Date'].append("None")

                        if check_album_master and check_album_master.artist_url is not None:
                            album_raw_data['Artist URL'].append(check_album_master.artist_url)
                        else:
                            album_raw_data['Artist URL'].append("None")

                        if check_album_master and check_album_master.label_catalogno is not None:
                            album_raw_data['Catalog Number'].append(check_album_master.label_catalogno)
                        else:
                            album_raw_data['Catalog Number'].append(row['None'])


                        if check_album_master and check_album_master.total_volumes is not None:
                            album_raw_data['Total Volumes'].append(check_album_master.total_volumes)
                        else:
                            album_raw_data['Total Volumes'].append(row['perf count'])

                        if check_album_master and check_album_master.total_tracks is not None:
                            album_raw_data['Total Tracks'].append(check_album_master.total_tracks)
                        else:
                            album_raw_data['Total Tracks'].append(row['perf count'])

                        

                        if check_album_master and check_album_master.label_name is not None:
                            album_raw_data['Imprint Label'].append(check_album_master.label_name)
                        else:
                            album_raw_data['Imprint Label'].append(row['perf source'])

                        if check_album_master and check_album_master.genre is not None:
                            album_raw_data['Genre'].append(check_album_master.genre)
                        else:
                            album_raw_data['Genre'].append("None")

                        if check_album_master and check_album_master.title is not None:
                            album_raw_data['Product Title'].append(check_album_master.title)
                        else:
                            album_raw_data['Product Title'].append(row['show name'])
                        
                        if check_album_master and check_album_master.artist is not None:
                            album_raw_data['Product Artist'].append(check_album_master.artist)
                        else:
                            album_raw_data['Product Artist'].append(row['show name'])



                    
                        account_raw_data['Period'].append(row['period'])
                        account_raw_data['Activity Period'].append(row['perf period'])
                        account_raw_data['Retailer'].append(row['perf source'])
                        account_raw_data['Product UPC'].append(str(row['ip #']))
                        account_raw_data['Territory'].append(row['country of performance'])
                        account_raw_data['Project Code'].append(row['show name'])
                        account_raw_data['Product Code'].append(row['episode name'])

                        
                        account_raw_data['Product Name'].append(row['title name'])
                        account_raw_data['ISRC'].append(str(row['title #']))
                        account_raw_data['Volume'].append(row['perf count'])
                        account_raw_data['Unit Price'].append(row['current activity amt'])
                        account_raw_data['Discount'].append(row['withhold'])
                        account_raw_data['Price'].append(str(row['current activity amt']))
                        account_raw_data['Quantity'].append(row['perf count'])
                        account_raw_data['Total'].append(str(row['royalty amount']))
                        account_raw_data['Label Share Net Receipts'].append(str(row['royalty amount']))

                        new_account, created = Accounting.objects.get_or_create( period = row['period'], activity_period = row['perf period'], retailer = row['perf source'], territory = row['country of performance'], orchard_UPC = row['ip #'], manufacturer_UPC = row['ip #'], project_code = row['show name'], product_code = row['episode name'],  product_name = row['title name'], track_name = row['title name'], isrc = row['title #'], volume = row['perf count'], unit_price = row['current activity amt'], discount = row['withhold'], actual_price = row['current activity amt'], quantity = row['perf count'], total = row['royalty amount'], label_share_net_receipts = row['royalty amount'], vendor="BMI", processed_day=today)
                    else:
                        print("wrong label name issues")
                else:
                    print('missing records!')
                    missing_data_raw_data['PERIOD'].append(f"{row['period']}")
                    missing_data_raw_data['W OR P'].append(f"{row['w or p']}")
                    missing_data_raw_data['PARTICIPANT NAME'].append(f"{row['participant name']}")
                    missing_data_raw_data['PARTICIPANT #'].append(f"{row['participant #']}")
                    missing_data_raw_data['IP #'].append(f"{row['ip #']}")
                    missing_data_raw_data['TITLE NAME'].append(f"{row['title name']}")
                    missing_data_raw_data['TITLE #'].append(f"{row['title #']}")
                    missing_data_raw_data['PERF SOURCE'].append(f"{row['perf source']}")
                    missing_data_raw_data['COUNTRY OF PERFORMANCE'].append(f"{row['country of performance']}")
                    missing_data_raw_data['SHOW NAME'].append(f"{row['show name']}")
                    missing_data_raw_data['EPISODE NAME'].append(f"{row['episode name']}")
                    missing_data_raw_data['SHOW #'].append(f"{row['show #']}")
                    missing_data_raw_data['USE CODE'].append(f"{row['use code']}")
                    missing_data_raw_data['TIMING'].append(f"{row['timing']}")
                    missing_data_raw_data['PARTICIPANT %'].append(f"{row['participant %']}")
                    missing_data_raw_data['PERF COUNT'].append(f"{row['perf count']}")
                    missing_data_raw_data['BONUS LEVEL'].append(f"{row['bonus level']}")
                    missing_data_raw_data['ROYALTY AMOUNT'].append(f"{row['royalty amount']}")
                    missing_data_raw_data['WITHHOLD'].append(f"{row['withhold']}")
                    missing_data_raw_data['PERF PERIOD'].append(f"{row['perf period']}")
                    missing_data_raw_data['CURRENT ACTIVITY AMT'].append(f"{row['current activity amt']}")
                    missing_data_raw_data['HITS SONG OR TV NET SUPER USAGE BONUS'].append(f"{row['hits song or tv net super usage bonus']}")
                    missing_data_raw_data['STANDARDS OR TV NET THEME BONUS'].append(f"{row['standards or tv net theme bonus']}")
                    missing_data_raw_data['FOREIGN SOCIETY ADJUSTMENT'].append(f"{row['foreign society adjustment']}")
                    missing_data_raw_data['COMPANY CODE'].append(f"{row['company code']}")
                    missing_data_raw_data['COMPANY NAME'].append(f"{row['company name']}")
                    
                    
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


        print(len(missing_data_raw_data))
        print(missing_data_raw_data)
        if len(missing_data_raw_data) != 0:
            new_missing_data_df = pd.DataFrame({key:pd.Series(value) for key, value in missing_data_raw_data.items() }, columns=missing_data_col)
            new_missing_data_file_name  = f"new_missing_data_{today_date}.csv"
            new_missing_data = new_missing_data_df.to_csv(new_missing_data_file_name, index = False, header=True)
            new_missing_data_doc = MissingRecordsDoc()
            new_missing_data_doc.document = bmi_file
            new_missing_data_doc.file_name = new_missing_data_file_name
            new_missing_data_doc.save()

            with open(new_missing_data_file_name, 'rb') as csv:
                new_missing_data_doc.file_doc.save( new_missing_data_file_name, File(csv))

            the_missing_file = MissingRecordsDoc.objects.filter(document=bmi_file).last()

            subject, from_email, to = 'Missing Data in DRMSYS', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']

            html_content = render_to_string('mail/missing_data.html', {'missing_data':new_missing_data_doc.file_doc.url})
            msg = EmailMessage(subject, html_content, from_email, to)
            msg.content_subtype = "html"
            the_file = the_missing_file.file_doc 
            the_filename = str(the_missing_file.file_name)
            response = requests.get(the_file.url)
            msg.attach(the_filename, response.content, mimetype="text/csv")
            msg.send()

        get_track_doc = ProcessedTrackFile.objects.filter(document=bmi_file).last()
        get_album_doc = ProcessedAlbumFile.objects.filter(document=bmi_file).last()
        get_account_doc = ProcessedAccountFile.objects.filter(document=bmi_file).last()

        the_track_doc_file = get_track_doc.file_doc 
        the_track_doc_filename = str(get_track_doc.file_name)
        track_response = requests.get(the_track_doc_file.url)

        the_album_doc_file = get_album_doc.file_doc 
        the_album_doc_filename = str(get_album_doc.file_name)
        album_response = requests.get(the_album_doc_file.url)

        the_account_doc_file = get_account_doc.file_doc 
        the_account_doc_filename = str(get_account_doc.file_name)
        account_response = requests.get(the_account_doc_file.url)

        subject, from_email, to = 'File Processed', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']
            
        html_content = render_to_string('mail/processed.html', {'file_name':bmi_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url, 'account_download': new_account_document.file_doc.url })
        msg = EmailMessage(subject, html_content, from_email, to)
        msg.content_subtype = "html"
        # the_file = the_missing_file.file_doc 
        # the_filename = str(the_missing_file.file_name)
        response = requests.get(the_file.url)
        msg.attach(the_track_doc_filename, track_response.content, mimetype="text/csv")
        msg.attach(the_album_doc_filename, album_response.content, mimetype="text/csv")
        msg.attach(the_account_doc_filename, account_response.content, mimetype="text/csv")
        msg.send()



        
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':bmi_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)

    except:
        print("Unexpected error!")
        raise






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
            from_email = 'Trackeet Support <hello@pipminds.com>'
            
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
                        print("updating display upc")
                        check_track.display_upc = dummy_upc
                    elif check_track.display_upc and track_raw_data['DisplayUPC'] == '':
                        track_raw_data['DisplayUPC'].append(dummy_upc)
                    else:
                        pass
                else:
                    print('digital upc  not here')
                if 'isrc' in orchard.columns:
                    if not check_track.isrc:
                        print("updating isrc ")
                        check_track.isrc = dummy_isrc
                    elif check_track.isrc and track_raw_data['ISRC'] == '':
                        track_raw_data['ISRC'].append(dummy_isrc)
                    else:
                        pass
                else:
                    print('isrc  not here')
                #####
                if 'orchard artist' in orchard.columns:
                    if not check_track.artist or check_track.artist is not row['orchard artist']:
                        print("updating track artist")
                        check_track.artist = row['orchard artist']
                    elif check_track.artist and track_raw_data['Artist'] == '':
                        track_raw_data['Artist'].append(check_track.artist)
                    else:
                        pass
                    #######
                    if not check_track.recording_artist or check_track.recording_artist is not row['orchard artist']:
                        print(f"updating recording artist ")
                        check_track.recording_artist = row['orchard artist']
                    elif check_track.recording_artist and  track_raw_data['Recording Artist'] == '':
                         track_raw_data['Recording Artist'].append(check_track.recording_artist)
                    else:
                        pass
                else:
                    print('artist_name not here')
                ######   
                if 'publisher(s)' in orchard.columns:
                    if not check_track.publisher or check_track.publisher is not row['publisher(s)']:
                        print("updating publisher ")
                        check_track.publisher = row['publisher(s)']
                    elif check_track.publisher and track_raw_data['Publisher'] == '':
                        track_raw_data['Publisher'].append(check_track.publisher)
                    else:
                        pass
                else:
                    print('publisher not here')
                #####
                if 'imprint' in orchard.columns:
                    if not check_track.label_name or check_track.label_name is not row['imprint']:
                        print("updating label ")
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
                            print("updating display upc ")
                            check_track_album.product_upc = dummy_upc
                        elif check_track_album.product_upc and album_raw_data['Product UPC'] == '':
                            album_raw_data['Product UPC'].append(dummy_upc)
                        else:
                            pass
                    #####
                    else:
                        print('digital upc  not here')
                    if 'orchard artist' in orchard.columns:
                        if not check_track_album.product_artist or check_track_album.product_artist is not row['orchard artist']:
                            print("updating product artist")
                            check_track_album.product_artist = row['orchard artist']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=dummy_isrc, display_upc= dummy_upc, title=row['track name'], recording_artist=row['orchard artist'] ,artist =row['orchard artist'] , release_name=row['release name'], label_name=row['imprint'], publisher=row['publisher(s)'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                #####     
                    print("track album does not exists, creating track album and track now! ")
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
        ###
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
        ####
        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))
        #####
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
        #####
        with open(album_file_name, 'rb') as csv:
            new_album_document.file_doc.save( album_file_name, File(csv))
        ###
        subject = 'File Processed'
        html_message = render_to_string('mail/processed.html', {'file_name':orchard_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)
    #####
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':orchard_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com', 'digger@wyldpytch.com'], html_message=html_message)
    ####
    except:
        print("Unexpected error!")
        raise
###############################
# PROCESS MASTER DRMSYS IMPORT# 
################################
@shared_task
def process_drmsys_import(pk):
    drmsys_file = ProcessedDocument.objects.get(pk=pk)
    try:
        drmsys = pd.read_excel(drmsys_file.file_doc, Index=None).fillna('')
        drmsys.columns = drmsys.columns.str.strip().str.lower()
        print(drmsys.columns)

        if not {'isrc', 'displayupc'}.issubset(drmsys.columns):

            print("isrc and upc not present")
            subject = 'Corrupted File'
            html_message = render_to_string('mail/corrupt.html', {'file_name':drmsys_file.file_name})
            plain_message = strip_tags(html_message)
            from_email = 'Trackeet Support <hello@pipminds.com>'
            send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)
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
        for index, row in drmsys.iterrows():
            if row['datatype'] == 'Album':
                print('this is an album')
                check_album_master = Master_DRMSYS.objects.filter(display_upc=row['displayupc'], data_type="Album").first()
                if check_album_master:
                    print(f"Album {row['displayupc']} already exists")
                else:
                    new_album, created = Master_DRMSYS.objects.get_or_create(display_upc=str(row['displayupc']), 
                    data_type="Album", artist = row['artist'], 
                    isrc=str(row['isrc']), label_name=row['labelname'], 
                    publisher=row['publisher'], recording_artist=row['recordingartist'], 
                    release_name=row['releasename'], title=row['title'], 
                    artist_url=row['artisturl'], bpm=row['bpm'], 
                    c_line=row['c_line'] , deleted=row["deleted?"], 
                    download=row["download"], error=row['error'],
                    exclusive=row["exclusive"], genre=row["genre"], 
                    genre_alt=row['genrealt'] ,genre_sub=row['genresub'], 
                    genre_subalt=row['genresubalt'], hidden_track=row['hiddentrack'], 
                    itunes_link=row['ituneslink'],label_catalogno=row['labelcatalogno'],
                    language=row['language'], manufacturer_upc=row['manufacturerupc'],
                    master_carveouts = row['mastercarveouts'], p_line=row['p_line'],
                    parental_advisory=row['parentaladvisory'], preview_clip_duration=row['previewclipduration'],
                    preview_clip_start_time=row['previewclipstarttime'], price_band=row['priceband'],
                    producer=row['producer'], release_date=row['releasedate'],
                    song_version=row['songversion'], royalty_rate=row['royaltyrate'],
                    sales_end_date=row['salesenddate'], sales_start_date=row['salesstartdate'],
                    territories=row['territories'], territories_carveouts=row['territoriescarveouts'],
                    total_tracks=row['totaltracks'], total_volumes=row['totalvolumes'],
                    track_duration=row['trackduration'], track_no=row['trackno'],
                    vendor_name=row['vendorname'], volume_no=row['volumeno'], 
                    wholesale_price=row['wholesaleprice'], writer=row['writer'],
                    zIDKey_UPCISRC=row['zidkey_upcisrc'],processed_day=today )
                    ####
                    new_album_record, created = Album.objects.get_or_create(
                        product_title = row['title'],
                        product_upc = str(row['displayupc']),
                        genre= row["genre"],
                        release_date =row['releasedate'] ,
                        total_volume = row['totalvolumes'],
                        imprint_label = row['labelname'],
                        product_artist = row['artist'],
                        artist_url = row['artisturl'], 
                        catalog_number = row['labelcatalogno'],
                        manufacture_upc = row['manufacturerupc'],
                        deleted = row["deleted?"],
                        total_tracks = row['totaltracks'] ,
                        cline= row['c_line'],
                        territories_carveouts = row['territoriescarveouts'],
                        master_carveouts= row['mastercarveouts'],
                        processed_day= today
                    )
                #####
                album_raw_data['Product UPC'].append(f"{row['displayupc']}")
                album_raw_data['CLINE'].append(f"{row['c_line']}")
                album_raw_data['Master Carveouts'].append(f"{row['mastercarveouts']}")
                album_raw_data['Territories Carveouts'].append(f"{row['territoriescarveouts']}")
                album_raw_data['Deleted?'].append(f"{row['deleted?']}")
                album_raw_data['Manufacturer UPC'].append(f"{row['manufacturerupc']}")
                album_raw_data['Release Date'].append(f"{row['releasedate']}")
                album_raw_data['Artist URL'].append(f"{row['artisturl']}")
                album_raw_data['Catalog Number'].append(f"{row['labelcatalogno']}")
                album_raw_data['Total Volumes'].append(f"{row['totalvolumes']}")
                album_raw_data['Total Tracks'].append(f"{row['totaltracks']}")
                album_raw_data['Imprint Label'].append(f"{row['labelname']}")
                album_raw_data['Genre'].append(f"{row['genre']}")
                album_raw_data['Product Title'].append(f"{row['title']}")
                album_raw_data['Product Artist'].append(f"{row['artist']}")
            #####
            elif row['datatype'] == 'Track':
                print('this is a track')
                check_track_master = Master_DRMSYS.objects.filter(isrc=row['isrc'], data_type='Track').first()
                if check_track_master:
                    print(f"track record {row['isrc']} already exists ")
                else:
                    new_track, created = Master_DRMSYS.objects.get_or_create(display_upc=str(row['displayupc']), 
                    data_type="Track", artist = row['artist'], 
                    isrc=str(row['isrc']), label_name=row['labelname'], 
                    publisher=row['publisher'], recording_artist=row['recordingartist'], 
                    release_name=row['releasename'], title=row['title'], 
                    artist_url=row['artisturl'], bpm=row['bpm'], 
                    c_line=row['c_line'] , deleted=row["deleted?"], 
                    download=row["download"], error=row['error'],
                    exclusive=row["exclusive"], genre=row["genre"], 
                    genre_alt=row['genrealt'] ,genre_sub=row['genresub'], 
                    genre_subalt=row['genresubalt'], hidden_track=row['hiddentrack'], 
                    itunes_link=row['ituneslink'],label_catalogno=row['labelcatalogno'],
                    language=row['language'], manufacturer_upc=row['manufacturerupc'],
                    master_carveouts = row['mastercarveouts'], p_line=row['p_line'],
                    parental_advisory=row['parentaladvisory'], preview_clip_duration=row['previewclipduration'],
                    preview_clip_start_time=row['previewclipstarttime'], price_band=row['priceband'],
                    producer=row['producer'], release_date=row['releasedate'],
                    song_version=row['songversion'], royalty_rate=row['royaltyrate'],
                    sales_end_date=row['salesenddate'], sales_start_date=row['salesstartdate'],
                    territories=row['territories'], territories_carveouts=row['territoriescarveouts'],
                    total_tracks=row['totaltracks'], total_volumes=row['totalvolumes'],
                    track_duration=row['trackduration'], track_no=row['trackno'],
                    vendor_name=row['vendorname'], volume_no=row['volumeno'], 
                    wholesale_price=row['wholesaleprice'], writer=row['writer'],
                    zIDKey_UPCISRC=row['zidkey_upcisrc'],processed_day=today )


                    new_track_record, created = Track.objects.get_or_create( display_upc =str(row['displayupc']),
                        volume_no = row['volumeno'],track_no = row['trackno'],
                        hidden_track = row['hiddentrack'],title = row['title'], 
                        song_version = row['songversion'], genre = row["genre"], 
                        isrc = str(row['isrc']), track_duration = row['trackduration'], 
                        preview_clip_start_time = row['previewclipstarttime'], preview_clip_duration = row['previewclipduration'], 
                        p_line = row['p_line'], recording_artist = row['recordingartist'], 
                        artist = row['artist'], release_name = row['releasename'], 
                        parental_advisory = row['parentaladvisory'], label_name = row['labelname'], 
                        producer = row['producer'], publisher = row['publisher'], 
                        writer = row['writer'], arranger = row['arranger'], 
                        territories =  row['territories'], exclusive = row["exclusive"] , 
                        wholesale_price = row['wholesaleprice'], download = row["download"], 
                        sales_start_date =  row['salesstartdate'],  sales_end_date = row['salesenddate'], 
                        error = row['error'], processed_day = today
                    )

                track_raw_data['ISRC'].append(f"{row['isrc']}")
                track_raw_data['DisplayUPC'].append(f"{row['displayupc']}")
                track_raw_data['TrackDuration'].append(f"{row['trackduration']}")
                track_raw_data['Download'].append(f"{row['download']}")
                track_raw_data['SalesStartDate'].append(f"{row['salesstartdate']}")
                track_raw_data['SalesEndDate'].append(f"{row['salesenddate']}")
                track_raw_data['Error'].append(f"{row['error']}")
                track_raw_data['ParentalAdvisory'].append(f"{row['parentaladvisory']}")
                track_raw_data['PreviewClipStartTime'].append(f"{row['previewclipstarttime']}")
                track_raw_data['PreviewClipDuration'].append(f"{row['previewclipduration']}")
                track_raw_data['SongVersion'].append(f"{row['songversion']}")
                track_raw_data['P_LINE'].append(f"{row['p_line']}")
                track_raw_data['TrackNo'].append(f"{row['trackno']}")
                track_raw_data['HiddenTrack'].append(f"{row['hiddentrack']}")
                track_raw_data['Artist'].append(f"{row['artist']}")
                track_raw_data['Title'].append(f"{row['title']}")
                track_raw_data['Release Name'].append(f"{row['releasename']}")
                track_raw_data['Recording Artist'].append(f"{row['recordingartist']}")
                track_raw_data['LabelName'].append(f"{row['labelname']}")
                track_raw_data['VolumeNo'].append(f"{row['volumeno']}")
                track_raw_data['Genre'].append(f"{row['genre']}")
                track_raw_data['Publisher'].append(f"{row['publisher']}")
                track_raw_data['Producer'].append(f"{row['producer']}")
                track_raw_data['Writer'].append(f"{row['writer']}")
                track_raw_data['Arranger'].append(f"{row['arranger']}")
                track_raw_data['Territories'].append(f"{row['territories']}")
                track_raw_data['Exclusive'].append(f"{row['exclusive']}")
                track_raw_data['WholesalePrice'].append(f"{row['wholesaleprice']}")

        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        #new_track_df = new_track_df.drop_duplicates(keep = False, inplace = False)
        track_file_name  = f"processed_drmsys_track_output_{today_date}.csv"
        track_file_name_txt  = f"processed_drmsys_track_output_{today_date}.txt"
        track_file = new_track_df.to_csv(track_file_name, index = False, header=True)
        track_file_txt = new_track_df.to_csv(track_file_name_txt, index = False, header=True, sep='\t')
        new_track_document = ProcessedTrackFile()
        new_track_document.document = drmsys_file
        new_track_document.file_name = track_file_name
        new_track_document.file_name_txt = track_file_name_txt
        new_track_document.save()

        with open(track_file_name, 'rb') as csv:
            new_track_document.file_doc.save(track_file_name, File(csv))

        with open(track_file_name_txt, 'rb') as txt:
            new_track_document.file_doc_txt.save(track_file_name_txt, File(txt))
        
        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        #new_album_df = new_album_df.drop_duplicates(keep = False, inplace = False)
        album_file_name  = f"processed_drmsys_album_output_{today_date}.csv"
        album_file_name_txt  = f"processed_drmsys_album_output_{today_date}.txt"
        album_file = new_album_df.to_csv(album_file_name, index = False, header=True)
        album_file_txt = new_album_df.to_csv(album_file_name_txt, index = False, header=True, sep='\t')
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = drmsys_file
        new_album_document.file_name = album_file_name
        new_album_document.file_name_txt = album_file_name_txt
        new_album_document.save()

        with open( album_file_name, 'rb') as csv:
            new_album_document.file_doc.save(album_file_name, File(csv))

        with open(album_file_name_txt, 'rb') as txt:
            new_album_document.file_doc_txt.save(album_file_name_txt, File(txt))


        get_track_doc = ProcessedTrackFile.objects.filter(document=drmsys_file).last()
        get_album_doc = ProcessedAlbumFile.objects.filter(document=drmsys_file).last()
      

        the_track_doc_file = get_track_doc.file_doc 
        the_track_doc_filename = str(get_track_doc.file_name)
        track_response = requests.get(the_track_doc_file.url)

        the_album_doc_file = get_album_doc.file_doc 
        the_album_doc_filename = str(get_album_doc.file_name)
        album_response = requests.get(the_album_doc_file.url) 
        

        subject, from_email, to = 'DRMSYS File Processed', 'Trackeet Support <hello@pipminds.com>', ['shola.albert@gmail.com']

        html_content = render_to_string('mail/processed.html', {'file_name':drmsys_file.file_name, 'track_download':new_track_document.file_doc.url, 'album_download':new_album_document.file_doc.url })
        msg = EmailMessage(subject, html_content, from_email, to)
        msg.content_subtype = "html"
        msg.attach(the_track_doc_filename, track_response.content, mimetype="text/csv")
        msg.attach(the_album_doc_filename, album_response.content, mimetype="text/csv")        
        msg.send() 

    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)

        subject = 'Error Processing File'
        html_message = render_to_string('mail/error.html', {'err_msg':err_msg, 'file_name':drmsys_file.file_name, })
        plain_message = strip_tags(html_message)
        from_email = 'Trackeet Support <hello@pipminds.com>'
        
        send_mail(subject, plain_message, from_email, ['shola.albert@gmail.com'], html_message=html_message)  
    except:
        print("Unexpected error!")
        raise
########################
# CLEAN UP SCRIPTS #####
########################
@shared_task
def run_clean_up_script():
    #check all master_drmsys records 
    all_records = Master_DRMSYS.objects .all()
    for record in all_records:
        print(record)
