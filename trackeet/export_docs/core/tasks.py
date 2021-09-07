from .models import *
import pandas as pd
from django.core.mail import send_mail
from datetime import date
from celery import shared_task
from django.core.files import File
import uuid 
import random, string

today = date.today()




def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

############################
#     ALBUM                 #
############################
@shared_task
def process_album(filename):
    album_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
        album = pd.read_excel(album_file.file_doc, usecols=col_list).fillna('')
        if 'Product UPC' not in album:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {album_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com', 'shola.albert@gmail.com'],
                            fail_silently=False,
                            )
        else:
            pass
        for index, row in album.iterrows():
            print(row['Product UPC'])
            check_album = Album.objects.filter(product_upc = row['Product UPC']).first()
            if check_album:
                check_album.product_title = row['Product Title']
                check_album.genre = row['Genre']
                check_album.release_date = row['Release Date']
                check_album.total_volume = row['Total Volumes']
                check_album.imprint_label = row['Imprint Label']
                check_album.product_artist = row['Product Artist']
                check_album.artist_url = row['Artist URL']
                check_album.catalog_number = row['Catalog Number']
                check_album.manufacture_upc = row['Manufacturer UPC']
                check_album.deleted = row['Deleted?']
                check_album.total_tracks = row['Total Tracks']
                check_album.cline = row['CLINE']
                check_album.territories_carveouts = row['Territories Carveouts']
                check_album.master_carveouts = row['Master Carveouts']
                check_album.save()
            else:
                new_album = Album(product_title = row['Product Title'], product_upc = row['Product UPC'], genre = row['Genre'], release_date = row['Release Date'], total_volume = row['Total Volumes'], imprint_label = row['Imprint Label'], product_artist = row['Product Artist'], artist_url = row['Artist URL'], catalog_number = row['Catalog Number'], manufacture_upc = row['Manufacturer UPC'], deleted = row['Deleted?'], total_tracks = row['Total Tracks'], cline = row['CLINE'], territories_carveouts = row['Territories Carveouts'], master_carveouts =row['Master Carveouts'], processed_day=today)
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
                        ['digger@wyldpytch.com', 'shola.albert@gmail.com'],
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
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise




############################
#     TRACK                 #
############################
@shared_task
def process_track(filename):
    track_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track = pd.read_excel(track_file.file_doc, usecols=col_list).fillna('')
        if 'ISRC' not in track:
            send_mail(
                        'Corrupted File',
                        f''' 
                        Hello Admin 
                        The file {track_file.file_name} was corrupt !
                        Pls login to run it again
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com', 'shola.albert@gmail.com'],
                        fail_silently=False,
                        )
        else:
            pass
        for index, row in track.iterrows():
            check_track = Track.objects.filter(isrc=row['ISRC']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists. updating now ")
                check_track.display_upc = row['DisplayUPC'],
                check_track.volume_no = row['VolumeNo'],
                check_track.track_no = row['TrackNo'],
                check_track.hidden_track = row['HiddenTrack'],
                check_track.title = row['Title'],
                check_track.song_version = row['SongVersion'],
                check_track.genre  = row['Genre'],
                check_track.track_duration  = row['TrackDuration'],
                check_track.preview_clip_start_time  = row['PreviewClipStartTime'],
                check_track.preview_clip_duration = row['PreviewClipDuration'],
                check_track.p_line = row['P_LINE'],
                check_track.recording_artist = row['Recording Artist'],
                check_track.artist  = row['Artist'],
                check_track.release_name = row['Release Name'],
                check_track.parental_advisory = row['ParentalAdvisory'],
                check_track.label_name = row['LabelName'],
                check_track.producer = row['Producer'],
                check_track.publisher = row['Publisher'],
                check_track.writer = row['Writer'],
                check_track.arranger = row['Arranger'],
                check_track.territories = row['Territories'],
                check_track.exclusive = row['Exclusive'],
                check_track.wholesale_price = row['WholesalePrice'],
                check_track.download = row['Download'],
                check_track.sales_start_date = row['SalesStartDate'],
                check_track.sales_end_date = row['SalesEndDate'],
                check_track.error = row['Error']
                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_upc= row['DisplayUPC']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    new_track = Track(album = check_track_album, display_upc = row['DisplayUPC'], volume_no = row['VolumeNo'], track_no = row['TrackNo'], hidden_track  = row['HiddenTrack'], title  = row['Title'],  song_version = row['SongVersion'], genre  = row['Genre'], isrc = row['ISRC'], track_duration = row['TrackDuration'], preview_clip_start_time  = row['PreviewClipStartTime'], preview_clip_duration  = row['PreviewClipDuration'], p_line = row['P_LINE'], recording_artist  = row['Recording Artist'], artist  = row['Artist'], release_name  = row['Release Name'], parental_advisory  = row['ParentalAdvisory'], label_name = row['LabelName'], producer  = row['Producer'], publisher  = row['Publisher'], writer  = row['Writer'], arranger  = row['Arranger'], territories  = row['Territories'], exclusive  = row['Exclusive'], wholesale_price  = row['WholesalePrice'], download  = row['Download'], sales_start_date  = row['SalesStartDate'], sales_end_date  = row['SalesEndDate'], error  = row['Error'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS(arranger = new_track.arranger, artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, download = new_track.download, error = new_track.error, exclusive = new_track.exclusive, genre = new_track.genre, hidden_track = new_track.hidden_track, isrc = new_track.isrc, label_name = new_track.label_name, p_line = new_track.p_line, parental_advisory = new_track.parental_advisory, preview_clip_duration = new_track.preview_clip_duration, preview_clip_start_time = new_track.preview_clip_start_time, producer = new_track.producer, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, sales_end_date = new_track.sales_end_date, sales_start_date = new_track.sales_start_date, song_version = new_track.song_version, territories = new_track.territories, title = new_track.title, track_duration = new_track.track_duration, track_no = new_track.track_no, volume_no = new_track.volume_no,  wholesale_price = new_track.wholesale_price,  writer = new_track.writer, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['DisplayUPC'], product_title = row['Title'] , genre = row['Genre'], product_artist = row['Artist'],  processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, display_upc = row['DisplayUPC'], volume_no = row['VolumeNo'], track_no = row['TrackNo'], hidden_track  = row['HiddenTrack'], title  = row['Title'],  song_version = row['SongVersion'], genre  = row['Genre'], isrc = row['ISRC'], track_duration = row['TrackDuration'], preview_clip_start_time  = row['PreviewClipStartTime'], preview_clip_duration  = row['PreviewClipDuration'], p_line = row['P_LINE'], recording_artist  = row['Recording Artist'], artist  = row['Artist'], release_name  = row['Release Name'], parental_advisory  = row['ParentalAdvisory'], label_name = row['LabelName'], producer  = row['Producer'], publisher  = row['Publisher'], writer  = row['Writer'], arranger  = row['Arranger'], territories  = row['Territories'], exclusive  = row['Exclusive'], wholesale_price  = row['WholesalePrice'], download  = row['Download'], sales_start_date  = row['SalesStartDate'], sales_end_date  = row['SalesEndDate'], error  = row['Error'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS(arranger = new_track.arranger, artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, download = new_track.download, error = new_track.error, exclusive = new_track.exclusive, genre = new_track.genre, hidden_track = new_track.hidden_track, isrc = new_track.isrc, label_name = new_track.label_name, p_line = new_track.p_line, parental_advisory = new_track.parental_advisory, preview_clip_duration = new_track.preview_clip_duration, preview_clip_start_time = new_track.preview_clip_start_time, producer = new_track.producer, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, sales_end_date = new_track.sales_end_date, sales_start_date = new_track.sales_start_date, song_version = new_track.song_version, territories = new_track.territories, title = new_track.title, track_duration = new_track.track_duration, track_no = new_track.track_no, volume_no = new_track.volume_no,  wholesale_price = new_track.wholesale_price,  writer = new_track.writer, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track,  processed_day=today)
                    new_master.save()
        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {track_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com', 'shola.albert@gmail.com'],
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
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise





############################
#     PPL                 #
############################
@shared_task
def process_ppl(filename):
    ppl_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = [ 'PPL_MEMBER_ID', 'MEMBER_NAME', 'ALLOCATION_PERIOD', 'RECORDING_CONTENT_TYPE', 'PPL_RECORDING_ID', 'RECORDING_ISRC', 'BAND_ARTIST_NAME', 'RECORDING_TITLE', 'COUNTRY_OF_RECORDING', 'RECORDING_P_DATE', 'RIGHTSHOLDING_PERCENTAGE', 'REVENUE_SOURCE_LEVEL_1', 'REVENUE_SOURCE_LEVEL_2', 'REVENUE_SOURCE_LEVEL_3', 'REVENUE_SOURCE_LEVEL_4', 'RECORDING_ALLOCATION_AMOUNT_GBP', 'NON_QUALIFYING_PERFORMER_ALLOCATION_AMOUNT_GBP', 'TOTAL_ALLOCATION_AMOUNT_GBP']
        ppl = pd.read_excel(ppl_file.file_doc, usecols=col_list).fillna('')
        if 'RECORDING_ISRC' not in ppl:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {ppl_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            [ 'digger@wyldpytch.com','shola.albert@gmail.com'],
                            fail_silently=False,
                            )
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

        for index, row in ppl.iterrows():
            track_raw_data['ISRC'].append(row['RECORDING_ISRC'])
            track_raw_data['DisplayUPC'].append(row['PPL_RECORDING_ID'])
            track_raw_data['Artist'].append(row['BAND_ARTIST_NAME'])
            track_raw_data['Title'].append(row['RECORDING_TITLE'])
            track_raw_data['Release Name'].append(row['RECORDING_TITLE'])
            track_raw_data['Recording Artist'].append(row['BAND_ARTIST_NAME'])
            track_raw_data['Publisher'].append(row['MEMBER_NAME'])
            track_raw_data['LabelName'].append(row['MEMBER_NAME'])
            


            album_raw_data['Product UPC'].append(row['PPL_RECORDING_ID'])
            album_raw_data['Product Title'].append(row['RECORDING_TITLE'])
            album_raw_data['Product Artist'].append(row['BAND_ARTIST_NAME'])



            check_track = Track.objects.filter(isrc=row['RECORDING_ISRC']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if not check_track.display_upc or check_track.display_upc != row['PPL_RECORDING_ID']:
                    print(f"updating display upc ")
                    check_track.display_upc = row['PPL_RECORDING_ID']
                if not check_track.artist or check_track.artist != row['BAND_ARTIST_NAME']:
                    print(f"updating track artist")
                    check_track.artist = row['BAND_ARTIST_NAME']
                if not check_track.title or check_track.title != row['RECORDING_TITLE']:
                    print(f"updating title ")
                    check_track.title = row['RECORDING_TITLE']
                if not check_track.release_name or check_track.release_name != row['RECORDING_TITLE']:
                    print(f"updating release name ")
                    check_track.release_name = row['RECORDING_TITLE']
                if not check_track.recording_artist or check_track.recording_artist != row['BAND_ARTIST_NAME']:
                    print(f"updating recording artist ")
                    check_track.recording_artist = row['BAND_ARTIST_NAME']
                if not check_track.publisher or check_track.publisher != row['MEMBER_NAME']:
                    print(f"updating publisher ")
                    check_track.publisher = row['MEMBER_NAME']
                if not check_track.label_name or check_track.label_name != row['MEMBER_NAME']:
                    print(f"updating label name ")
                    check_track.label_name = row['MEMBER_NAME']
                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_upc= row['PPL_RECORDING_ID']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    if not check_track_album.product_title or check_track_album.product_title != row['RECORDING_TITLE']:
                        print(f"updating product_title")
                        check_track_album.product_title = row['RECORDING_TITLE']
                    if not check_track_album.product_artist or check_track_album.product_artist != row['BAND_ARTIST_NAME']:
                        print(f"updating product artist")
                        check_track_album.product_artist = row['BAND_ARTIST_NAME']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['RECORDING_ISRC'], display_upc= row['PPL_RECORDING_ID'], title=row['RECORDING_TITLE'], recording_artist=row['BAND_ARTIST_NAME'] ,artist =row['BAND_ARTIST_NAME'] , release_name=row['RECORDING_TITLE'], label_name=row['MEMBER_NAME'], publisher=row['MEMBER_NAME'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['PPL_RECORDING_ID'], product_title = row['RECORDING_TITLE'] , product_artist = row['BAND_ARTIST_NAME'],  processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, display_upc = row['PPL_RECORDING_ID'], title  = row['RECORDING_TITLE'], isrc = row['RECORDING_ISRC'], recording_artist  = row['BAND_ARTIST_NAME'], artist  = row['BAND_ARTIST_NAME'], release_name  = row['BAND_ARTIST_NAME'], label_name = row['MEMBER_NAME'], publisher  = row['MEMBER_NAME'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()

        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_file = new_track_df.to_csv('processed_ppl_track.csv', index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = ppl_file
        new_track_document.file_name = 'processed_ppl_track.csv'
        new_track_document.save()

        with open('processed_ppl_track.csv', 'rb') as csv:
            new_track_document.file_doc.save('processed_ppl_track.csv', File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        album_file = new_album_df.to_csv('processed_ppl_album.csv', index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = ppl_file
        new_album_document.file_name = 'processed_ppl_album.csv'
        new_album_document.save()

        with open('processed_ppl_album.csv', 'rb') as csv:
            new_album_document.file_doc.save('processed_ppl_album.csv', File(csv))


        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {ppl_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {ppl_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise

############################
#     PPL  ACCOUNT          #
############################
@shared_task
def process_ppl_account(filename):
    ppl_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = [ 'PPL_MEMBER_ID', 'MEMBER_NAME', 'ALLOCATION_PERIOD', 'RECORDING_CONTENT_TYPE', 'PPL_RECORDING_ID', 'RECORDING_ISRC', 'BAND_ARTIST_NAME', 'RECORDING_TITLE', 'COUNTRY_OF_RECORDING', 'RECORDING_P_DATE', 'RIGHTSHOLDING_PERCENTAGE', 'REVENUE_SOURCE_LEVEL_1', 'REVENUE_SOURCE_LEVEL_2', 'REVENUE_SOURCE_LEVEL_3', 'REVENUE_SOURCE_LEVEL_4', 'RECORDING_ALLOCATION_AMOUNT_GBP', 'NON_QUALIFYING_PERFORMER_ALLOCATION_AMOUNT_GBP', 'TOTAL_ALLOCATION_AMOUNT_GBP']
        ppl = pd.read_excel(ppl_file.file_doc, usecols=col_list).fillna('')
        print(ppl.head())
        if 'RECORDING_ISRC' not in ppl:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {ppl_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com','shola.albert@gmail.com'],
                            fail_silently=False,
                            )
        else:
            pass
        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount',' Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }

        for index, row in ppl.iterrows():
            account_raw_data['Period'].append(row['ALLOCATION_PERIOD'])
            account_raw_data['Activity Period'].append(row['ALLOCATION_PERIOD'])
            account_raw_data['Retailer'].append(row['REVENUE_SOURCE_LEVEL_4'])
            account_raw_data['Territory'].append(row['COUNTRY_OF_RECORDING'])
            account_raw_data['Product UPC'].append(row['PPL_RECORDING_ID'])
            account_raw_data['Manufacturer UPC'].append(row['PPL_RECORDING_ID'])
            account_raw_data['Imprint Label'].append(row['MEMBER_NAME'])
            account_raw_data['Artist Name'].append(row['BAND_ARTIST_NAME'])
            account_raw_data['Product Name'].append(row['RECORDING_TITLE'])
            account_raw_data['Track Name'].append(row['RECORDING_TITLE'])
            account_raw_data['Track Artist'].append(row['BAND_ARTIST_NAME'])
            account_raw_data['ISRC'].append(row['RECORDING_ISRC'])
            account_raw_data['Total'].append(row['RECORDING_ALLOCATION_AMOUNT_GBP'])
            account_raw_data['Adjusted Total'].append(row['NON_QUALIFYING_PERFORMER_ALLOCATION_AMOUNT_GBP'])
            account_raw_data['Label Share Net Receipts'].append(row['TOTAL_ALLOCATION_AMOUNT_GBP'])
            

            new_account = Accounting( period = row['ALLOCATION_PERIOD'], activity_period = row['ALLOCATION_PERIOD'], retailer = row['REVENUE_SOURCE_LEVEL_4'], territory = row['COUNTRY_OF_RECORDING'], orchard_UPC = row['PPL_RECORDING_ID'], manufacturer_UPC = row['PPL_RECORDING_ID'], imprint_label = row['MEMBER_NAME'], artist_name = row['BAND_ARTIST_NAME'], product_name = row['RECORDING_TITLE'], track_name = row['RECORDING_TITLE'], track_artist =  row['BAND_ARTIST_NAME'], isrc = row['RECORDING_ISRC'], total = row['RECORDING_ALLOCATION_AMOUNT_GBP'], adjusted_total = row['NON_QUALIFYING_PERFORMER_ALLOCATION_AMOUNT_GBP'], label_share_net_receipts = row['TOTAL_ALLOCATION_AMOUNT_GBP'], vendor = "PPL", processed_day = today)
            new_account.save()

        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_account = new_account_df.to_csv('processed_ppl_account.csv', index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = ppl_file
        new_account_document.file_name = 'processed_ppl_account.csv'
        new_account_document.save()

        with open('processed_ppl_account.csv', 'rb') as csv:
            new_account_document.file_doc.save('processed_ppl_account.csv', File(csv))

        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {ppl_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {ppl_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise



############################
#     MCPS                 #
############################
@shared_task
def process_mcps(filename):
    mcps_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:        
        col_list = [ 'Distribution ID', 'Manufacturer Number', 'Manufacturer Short Name', 'Manufacturer Name', 'Catalogue Number', 'Product Title', 'Artist', 'Tunecode', 'Title', 'Interested Party (1)', 'Interested Party (2)', 'Interested Party (3)', 'Interested Party (4)', 'Period', 'Quantity', 'Territory', 'CAE Number', 'Member', 'Role', 'Share(%)', 'Gross Amount', 'Commission', 'Nett Amount', 'Royalty Amount', 'Invoice & Batch', 'Source', 'Source Description']
        mcps = pd.read_excel(mcps_file.file_doc, usecols=col_list).fillna('')
        if 'Catalogue Number' not in mcps:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {mcps_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com','shola.albert@gmail.com'],
                            fail_silently=False,
                            )
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

        for index, row in mcps.iterrows():

            track_raw_data['ISRC'].append(row['Tunecode'])
            track_raw_data['DisplayUPC'].append(row['Manufacturer Number'])
            track_raw_data['Artist'].append(row['Interested Party (1)'])
            track_raw_data['Title'].append(row['Title'])
            track_raw_data['Release Name'].append(row['Product Title'])
            track_raw_data['Recording Artist'].append(row['Interested Party (1)'])
            track_raw_data['Publisher'].append(row['Member'])
            track_raw_data['LabelName'].append(row['Interested Party (3)'])



            album_raw_data['Product UPC'].append(row['Manufacturer Number'])
            album_raw_data['Product Artist'].append(row['Interested Party (1)'])
            album_raw_data['Release Date'].append(row['Period'])
            album_raw_data['Catalog Number'].append(row['Catalogue Number'])
            album_raw_data['Imprint Label'].append(row['Interested Party (3)'])

            check_track = Track.objects.filter(isrc=row['Tunecode']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if not check_track.display_upc or check_track.display_upc != row['Manufacturer Number']:
                    print(f"updating display upc ")
                    check_track.display_upc = row['Manufacturer Number']
                if not check_track.artist or check_track.artist != row['Interested Party (1)']:
                    print(f"updating track artist")
                    check_track.artist = row['Interested Party (1)']
                if not check_track.title or check_track.title != row['Title']:
                    print(f"updating title ")
                    check_track.title = row['Title']
                if not check_track.release_name or check_track.release_name != row['Product Title']:
                    print(f"updating release name ")
                    check_track.release_name = row['Product Title']
                if not check_track.recording_artist or check_track.recording_artist != row['Interested Party (1)']:
                    print(f"updating recording artist")
                    check_track.recording_artist = row['Interested Party (1)']
                if not check_track.publisher or check_track.publisher !=  row['Member']:
                    print(f"updating publisher ")
                    check_track.publisher = row['Member']
                if not check_track.label_name or check_track.label_name != row['Interested Party (3)']:
                    print(f"updating label name ")
                    check_track.label_name = row['Interested Party (3)']
                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_upc= row['Manufacturer Number']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    if not check_track_album.release_date:
                        check_track_album.release_date = row['Period']
                    if not check_track_album.product_artist:
                        print(f"updating product artist")
                        check_track_album.product_artist =  row['Interested Party (1)']
                    if not check_track_album.imprint_label:
                        check_track_album.imprint_label = row['Interested Party (3)']
                    if not check_track_album.catalog_number:
                        check_track_album.catalog_number = row['Catalogue Number']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['Tunecode'], display_upc= row['Manufacturer Number'], title = row['Title'], recording_artist=row['Interested Party (1)'] ,artist =row['Interested Party (1)'] , release_name=row['Product Title'], label_name=row['Interested Party (3)'], publisher=row['Member'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                    
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['Manufacturer Number'] , product_artist = row['Interested Party (1)'],release_date = row['Period'], imprint_label = row['Interested Party (3)'], catalog_number = row['Catalogue Number'],  processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, display_upc = row['Manufacturer Number'], title  = row['Title'], isrc =row['Tunecode'], recording_artist  = row['Interested Party (1)'], artist  = row['Interested Party (1)'], release_name  = row['Product Title'], label_name = row['Interested Party (3)'], publisher  = row['Member'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                    
        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_file = new_track_df.to_csv('processed_mcps_track.csv', index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = mcps_file
        new_track_document.file_name = 'processed_mcps_track.csv'
        new_track_document.save()

        with open('processed_mcps_track.csv', 'rb') as csv:
            new_track_document.file_doc.save('processed_mcps_track.csv', File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        album_file = new_album_df.to_csv('processed_mcps_album.csv', index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = mcps_file
        new_album_document.file_name = 'processed_mcps_album.csv'
        new_album_document.save()

        with open('processed_mcps_album.csv', 'rb') as csv:
            new_album_document.file_doc.save('processed_mcps_album.csv', File(csv))

        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {mcps_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {mcps_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise





############################
#     MCPS  ACCOUNT        #
############################
@shared_task
def process_mcps_account(filename):
    mcps_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = [ 'Distribution ID', 'Manufacturer Number', 'Manufacturer Short Name', 'Manufacturer Name', 'Catalogue Number', 'Product Title', 'Artist', 'Tunecode', 'Title', 'Interested Party (1)', 'Interested Party (2)', 'Interested Party (3)', 'Interested Party (4)', 'Period', 'Quantity', 'Territory', 'CAE Number', 'Member', 'Role', 'Share(%)', 'Gross Amount', 'Commission', 'Nett Amount', 'Royalty Amount', 'Invoice & Batch', 'Source', 'Source Description']
        mcps = pd.read_excel(mcps_file.file_doc, usecols=col_list).fillna('')
        if 'Catalogue Number' not in mcps:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {mcps_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com','shola.albert@gmail.com'],
                            fail_silently=False,
                            )
        else:
            pass

        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }

        for index, row in mcps.iterrows():
            account_raw_data['Period'].append(row['Distribution ID'])
            account_raw_data['Activity Period'].append(row['Period'])
            account_raw_data['Retailer'].append(row['Manufacturer Name'])
            account_raw_data['Territory'].append(row['Territory'])
            account_raw_data['Product UPC'].append(row['Catalogue Number'])
            account_raw_data['Project Code'].append(row['Manufacturer Short Name'])
            account_raw_data['Product Code'].append(row['Tunecode'])
            account_raw_data['Manufacturer UPC'].append(row['Manufacturer Number'])
            account_raw_data['Subaccount'].append(row['Interested Party (2)'])
            account_raw_data['Imprint Label'].append(row['Interested Party (3)'])
            account_raw_data['Artist Name'].append(row['Artist'])
            account_raw_data['Product Name'].append(row['Product Title'])
            account_raw_data['Track Name'].append(row['Title'])
            account_raw_data['Track Artist'].append(row['Interested Party (1)'])
            account_raw_data['ISRC'].append(row['Tunecode'])
            account_raw_data['Volume'].append(row['Quantity'])
            account_raw_data['Trans Type'].append(row['Source'])
            account_raw_data['Trans Type Description'].append(row['Source Description'])
            account_raw_data['Discount'].append(row['Commission'])
            account_raw_data['Price'].append(row['Gross Amount'])
            account_raw_data['Quantity'].append(row['Quantity'])
            account_raw_data['Total'].append(row['Nett Amount'])
           

            new_account = Accounting(period = row['Distribution ID'], activity_period=row['Period'], retailer = row['Manufacturer Name'], territory = row['Territory'], orchard_UPC = row['Catalogue Number'], manufacturer_UPC = row['Manufacturer Number'], project_code = row['Manufacturer Short Name'], product_code = row['Tunecode'], subaccount = row['Interested Party (2)'], imprint_label = row['Interested Party (3)'], artist_name = row['Artist'], product_name = row['Product Title'], track_name = row['Title'], track_artist = row['Interested Party (1)'], isrc =  row['Tunecode'], volume = row['Quantity'], trans_type = row['Source'], trans_type_description = row['Source Description'], discount = row['Commission'], actual_price = row['Gross Amount'], quantity = row['Quantity'], total = row['Nett Amount'],vendor="MCPS", processed_day=today)
            new_account.save()
        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {mcps_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)

        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_account = new_account_df.to_csv('processed_mcps_account.csv', index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = mcps_file
        new_account_document.file_name = 'processed_mcps_account.csv'
        new_account_document.save()

        with open('processed_mcps_account.csv', 'rb') as csv:
            new_account_document.file_doc.save('processed_mcps_account.csv', File(csv))

    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {mcps_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise


############################
#     DITTO                #
############################
@shared_task
def process_ditto(filename):
    ditto_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = ['Track Number (required)', 'Track Titles (required)', 'Subtitle / Version', 'Volume Number (required)',	 'Release Title (required)', 'Primary Artists (commas seperated) (required)', 'Featuring Artists (comma seperated) ', 'Remixer Artists (comma seperated)',	 'Authors (comma seperated)',	 'Composers (comma seperated)', 'Label (required)', 'Production Year (required)', 'Production Owner (required)', 'Copyright Owner (required)',	 'Genre (required)', 'Track Type (required)', 'Lyrics Language (required)', 'Title Language (required)', 'Parental Advisory (required)', 'Territories to Deliver (World or comma seperated list of country codes BR,GB,US) (required)', 'Release Price Teir (required)', 'Track Price Tier (required)', 'Publisher (required)', 'Digital release date', 'Physical Release Date (required)',	 'Sample Start Index', 'ISRC Code', 'UPC Code', 'EAN Code', 'Grid', 'Release catalog Number', 'Track catalog number' ]
        ditto = pd.read_excel(ditto_file.file_doc, usecols=col_list).fillna('')
        print(ditto.head())
        if 'ISRC Code' not in ditto:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {ditto_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com','shola.albert@gmail.com'],
                            fail_silently=False,
                            )
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
            track_raw_data['ISRC'].append(row['ISRC Code'])
            track_raw_data['DisplayUPC'].append(row['UPC Code'])
            track_raw_data['Artist'].append(row['Primary Artists (commas seperated) (required)'])
            track_raw_data['Title'].append(row['Track Titles (required)'])
            track_raw_data['Genre'].append(row['Genre (required)'])
            track_raw_data['Release Name'].append(row['Release Title (required)'])
            track_raw_data['Recording Artist'].append(row['Primary Artists (commas seperated) (required)'])
            track_raw_data['Publisher'].append(row['Publisher (required)'])
            track_raw_data['LabelName'].append(row['Label (required)'])
            track_raw_data['Territories'].append(row['Territories to Deliver (World or comma seperated list of country codes BR,GB,US) (required)'])
            track_raw_data['SalesStartDate'].append(row['Digital release date'])


            album_raw_data['Product UPC'].append(row['UPC Code'])
            album_raw_data['Total Volumes'].append(row['Volume Number (required)'])
            album_raw_data['Genre'].append(row['Genre (required)'])
            album_raw_data['Product Title'].append(row['Release Title (required)'])
            album_raw_data['Release Date'].append(row['Digital release date'])
            album_raw_data['Product Artist'].append(row['Primary Artists (commas seperated) (required)'])
            album_raw_data['Imprint Label'].append(row['Label (required)'])
            album_raw_data['Catalog Number'].append(row['Track catalog number'])

            check_track = Track.objects.filter(isrc=row['ISRC Code']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if not check_track.display_upc:
                    print(f"updating display upc ")
                    check_track.display_upc = row['UPC Code']
                if not check_track.artist:
                    print(f"updating track artist")
                    check_track.artist = row['Primary Artists (commas seperated) (required)']
                if not check_track.title:
                    print(f"updating title ")
                    check_track.title = row['Track Titles (required)']
                if not check_track.genre:
                    check_track.genre = row['Genre (required)']
                if not check_track.release_name:
                    print(f"updating release name ")
                    check_track.release_name = row['Release Title (required)']
                if not check_track.recording_artist:
                    print(f"updating recording artist")
                    check_track.recording_artist = row['Primary Artists (commas seperated) (required)']
                if not check_track.publisher:
                    print(f"updating publisher ")
                    check_track.publisher = row['Publisher (required)']
                if not check_track.label_name:
                    print(f"updating label name ")
                    check_track.label_name = row['Label (required)']
                if not check_track.territories:
                    check_track.territories = row['Territories to Deliver (World or comma seperated list of country codes BR,GB,US) (required)']
                if not check_track.sales_start_date:
                    check_track.sales_start_date = row['Digital release date']
                check_track.save()
            else:
                check_track_album = Album.objects.filter(product_upc= row['UPC Code']).first()
                if check_track_album:
                    print(f" track album {check_track_album.product_upc} exists, creating track now! ")
                    if not check_track_album.total_volume:
                        check_track_album.total_volume = row['Volume Number (required)']
                    if not check_track_album.product_title:
                        check_track_album.product_title = row['Release Title (required)']
                    if not check_track_album.genre:
                        check_track_album.genre = row['Genre (required)']
                    if not check_track_album.release_date:
                        check_track_album.release_date = row['Digital release date']
                    if not check_track_album.product_artist:
                        print(f"updating product artist")
                        check_track_album.product_artist =  row['Primary Artists (commas seperated) (required)']
                    if not check_track_album.imprint_label:
                        check_track_album.imprint_label = row['Label (required)']
                    if not check_track_album.catalog_number:
                        check_track_album.catalog_number = row['Track catalog number']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['ISRC Code'],genre=row['Genre (required)'], display_upc= row['UPC Code'], title = row['Track Titles (required)'], recording_artist=row['Primary Artists (commas seperated) (required)'], artist = row['Primary Artists (commas seperated) (required)'] , release_name=row['Release Title (required)'], label_name=row['Label (required)'], publisher=row['Publisher (required)'], territories = row['Territories to Deliver (World or comma seperated list of country codes BR,GB,US) (required)'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['UPC Code'] , product_title = row['Release Title (required)'], product_artist = row['Primary Artists (commas seperated) (required)'], genre = row['Genre (required)'], release_date = row['Digital release date'], imprint_label = row['Label (required)'], catalog_number = row['Track catalog number'],  processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, display_upc = row['UPC Code'], genre=row['Genre (required)'], title  = row['Track Titles (required)'], isrc = row['ISRC Code'], recording_artist  = row['Primary Artists (commas seperated) (required)'], artist  = row['Primary Artists (commas seperated) (required)'], release_name  = row['Release Title (required)'], label_name = row['Label (required)'],territories = row['Territories to Deliver (World or comma seperated list of country codes BR,GB,US) (required)'], publisher  = row['Publisher (required)'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()

        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_file = new_track_df.to_csv('processed_ditto_track.csv', index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = ditto_file
        new_track_document.file_name = 'processed_ditto_track.csv'
        new_track_document.save()

        with open('processed_ditto_track.csv', 'rb') as csv:
            new_track_document.file_doc.save('processed_ditto_track.csv', File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        album_file = new_album_df.to_csv('processed_ditto_album.csv', index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = ditto_file
        new_album_document.file_name = 'processed_ditto_album.csv'
        new_album_document.save()

        with open('processed_ditto_album.csv', 'rb') as csv:
            new_album_document.file_doc.save('processed_ditto_album.csv', File(csv))

        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {ditto_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {ditto_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise

############################
#     DITTO ACCOUNT       #
############################
@shared_task
def process_ditto_account(filename):
    ditto_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        # col_list = ['Track Number (required)', 'Track Titles (required)', 'Subtitle / Version', 'Volume Number (required)',	 'Release Title (required)', 'Primary Artists (commas seperated) (required)', 'Featuring Artists (comma seperated) ', 'Remixer Artists (comma seperated)',	 'Authors (comma seperated)',	 'Composers (comma seperated)', 'Label (required)', 'Production Year (required)', 'Production Owner (required)', 'Copyright Owner (required)',	 'Genre (required)', 'Track Type (required)', 'Lyrics Language (required)', 'Title Language (required)', 'Parental Advisory (required)', 'Territories to Deliver (World or comma seperated list of country codes BR,GB,US) (required)', 'Release Price Teir (required)', 'Track Price Tier (required)', 'Publisher (required)', 'Digital release date', 'Physical Release Date (required)',	 'Sample Start Index', 'ISRC Code', 'UPC Code', 'EAN Code', 'Grid', 'Release catalog Number', 'Track catalog number' ]
        col_list = ['Artist Display Name','Begin Date', 'Countries',  'End Date', 'Id', 'Import Date', 'Isrc Code', 'Manual Barcode', 'Release Title1', 'Stores', 'Track Title', 'Total Sales', 'Unit Price', 'Units']
        ditto = pd.read_excel(ditto_file.file_doc, usecols=col_list).fillna('')
        print(ditto.head())
        if 'Isrc Code' not in ditto:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {ditto_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com','shola.albert@gmail.com'],
                            fail_silently=False,
                            )
        else:
            pass
        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount',' Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],' Project Code':[],'Product Code':[],'Subaccount':[],' Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }
        for index, row in ditto.iterrows():
            account_raw_data['Period'].append(row['End Date'])
            account_raw_data['Territory'].append(row['Countries'])
            account_raw_data['Activity Period'].append(f"{row['Begin Date']} - {row['End Date']}")
            account_raw_data['Product UPC'].append(row['Manual Barcode'])
            account_raw_data['Manufacturer UPC'].append(row['Manual Barcode'])
            account_raw_data['Product Code'].append(row['Id'])
            account_raw_data['Track Artist'].append(row['Artist Display Name'])
            account_raw_data['ISRC'].append(row['Isrc Code'])
            account_raw_data['Track Name'].append(row['Track Title'])
            account_raw_data['Unit Price'].append(row['Unit Price'])
            account_raw_data['Price'].append(row['Unit Price'])
            account_raw_data['Quantity'].append(row['Units'])
            account_raw_data['Total'].append(row['Total Sales'])
            account_raw_data['Label Share Net Receipts'].append(row['Total Sales'])

            new_account = Accounting( period = row['End Date'], activity_period =f"{row['Begin Date']} - {row['End Date']}", territory = row['Countries'], orchard_UPC = row['Manual Barcode'], manufacturer_UPC = row['Manual Barcode'], product_code = row['Id'], artist_name = row['Artist Display Name'],  product_name = row['Release Title1'], track_name = row['Track Title'], track_artist = row['Artist Display Name'], isrc = row['Isrc Code'], unit_price = row['Unit Price'], actual_price = row['Unit Price'], quantity = row['Units'], total = row['Total Sales'], label_share_net_receipts = row['Total Sales'], vendor="DITTO", processed_day=today)
            new_account.save()
        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_account = new_account_df.to_csv('processed_ditto_account.csv', index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = ditto_file
        new_account_document.file_name = 'processed_ditto_account.csv'
        new_account_document.save()

        with open('processed_ditto_account.csv', 'rb') as csv:
            new_account_document.file_doc.save('processed_ditto_account.csv', File(csv))

        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {ditto_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {ditto_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise



############################
#     HORUS                #
############################
@shared_task
def process_horus(filename):
    horus_file = ProcessedDocument.objects.filter(file_name=filename).first()
    # col_list = ['CHECK NO.', 'GROUPING ID', 'PRODUCT TITLE', 'VERSION DESCRIPTION', 'ARTIST(S)', 'DISPLAY ARTIST', 'BARCODE', 'CATALOGUE NO.', 'RELEASE FORMAT TYPE', 'SOUND CARRIER', 'PRICE BAND', 'LICENSED TERRITORIES to INCLUDE', 'LICENSED TERRITORIES to EXCLUDE', 'RELEASE START DATE', 'RELEASE END DATE', 'GRid', '(P) YEAR', '(P) HOLDER', '(C) YEAR', '(C) HOLDER', 'STATUS', 'LABEL', 'GENRE(S)', 'EXPLICIT CONTENT', 'VOLUME NO.', 'VOLUME TOTAL', 'SERVICES', 'TRACK NO.', 'TRACK TITLE', 'MIX / VERSION', 'ARTIST(S)', 'DISPLAY ARTIST', 'ISRC', 'GRid', 'AVAILABLE SEPARATELY', '(P) YEAR', '(P) HOLDER', '(C) YEAR', '(C) HOLDER', 'GENRE(S)', 'EXPLICIT CONTENT', 'PRODUCER(S)', 'MIXER(S)', 'COMPOSER(S)', 'LYRICIST(S)', 'PUBLISHER(S)', 'Language']
    try:
        col_list = ['CHECK NO.', 'GROUPING ID', 'PRODUCT TITLE', 'VERSION DESCRIPTION', 'ARTIST(S)', 'DISPLAY ARTIST', 'BARCODE', 'CATALOGUE NO.', 'RELEASE FORMAT TYPE', 'SOUND CARRIER', 'PRICE BAND', 'LICENSED TERRITORIES to INCLUDE', 'LICENSED TERRITORIES to EXCLUDE', 'RELEASE START DATE', 'RELEASE END DATE', 'GRid', '(P) YEAR', '(P) HOLDER', '(C) YEAR', '(C) HOLDER', 'STATUS', 'LABEL', 'GENRE(S)', 'EXPLICIT CONTENT', 'VOLUME NO.', 'VOLUME TOTAL', 'SERVICES', 'TRACK NO.', 'TRACK TITLE', 'MIX / VERSION', 'ARTIST(S)', 'DISPLAY ARTIST', 'ISRC', 'GRid', '(P) YEAR', '(P) HOLDER', '(C) YEAR', '(C) HOLDER', 'GENRE(S)', 'EXPLICIT CONTENT', 'PRODUCER(S)', 'MIXER(S)', 'COMPOSER(S)', 'LYRICIST(S)', 'PUBLISHER(S)', 'Language']
        horus = pd.read_excel(horus_file.file_doc, skipinitialspace=True, header=[10], usecols=col_list).fillna('')
        # horus.replace({'-': None, 'None': None})
        if 'ISRC' not in horus:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {horus_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com','shola.albert@gmail.com'],
                            fail_silently=False,
                            )
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
            track_raw_data['DisplayUPC'].append(row['BARCODE'])
            track_raw_data['ISRC'].append(row['ISRC'])
            track_raw_data['Genre'].append(row['GENRE(S)'])
            track_raw_data['Title'].append(row['TRACK TITLE'])
            track_raw_data['Artist'].append(row['DISPLAY ARTIST'])
            track_raw_data['Release Name'].append(row['PRODUCT TITLE'])
            track_raw_data['Recording Artist'].append(row['ARTIST(S)'])
            track_raw_data['Publisher'].append(row['PUBLISHER(S)'])
            track_raw_data['LabelName'].append(row['LABEL'])
            track_raw_data['Writer'].append(row['COMPOSER(S)'])
            track_raw_data['SalesStartDate'].append(row['RELEASE START DATE'])

            album_raw_data['Product UPC'].append(row['BARCODE'])
            album_raw_data['Total Volumes'].append(row['VOLUME TOTAL'])
            album_raw_data['Product Title'].append(row['PRODUCT TITLE'])
            album_raw_data['Genre'].append(row['GENRE(S)'])
            album_raw_data['Release Date'].append(row['RELEASE START DATE'])
            album_raw_data['Product Artist'].append(row['ARTIST(S)'])
            album_raw_data['Imprint Label'].append(row['LABEL'])
            album_raw_data['Catalog Number'].append(row['CATALOGUE NO.'])

            check_track = Track.objects.filter(isrc=row['ISRC']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if not check_track.display_upc:
                    print(f"updating display upc ")
                    check_track.display_upc = row['BARCODE']
                if not check_track.artist:
                    print(f"updating track artist")
                    check_track.artist = row['DISPLAY ARTIST']
                if not check_track.title:
                    print(f"updating title")
                    check_track.title = row['TRACK TITLE']
                if not check_track.genre:
                    check_track.genre = row['GENRE(S)']
                if not check_track.release_name:
                    print(f"updating release name ")
                    check_track.release_name = row['PRODUCT TITLE']
                if not check_track.recording_artist:
                    print(f"updating recording artist")
                    check_track.recording_artist = row['ARTIST(S)']
                if not check_track.publisher:
                    print(f"updating publisher ")
                    check_track.publisher = row['PUBLISHER(S)']
                if not check_track.label_name:
                    print(f"updating label name ")
                    check_track.label_name = row['LABEL']
                if not check_track.writer:
                    check_track.writer = row['COMPOSER(S)']
                if not check_track.sales_start_date:
                    check_track.sales_start_date = row['RELEASE START DATE']
                check_track.save()

            else:
                check_track_album = Album.objects.filter(product_upc= row['BARCODE']).first()
                if check_track_album:
                    print(f"track album {check_track_album.product_upc} exists, creating track now! ")
                    if not check_track_album.total_volume:
                        check_track_album.total_volume = row['VOLUME TOTAL']
                    if not check_track_album.product_title:
                        check_track_album.product_title = row['PRODUCT TITLE']
                    if not check_track_album.genre:
                        check_track_album.genre = row['GENRE(S)']
                    if not check_track_album.release_date:
                        check_track_album.release_date = row['RELEASE START DATE']
                    if not check_track_album.product_artist:
                        print(f"updating product artist")
                        check_track_album.product_artist =  row['ARTIST(S)']
                    if not check_track_album.imprint_label:
                        check_track_album.imprint_label = row['LABEL']
                    if not check_track_album.catalog_number:
                        check_track_album.catalog_number = row['CATALOGUE NO.']
                    check_track_album.save()
                    new_track = Track(album = check_track_album, isrc=row['ISRC'],genre=row['GENRE(S)'], display_upc= row['BARCODE'], title = row['TRACK TITLE'], recording_artist=row['ARTIST(S)'], artist = row['DISPLAY ARTIST'] , release_name=row['PRODUCT TITLE'], label_name=row['LABEL'], publisher=row['PUBLISHER(S)'], processed_day=today)
                    new_track.save()

                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{check_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()
                else:
                    print(f"track album does not exists, creating track album and track now! ")
                    new_track_album = Album(product_upc = row['BARCODE'] , product_title = row['PRODUCT TITLE'], product_artist = row['ARTIST(S)'], genre = row['GENRE(S)'], release_date = row['RELEASE START DATE'], imprint_label = row['LABEL'], processed_day=today )
                    new_track_album.save()
                    new_track = Track(album = new_track_album, isrc=row['ISRC'],genre=row['GENRE(S)'], display_upc= row['BARCODE'], title = row['TRACK TITLE'], recording_artist=row['ARTIST(S)'], artist = row['DISPLAY ARTIST'] , release_name=row['PRODUCT TITLE'], label_name=row['LABEL'], publisher=row['PUBLISHER(S)'], processed_day=today)
                    new_track.save()
                    new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track", display_upc = new_track.display_upc, isrc = new_track.isrc, label_name = new_track.label_name, publisher = new_track.publisher, recording_artist = new_track.recording_artist, release_name = new_track.release_name, title = new_track.title, zIDKey_UPCISRC = f"{new_track_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                    new_master.save()

        
        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_file = new_track_df.to_csv('processed_horus_track.csv', index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = horus_file
        new_track_document.file_name = 'processed_horus_track.csv'
        new_track_document.save()

        with open('processed_horus_track.csv', 'rb') as csv:
            new_track_document.file_doc.save('processed_horus_track.csv', File(csv))

        print(album_raw_data)
        new_album_df = pd.DataFrame({key:pd.Series(value) for key, value in album_raw_data.items() }, columns=album_col)
        album_file = new_album_df.to_csv('processed_horus_album.csv', index = False, header=True)
        # alb_name = f'processed_horus_{today}_album.csv'
        new_album_document = ProcessedAlbumFile()
        new_album_document.document = horus_file
        new_album_document.file_name = 'processed_horus_album.csv'
        new_album_document.save()

        with open('processed_horus_album.csv', 'rb') as csv:
            new_album_document.file_doc.save('processed_horus_album.csv', File(csv))
        
          
        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {horus_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {horus_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise


############################
#     BMI                  #
############################
@shared_task
def process_bmi(filename):
    bmi_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = [ 'PERIOD', 'W OR P', 'PARTICIPANT NAME',	 'PARTICIPANT #', 'IP #', 'TITLE NAME',	 'ARTISTS ', 'LABEL IMPRINT', 'TITLE #', 'PERF SOURCE', 'COUNTRY OF PERFORMANCE',	 'SHOW NAME', 'EPISODE NAME', 'SHOW #', 'USE CODE', 'TIMING', 'PARTICIPANT %', 'PERF COUNT', 'BONUS LEVEL', 'ROYALTY AMOUNT', 'WITHHOLD', 'PERF PERIOD', 'CURRENT ACTIVITY AMT', 'HITS SONG OR TV NET SUPER USAGE BONUS', 'STANDARDS OR TV NET THEME BONUS', 'FOREIGN SOCIETY ADJUSTMENT', 'COMPANY CODE',	 'COMPANY NAME' ]
        bmi = pd.read_excel(bmi_file.file_doc, usecols=col_list).fillna('')
        print(bmi.head())
        if 'PERF SOURCE' not in bmi:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {bmi_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['digger@wyldpytch.com', 'shola.albert@gmail.com'],
                            fail_silently=False,
                            )
        else:
            pass

        track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
        track_raw_data = {
            'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
        }


        for index, row in bmi.iterrows():
            track_raw_data['ISRC'].append(row['TITLE #'])
            track_raw_data['Title'].append(row['TITLE NAME'])
            track_raw_data['Artist'].append(row['ARTISTS '])
            track_raw_data['Recording Artist'].append(row['ARTISTS '])
            track_raw_data['LabelName'].append(row['LABEL IMPRINT'])

            check_track = Track.objects.filter(isrc=row['TITLE #']).first()
            if check_track:
                print(f" track {check_track.isrc} already exists")
                if not check_track.title:
                    print(f"updating title ")
                    check_track.title = row['TITLE NAME']
                if not check_track.artist:
                    check_track.artist = row['ARTISTS ']
                if not check_track.recording_artist:
                    print(f"updating recording artist")
                    check_track.recording_artist = row['ARTISTS ']
                if not check_track.label_name:
                    print(f"updating label name ")
                    check_track.label_name = row['LABEL IMPRINT']
                check_track.save()
            else:
                print(f"track does not exists, creating track now! ")
                new_track = Track( title  = row['TITLE NAME'], isrc = row['TITLE #'], recording_artist  = row['ARTISTS '], artist  = row['ARTISTS '], label_name = row['LABEL IMPRINT'], processed_day=today)
                new_track.save()
                new_master = Master_DRMSYS( artist = new_track.artist, data_type = "Track",  isrc = new_track.isrc, label_name = new_track.label_name, recording_artist = new_track.recording_artist, title = new_track.title, track = new_track , processed_day=today)
                new_master.save()

        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {bmi_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)

        print(track_raw_data) 
        new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_file = new_track_df.to_csv('processed_bmi_track.csv', index = False, header=True)
        new_track_document = ProcessedTrackFile()
        new_track_document.document = bmi_file
        new_track_document.file_name = 'processed_bmi_track.csv'
        new_track_document.save()

        with open('processed_bmi_track.csv', 'rb') as csv:
            new_track_document.file_doc.save('processed_bmi_track.csv', File(csv))

        

    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {bmi_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise



        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {bmi_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com', 'shola.albert@gmail.com'],
                        fail_silently=False,)


############################
#     BMI ACCOUNT          #
############################
@shared_task
def process_bmi_account(filename):
    bmi_file = ProcessedDocument.objects.filter(file_name=filename).first()
    try:
        col_list = [ 'PERIOD', 'W OR P', 'PARTICIPANT NAME',	 'PARTICIPANT #', 'IP #', 'TITLE NAME',	 'ARTISTS ', 'LABEL IMPRINT', 'TITLE #', 'PERF SOURCE', 'COUNTRY OF PERFORMANCE',	 'SHOW NAME', 'EPISODE NAME', 'SHOW #', 'USE CODE', 'TIMING', 'PARTICIPANT %', 'PERF COUNT', 'BONUS LEVEL', 'ROYALTY AMOUNT', 'WITHHOLD', 'PERF PERIOD', 'CURRENT ACTIVITY AMT', 'HITS SONG OR TV NET SUPER USAGE BONUS', 'STANDARDS OR TV NET THEME BONUS', 'FOREIGN SOCIETY ADJUSTMENT', 'COMPANY CODE',	 'COMPANY NAME' ]
        bmi = pd.read_excel(bmi_file.file_doc, usecols=col_list).fillna('')
        print(bmi.head())

        if 'PERF SOURCE' not in bmi:
            send_mail(
                            'Corrupted File',
                            f''' 
                            Hello Admin 
                            The file {bmi_file.file_name} was corrupt !
                            Pls login to run it again
                            ''',
                            'Trackeet Recording Panel ',
                            ['shola.albert@gmail.com'],
                            fail_silently=False,
                            )
        else:
            pass

        account_col = [ 'Period', 'Activity Period', 'Retailer','Territory','Product UPC','Manufacturer UPC','Project Code','Product Code','Subaccount','Imprint Label','Artist Name','Product Name','Track Name','Track Artist','ISRC','Volume','Track ','Trans Type','Trans Type Description','Unit Price','Discount','Price','Quantity','Total','Adjusted Total','Split Rate','Label Share Net Receipts','Ringtone Publishing','Cloud Publishing','Publishing','Mech. Administrative Fee','Preferred Currency']
        account_raw_data = {
                'Period':[], 'Activity Period':[], 'Retailer':[],'Territory':[],'Product UPC':[],'Manufacturer UPC':[],'Project Code':[],'Product Code':[],'Subaccount':[],'Imprint Label':[],'Artist Name':[],'Product Name':[],'Track Name':[],'Track Artist':[],'ISRC':[],'Volume':[],'Track':[],'Trans Type':[],'Trans Type Description':[],'Unit Price':[],'Discount':[],'Price':[],'Quantity':[],'Total':[],'Adjusted Total':[],'Split Rate':[],'Label Share Net Receipts':[],'Ringtone Publishing':[],'Cloud Publishing':[],'Publishing':[],'Mech. Administrative Fee':[],'Preferred Currency':[]
                            }

        for index, row in bmi.iterrows():

            account_raw_data['Period'].append(row['PERIOD'])
            account_raw_data['Activity Period'].append(row['PERF PERIOD'])
            account_raw_data['Retailer'].append(row['PERF SOURCE'])
            account_raw_data['Product UPC'].append(row['TITLE #'])
            account_raw_data['Territory'].append(row['COUNTRY OF PERFORMANCE'])
            account_raw_data['Project Code'].append(row['SHOW NAME'])
            account_raw_data['Product Code'].append(row['EPISODE NAME'])
            account_raw_data['Imprint Label'].append(row['LABEL IMPRINT'])
            account_raw_data['Artist Name'].append(row['ARTISTS '])
            account_raw_data['Product Name'].append(row['TITLE NAME'])
            account_raw_data['ISRC'].append(row['TITLE #'])
            account_raw_data['Volume'].append(row['SHOW #'])
            account_raw_data['Unit Price'].append(row['CURRENT ACTIVITY AMT'])
            account_raw_data['Discount'].append(row['WITHHOLD'])
            account_raw_data['Price'].append(row['CURRENT ACTIVITY AMT'])
            account_raw_data['Quantity'].append(row['PERF COUNT'])
            account_raw_data['Total'].append(row['ROYALTY AMOUNT'])
            account_raw_data['Label Share Net Receipts'].append(row['ROYALTY AMOUNT'])


            new_account = Accounting( period = row['PERIOD'], activity_period = row['PERF PERIOD'], retailer = row['PERF SOURCE'], territory = row['COUNTRY OF PERFORMANCE'], orchard_UPC = row['TITLE #'], manufacturer_UPC = row['TITLE #'], project_code = row['SHOW NAME'], product_code = row['EPISODE NAME'], imprint_label = row['LABEL IMPRINT'], artist_name = row['ARTISTS '], product_name = row['TITLE NAME'], track_name = row['TITLE NAME'], track_artist = row['ARTISTS '], isrc = row['TITLE #'], volume = row['SHOW #'], unit_price = row['CURRENT ACTIVITY AMT'], discount = row['WITHHOLD'], actual_price = row['CURRENT ACTIVITY AMT'], quantity = row['PERF COUNT'], total = row['ROYALTY AMOUNT'], label_share_net_receipts = row['ROYALTY AMOUNT'], vendor="BMI", processed_day=today)
            new_account.save()

        print(account_raw_data) 
        new_account_df = pd.DataFrame({key:pd.Series(value) for key, value in account_raw_data.items() }, columns=account_col)
        # trk_name = f'processed_horus_{today}_track.csv'
        track_account = new_account_df.to_csv('processed_bmi_account.csv', index = False, header=True)
        new_account_document = ProcessedAccountFile()
        new_account_document.document = bmi_file
        new_account_document.file_name = 'processed_bmi_account.csv'
        new_account_document.save()

        with open('processed_bmi_account.csv', 'rb') as csv:
            new_account_document.file_doc.save('processed_bmi_account.csv', File(csv))

        send_mail(
                        'File Processed',
                        f''' 
                        Hello Admin 
                        The file {bmi_file.file_name} has been successfully processed !
                        Congratulations
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except (ValueError, NameError, TypeError) as error:
        err_msg = str(error)
        print(err_msg)
        send_mail(
                        'Error Processing File',
                        f''' 
                        Hello Admin 
                        The file {bmi_file.file_name} is corrupt!
                        See the details below:
                        {err_msg}
                            ''',
                        'Trackeet Recording Panel ',
                        ['digger@wyldpytch.com','shola.albert@gmail.com'],
                        fail_silently=False,)
    except:
        print("Unexpected error!")
        raise




############################
#     WARNER ADA           #
############################
def process_warner(filename):
    warner_file = ProcessedDocument.objects.filter(file_name=filename).first()