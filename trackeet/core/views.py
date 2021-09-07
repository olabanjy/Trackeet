from datetime import date
import xlwt, csv, pandas as pd
from django.shortcuts import render, reverse
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.core.exceptions import ObjectDoesNotExist
from django.contrib import messages
from django.views.generic import  View
from django.urls import reverse
import spotipy
import spotipy.oauth2 as oauth2
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
from .forms import *
from .models import *
from .tasks_new import *

#
#
#
#
#
#
today = date.today()
############################
#     DASHBOARD VIEW       #
############################
def dashboard(request):
    template = 'core/index.html'
    track_count = Track.objects.all().count()
    album_count = Album.objects.all().count()
    master_count = Master_DRMSYS.objects.all().count()
    docs = ProcessedDocument.objects.all()
    context = {
        'track_count': track_count,
        'album_count': album_count,
        'master_count': master_count,
        'docs': docs
    }
    return render(request, template, context)
############################
#     ALBUM SEARCH         #
############################
def search_album(request):
    if request.method == "GET":
        product_title = request.GET['product_title']
        product_upc = request.GET['product_upc']
        genre = request.GET['genre']
        product_artist = request.GET['product_artist']
        if product_title is not None and product_title != u"":
            product_title = request.GET['product_title']
            album = Album.objects.filter(product_title__icontains= product_title)
        elif genre is not None and genre != u"":
            genre = request.GET['genre']
            album = Album.objects.filter(genre__icontains= genre)
        elif product_upc is not None and product_upc != u"":
            product_upc = request.GET['product_upc']
            album = Album.objects.filter(product_upc__icontains= product_upc)
        elif product_artist is not None and product_artist != u"":
            product_artist = request.GET['product_artist']
            album = Album.objects.filter(product_artist__icontains= product_artist)
        else:
            album = []
        return render(request, 'core/album_search_result.html', {'album':album})
############################
#     TRACK SEARCH         #
############################
def search_track(request):
    if request.method == "GET":
        display_upc = request.GET['display_upc']
        title = request.GET['title']
        genre = request.GET['genre']
        isrc = request.GET['isrc']
        p_line = request.GET['p_line']
        recording_artist = request.GET['recording_artist']
        artist = request.GET['artist']
        release_name = request.GET['release_name']
        label_name = request.GET['label_name']
        producer = request.GET['producer']
        publisher = request.GET['publisher']
        writer = request.GET['writer']
        arranger = request.GET['arranger']
        territories = request.GET['territories']
        if display_upc is not None and display_upc != u"":
            display_upc = request.GET['display_upc']
            track = Track.objects.filter(display_upc__icontains= display_upc)
        elif title is not None and title != u"":
            title = request.GET['title']
            track = Track.objects.filter(title__icontains= title)
        elif release_name is not None and release_name != u"":
            release_name = request.GET['release_name']
            track = Track.objects.filter(release_name__icontains= release_name)
        elif genre is not None and genre != u"":
            genre = request.GET['genre']
            track = Track.objects.filter(genre__icontains= genre)
        elif territories is not None and territories != u"":
            territories = request.GET['territories']
            track = Track.objects.filter(territories__icontains= territories)
        elif arranger is not None and arranger != u"":
            arranger = request.GET['arranger']
            track = Track.objects.filter(arranger__icontains= arranger)
        elif publisher is not None and publisher != u"":
            publisher = request.GET['publisher']
            track = Track.objects.filter(publisher__icontains= publisher)
        elif producer is not None and producer != u"":
            producer = request.GET['producer']
            track = Track.objects.filter(producer__icontains= producer)
        elif label_name is not None and label_name != u"":
            label_name = request.GET['label_name']
            track = Track.objects.filter(label_name__icontains= label_name)
        elif isrc is not None and isrc != u"":
            isrc = request.GET['isrc']
            track = Track.objects.filter(isrc__icontains= isrc)
        elif p_line is not None and p_line != u"":
            p_line = request.GET['p_line']
            track = Track.objects.filter(p_line__icontains= p_line)
        else:
            track = []
        return render(request, 'core/track_search_result.html', {'track':track})
############################
#  MASTER DRMSYS SEARCH    #
############################
def search_master(request):
    if request.method == "GET":
        display_upc = request.GET['display_upc']
        title = request.GET['title']
        genre = request.GET['genre']
        isrc = request.GET['isrc']
        p_line = request.GET['p_line']
        recording_artist = request.GET['recording_artist']
        artist = request.GET['artist']
        release_name = request.GET['release_name']
        label_name = request.GET['label_name']
        producer = request.GET['producer']
        publisher = request.GET['publisher']
        writer = request.GET['writer']
        arranger = request.GET['arranger']
        territories = request.GET['territories']
        if display_upc is not None and display_upc != u"":
            display_upc = request.GET['display_upc']
            master = Master_DRMSYS.objects.filter(display_upc__icontains= display_upc)
        elif title is not None and title != u"":
            title = request.GET['title']
            master = Master_DRMSYS.objects.filter(title__icontains= title)
            # track = []
        elif arranger is not None and arranger != u"":
            arranger = request.GET['arranger']
            master = Master_DRMSYS.objects.filter(arranger__icontains= arranger)
        elif territories is not None and territories != u"":
            territories = request.GET['territories']
            master = Master_DRMSYS.objects.filter(territories__icontains= territories)
        elif writer is not None and writer != u"":
            writer = request.GET['writer']
            master = Master_DRMSYS.objects.filter(writer__icontains= writer)
        elif genre is not None and genre != u"":
            genre = request.GET['genre']
            master = Master_DRMSYS.objects.filter(genre__icontains= genre)
        elif artist is not None and genre != u"":
            artist = request.GET['artist']
            master = Master_DRMSYS.objects.filter(artist__icontains= artist)
        elif recording_artist is not None and recording_artist != u"":
            recording_artist = request.GET['recording_artist']
            master = Master_DRMSYS.objects.filter(recording_artist__icontains= recording_artist)
        elif isrc is not None and isrc != u"":
            isrc = request.GET['isrc']
            master = Master_DRMSYS.objects.filter(isrc__icontains= isrc)
        elif p_line is not None and p_line != u"":
            p_line = request.GET['p_line']
            master = Master_DRMSYS.objects.filter(p_line__icontains= p_line)
        elif producer is not None and producer != u"":
            producer = request.GET['producer']
            master = Master_DRMSYS.objects.filter(producer__icontains= producer)
        elif publisher is not None and publisher != u"":
            publisher = request.GET['publisher']
            master = Master_DRMSYS.objects.filter(publisher__icontains= publisher)
        elif label_name is not None and label_name != u"":
            label_name = request.GET['label_name']
            master = Master_DRMSYS.objects.filter(label_name__icontains= label_name)
        else:
            master = []
        return render(request, 'core/master_search_result.html', {'master':master})
############################
#     EXPORT TRACK       #
############################
def export_track_xls(request):
    response = HttpResponse(content_type='application/ms-excel')
    f_name = f"track_export_{today}.xls"
    response['Content-Disposition'] = f'attachment; filename={f_name}'
    ####
    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('Track')
    ####
    row_num = 0
    ####
    font_style = xlwt.XFStyle()
    font_style.font.bold = True
    ####
    columns = ['DisplayUPC', 'VolumeNo', 'TrackNo',  'HiddenTrack',' Title', 'SongVersion', 'Genre',
    'ISRC',  'TrackDuration', 'PreviewClipStartTime', 'PreviewClipDuration', 'P_LINE',
    'Recording Artist', 'Artist', 'Release Name', 'ParentalAdvisory', 'LabelName', 'Producer',
    ' Publisher', 'Writer', 'Arranger', 'Territories', 'Exclusive', 'WholesalePrice', ' Download',
    'SalesStartDate',  'SalesEndDate', 'Error', 'Processed Day', ]
    ###
    for col_num in range(len(columns)):
        ws.write(row_num, col_num, columns[col_num], font_style)
    font_style = xlwt.XFStyle()
    rows = Track.objects.all().values_list( 'display_upc', 'volume_no', 'track_no', 'hidden_track',
    'title','song_version', 'genre', 'isrc', 'track_duration', 'preview_clip_start_time',
    'preview_clip_duration', 'p_line', 'recording_artist', 'artist', 'release_name',
    'parental_advisory','label_name', 'producer', 'publisher', 'writer', 'arranger',
    'territories','exclusive','wholesale_price', 'download','sales_start_date','sales_end_date',
    'error','processed_day')
    for row in rows:
        row_num += 1
        for col_num in range(len(row)):
            ws.write(row_num, col_num, row[col_num], font_style)
    wb.save(response)
    return response
######
def export_track_csv(request):
    response = HttpResponse(content_type='text/csv')
    f_name = f"track_export_{today}.csv"
    response['Content-Disposition'] = f'attachment; filename={f_name}'
    writer = csv.writer(response)
    ######
    writer.writerow(['DisplayUPC', 'VolumeNo', 'TrackNo',  'HiddenTrack',' Title', 'SongVersion',
     'Genre', 'ISRC',  'TrackDuration', 'PreviewClipStartTime', 'PreviewClipDuration', 'P_LINE',
     'Recording Artist', 'Artist', 'Release Name', 'ParentalAdvisory', 'LabelName', 'Producer',
      ' Publisher', 'Writer', 'Arranger', 'Territories', 'Exclusive', 'WholesalePrice',
      ' Download', 'SalesStartDate',  'SalesEndDate', 'Error', 'Processed Day', ])
    ####
    rows = Track.objects.all().values_list( 'display_upc', 'volume_no', 'track_no', 'hidden_track',
     'title','song_version', 'genre', 'isrc', 'track_duration', 'preview_clip_start_time',
     'preview_clip_duration', 'p_line', 'recording_artist', 'artist', 'release_name',
      'parental_advisory','label_name', 'producer', 'publisher', 'writer', 'arranger',
      'territories','exclusive','wholesale_price', 'download','sales_start_date',
      'sales_end_date','error','processed_day')
    for row in rows:
        writer.writerow(row)
    return response
############################
#     EXPORT ALBUM       #
############################
def export_album_xls(request):
    response = HttpResponse(content_type='application/ms-excel')
    f_name = f"album_export_{today}.xls"
    response['Content-Disposition'] = f'attachment; filename={f_name}'

    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('Album')
    row_num = 0
    font_style = xlwt.XFStyle()
    font_style.font.bold = True
    columns = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title', 'Product Artist',
     'Genre', 'CLINE', 'Release Date', 'Imprint Label',  'Artist URL', 'Catalog Number',
     'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts','Processed Day',]
    for col_num in range(len(columns)):
        ws.write(row_num, col_num, columns[col_num], font_style)
    font_style = xlwt.XFStyle()
    rows = Album.objects.all().values_list('product_upc', 'total_volume', 'total_tracks',
     'product_title', 'product_artist', 'genre', 'cline', 'release_date', 'imprint_label',
      'artist_url', 'catalog_number', 'manufacture_upc', 'deleted', 'territories_carveouts',
      'master_carveouts', 'processed_day' )
    for row in rows:
        row_num += 1
        for col_num in range(len(row)):
            ws.write(row_num, col_num, row[col_num], font_style)
    wb.save(response)
    return response
###########
def export_album_csv(request):
    response = HttpResponse(content_type='text/csv')
    f_name = f"album_export_{today}.csv"
    response['Content-Disposition'] = f'attachment; filename={f_name}'
    writer = csv.writer(response)
    writer.writerow(['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title',
    'Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label',  'Artist URL',
     'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts',
      'Master Carveouts','Processed Day',])
    rows = Album.objects.all().values_list('product_upc', 'total_volume', 'total_tracks',
     'product_title', 'product_artist', 'genre', 'cline', 'release_date', 'imprint_label', 
     'artist_url', 'catalog_number', 'manufacture_upc', 'deleted', 'territories_carveouts',
     'master_carveouts', 'processed_day' )
    for row in rows:
        writer.writerow(row)
    return response
############################
#     EXPORT DRMSYS        #
############################
def export_drmsys_xls(request):
    response = HttpResponse(content_type='application/ms-excel')
    f_name = f"master_drmsys_export_{today}.xls"
    response['Content-Disposition'] = f'attachment; filename={f_name}'
    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('Master DRMSYS')
    row_num = 0
    font_style = xlwt.XFStyle()
    font_style.font.bold = True
    columns = [ 'Arranger', 'Artist', 'ArtistURL', 'BPM', 'C_Line', 'DataType', 'Deleted?',
     'DisplayUPC', 'Download', 'Error', 'Exclusive', 'Genre', 'GenreAlt', 'GenreSub',
      'GenreSubAlt', 'HiddenTrack', 'ISRC', 'ItunesLink', 'LabelCatalogNo', 'LabelName',
       'Language', 'ManufacturerUPC', 'MasterCarveouts', 'P_Line', 'ParentalAdvisory',
       'PreviewClipDuration', 'PreviewClipStartTime', 'PriceBand', 'Producer', 'Publisher',
       'RecordingArtist', 'ReleaseDate', 'ReleaseName', 'RoyaltyRate', 'SalesEndDate',
       'SalesStartDate', 'SongVersion', 'Territories', 'TerritoriesCarveouts', 'Title',
       'TotalTracks', 'TotalVolumes', 'TrackDuration', 'TrackNo', 'VendorName', 'VolumeNo',
       'WholesalePrice', 'Writer', 'zIDKey_UPCISRC', 'Processed Day', ]
    for col_num in range(len(columns)):
        ws.write(row_num, col_num, columns[col_num], font_style)
    font_style = xlwt.XFStyle()
    rows = Master_DRMSYS.objects.all().values_list('arranger', 'artist', 'artist_url', 'bpm',
    'c_line', 'data_type', 'deleted', 'display_upc', 'download', 'error', 'exclusive', 'genre',
     'genre_alt', 'genre_sub', 'genre_subalt', 'hidden_track', 'isrc', 'itunes_link',
     'label_catalogno', 'label_name', 'language', 'manufacturer_upc', 'master_carveouts', 'p_line',
     'parental_advisory', 'preview_clip_duration', 'preview_clip_start_time', 'price_band',
      'producer', 'publisher', 'recording_artist', 'release_date', 'release_name', 'royalty_rate',
       'sales_end_date', 'sales_start_date', 'song_version', 'territories', 'territories_carveouts',
        'title', 'total_tracks', 'total_volumes', 'track_duration', 'track_no', 'vendor_name',
         'volume_no', 'wholesale_price', 'writer', 'zIDKey_UPCISRC', 'processed_day')
    for row in rows:
        row_num += 1
        for col_num in range(len(row)):
            ws.write(row_num, col_num, row[col_num], font_style)
    wb.save(response)
    return response
###############
def export_drmsys_csv(request):
    response = HttpResponse(content_type='text/csv')
    f_name = f"master_drmsys_export_{today}.csv"
    response['Content-Disposition'] = f'attachment; filename={f_name}'
    writer = csv.writer(response)
    writer.writerow([ 'Arranger', 'Artist', 'ArtistURL', 'BPM', 'C_Line', 'DataType',
     'Deleted?', 'DisplayUPC', 'Download', 'Error', 'Exclusive', 'Genre', 'GenreAlt',
     'GenreSub', 'GenreSubAlt', 'HiddenTrack', 'ISRC', 'ItunesLink', 'LabelCatalogNo',
     'LabelName', 'Language', 'ManufacturerUPC', 'MasterCarveouts', 'P_Line', 'ParentalAdvisory',
      'PreviewClipDuration', 'PreviewClipStartTime', 'PriceBand', 'Producer', 'Publisher',
      'RecordingArtist', 'ReleaseDate', 'ReleaseName', 'RoyaltyRate', 'SalesEndDate',
       'SalesStartDate', 'SongVersion', 'Territories', 'TerritoriesCarveouts', 'Title',
        'TotalTracks', 'TotalVolumes', 'TrackDuration', 'TrackNo', 'VendorName', 'VolumeNo',
         'WholesalePrice', 'Writer', 'zIDKey_UPCISRC', 'Processed Day', ])
         ####
    rows = Master_DRMSYS.objects.all().values_list('arranger', 'artist', 'artist_url',
     'bpm', 'c_line', 'data_type', 'deleted', 'display_upc', 'download', 'error', 'exclusive',
     'genre', 'genre_alt', 'genre_sub', 'genre_subalt', 'hidden_track', 'isrc',
      'itunes_link', 'label_catalogno', 'label_name', 'language', 'manufacturer_upc',
       'master_carveouts', 'p_line', 'parental_advisory', 'preview_clip_duration',
        'preview_clip_start_time', 'price_band', 'producer', 'publisher', 'recording_artist',
         'release_date', 'release_name', 'royalty_rate', 'sales_end_date', 'sales_start_date',
          'song_version', 'territories', 'territories_carveouts', 'title', 'total_tracks',
          'total_volumes', 'track_duration', 'track_no', 'vendor_name', 'volume_no',
          'wholesale_price', 'writer', 'zIDKey_UPCISRC', 'processed_day')
    for row in rows:
        writer.writerow(row)
    return response
##############
#############
###########
def export_accounting_xls(request):
    response = HttpResponse(content_type='application/ms-excel')
    f_name = f"master_accounting_export_{today}.xls"
    response['Content-Disposition'] = f'attachment; filename={f_name}'
    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('General Accounting Format')
    row_num = 0
    font_style = xlwt.XFStyle()
    font_style.font.bold = True
    columns = [ 'period', 'activity_period','retailer','territory','orchard_UPC',
    'manufacturer_UPC','project_code','product_code','subaccount','imprint_label',
    'artist_name','product_name','track_name','track_artist','isrc','volume','track',
    'trans_type','trans_type_description','unit_price','discount','actual_price','quantity',
    'total','adjusted_total','split_rate','label_share_net_receipts','ringtone_publishing',
    'cloud_publishing','publishing','mech_administrative_fee',
    'preferred_currency','vendor','processed_day', ]
    for col_num in range(len(columns)):
        ws.write(row_num, col_num, columns[col_num], font_style)
    font_style = xlwt.XFStyle()
    rows = Accounting.objects.all().values_list('period', 'activity_period','retailer','territory',
    'orchard_UPC','manufacturer_UPC','project_code','product_code','subaccount','imprint_label',
    'artist_name','product_name','track_name','track_artist','isrc','volume','track',
    'trans_type','trans_type_description','unit_price','discount','actual_price','quantity',
    'total','adjusted_total','split_rate','label_share_net_receipts',
    'ringtone_publishing','cloud_publishing','publishing','mech_administrative_fee',
    'preferred_currency','vendor','processed_day')
    for row in rows:
        row_num += 1
        for col_num in range(len(row)):
            ws.write(row_num, col_num, row[col_num], font_style)
    wb.save(response)
    return response
#############
def export_accounting_csv(request):
    response = HttpResponse(content_type='text/csv')
    f_name = f"master_accounting_export_{today}.csv"
    response['Content-Disposition'] = f'attachment; filename={f_name}'
    writer = csv.writer(response)
    writer.writerow([  'period', 'activity_period','retailer','territory','orchard_UPC',
    'manufacturer_UPC','project_code','product_code','subaccount','imprint_label',
    'artist_name','product_name','track_name','track_artist','isrc','volume','track',
    'trans_type','trans_type_description','unit_price','discount','actual_price','quantity',
    'total','adjusted_total','split_rate','label_share_net_receipts','ringtone_publishing',
    'cloud_publishing','publishing','mech_administrative_fee',
    'preferred_currency','vendor','processed_day', ])
    rows = Accounting.objects.all().values_list('period', 'activity_period','retailer','territory',
    'orchard_UPC','manufacturer_UPC','project_code','product_code','subaccount','imprint_label',
    'artist_name','product_name','track_name','track_artist','isrc','volume','track','trans_type',
    'trans_type_description','unit_price','discount','actual_price','quantity','total',
    'adjusted_total','split_rate','label_share_net_receipts','ringtone_publishing',
    'cloud_publishing','publishing','mech_administrative_fee','preferred_currency',
    'vendor','processed_day')
    for row in rows:
        writer.writerow(row)
    return response
############################
#     ALBUM VIEW           #
############################
class AlbumView(View):
    def get(self, *args, **kwargs):
        template = 'core/album.html'
        album = Album.objects.all().order_by('-updated_time')[:5000]
        form = AddAlbumRecord()
        context = {
            'album': album, 
            'page_obj': album,
            'form': form,   
        }
        return render(self.request,template, context)
        ######
    def post(self, *args, **kwargs):
        form = AddAlbumRecord(self.request.POST or None)
        try:
            if form.is_valid():
                product_title = form.cleaned_data.get('product_title')
                product_upc = form.cleaned_data.get('product_upc')
                genre= form.cleaned_data.get('genre')
                release_date = form.cleaned_data.get('release_date')
                total_volume = form.cleaned_data.get('total_volume')
                imprint_label = form.cleaned_data.get('imprint_label')
                product_artist = form.cleaned_data.get('product_artist')
                artist_url = form.cleaned_data.get('artist_url')
                catalog_number = form.cleaned_data.get('catalog_number')
                manufacture_upc = form.cleaned_data.get('manufacture_upc')
                deleted = form.cleaned_data.get('deleted')
                total_tracks = form.cleaned_data.get('total_tracks')
                cline = form.cleaned_data.get('cline')
                territories_carveouts = form.cleaned_data.get('territories_carveouts')
                master_carveouts = form.cleaned_data.get('master_carveouts')
                check_album = Album.objects.filter(product_upc=product_upc).first()
                if check_album:
                    messages.info(self.request, "Album record already exist!")
                    return HttpResponseRedirect(reverse("core:album-view"))
                else:
                    new_album = Album(product_title = product_title, product_upc = product_upc,
                     genre = genre, release_date = release_date, total_volume = total_volume,
                      imprint_label = imprint_label, product_artist = product_artist, 
                      artist_url =artist_url, catalog_number = catalog_number,
                       manufacture_upc = manufacture_upc, deleted = deleted, 
                       total_tracks = total_tracks, cline = cline,
                        territories_carveouts = territories_carveouts,
                         master_carveouts = master_carveouts, processed_day = today)
                    new_album.save()
                    new_master = Master_DRMSYS(title = new_album.product_title, display_upc = new_album.product_upc, genre = new_album.genre, release_date = new_album.release_date, total_volumes = new_album.total_volume, label_name = f"{new_album.imprint_label}", artist = new_album.product_artist, artist_url = new_album.artist_url, label_catalogno = new_album.catalog_number, manufacturer_upc = new_album.manufacture_upc, deleted = new_album.deleted, total_tracks = new_album.total_tracks, c_line = new_album.cline, territories_carveouts = new_album.territories_carveouts, master_carveouts= new_album.master_carveouts, album = new_album, data_type = 'Album', zIDKey_UPCISRC = f"{new_album.product_upc}-{new_album.product_upc}", processed_day=today)
                    new_master.save()
            messages.info(self.request, "The album record has been added")
            return HttpResponseRedirect(reverse("core:album-view"))
        except ObjectDoesNotExist:
            pass
##########
def update_album(request):
    album_id = request.POST.get('id','')
    album_type = request.POST.get('type','')
    album_value = request.POST.get('value','')
    album = Album.objects.get(id=album_id)
    master = Master_DRMSYS.objects.get(data_type="Album", album=album)
    if album_type == 'product_title':
        album.product_title = album_value
        master.title = album_value
    if album_type == 'genre':
        album.genre = album_value
        master.genre = album_value
    if album_type == 'release_date':
        album.release_date = album_value
        master.release_date = album_value
    if album_type == 'total_volume':
        album.total_volume = album_value
        master.total_volumes = album_value
    if album_type == 'imprint_label':
        album.imprint_label = album_value
        master.label_name = album_value
    if album_type == 'product_artist':
        album.product_artist = album_value
        master.artist = album_value
    if album_type == 'artist_url':
        album.artist_url = album_value
        master.artist_url = album_value
    if album_type == 'catalog_number':
        album.catalog_number = album_value
    album.save()
    master.save()
    return JsonResponse({"success":"Updated"})
############################
#     TRACK VIEW           #
############################
class TrackView(View):
    def get(self, *args, **kwargs):
        template = "core/track.html"
        track = Track.objects.all().order_by('-updated_time')[:5000]
        form = AddTrackRecord()
        context = {
            'track': track,
            'page_obj': track, 
            'form':form
           
        }
        return render(self.request,template,context)
    #####
    def post(self, *args, **kwargs):
        form = AddTrackRecord(self.request.POST or None)
        try:
            if form.is_valid():
                display_upc = form.cleaned_data.get('display_upc')
                volume_no = form.cleaned_data.get('volume_no')
                track_no = form.cleaned_data.get('track_no')
                hidden_track = form.cleaned_data.get('hidden_track')
                title = form.cleaned_data.get('title')
                song_version = form.cleaned_data.get('song_version')
                genre = form.cleaned_data.get('genre')
                isrc = form.cleaned_data.get('isrc')
                track_duration = form.cleaned_data.get('track_duration')
                preview_clip_start_time = form.cleaned_data.get('preview_clip_start_time')
                preview_clip_duration = form.cleaned_data.get('preview_clip_duration')
                p_line = form.cleaned_data.get('p_line')
                recording_artist = form.cleaned_data.get('recording_artist')
                artist = form.cleaned_data.get('artist')
                release_name = form.cleaned_data.get('release_name')
                parental_advisory = form.cleaned_data.get('parental_advisory')
                label_name = form.cleaned_data.get('label_name')
                producer = form.cleaned_data.get('producer')
                publisher = form.cleaned_data.get('publisher')
                writer = form.cleaned_data.get('writer')
                arranger = form.cleaned_data.get('arranger')
                territories = form.cleaned_data.get('territories')
                exclusive = form.cleaned_data.get('exclusive')
                wholesale_price = form.cleaned_data.get('wholesale_price')
                download = form.cleaned_data.get('download')
                sales_start_date = form.cleaned_data.get('sales_start_date')
                sales_end_date = form.cleaned_data.get('sales_end_date')
                error = form.cleaned_data.get('error')
                check_track = Track.objects.filter(isrc=isrc).first()
                if check_track:
                    messages.info(self.request, "The track record you are trying to add already exist!")
                    return HttpResponseRedirect(reverse("core:track-view"))
                else:
                    check_album = Album.objects.filter(product_upc = display_upc).first()
                    if check_album:
                        new_track = Track(album = check_album, display_upc = display_upc,volume_no = volume_no,
                         track_no = track_no, hidden_track = hidden_track, title = title,
                         song_version = song_version, genre = genre, isrc = isrc, track_duration = track_duration,
                          preview_clip_start_time = preview_clip_start_time,
                          preview_clip_duration = preview_clip_duration, p_line = p_line,
                           recording_artist = recording_artist, artist= artist, release_name = release_name,
                            parental_advisory = parental_advisory, label_name = label_name, producer = producer,
                             publisher= publisher, writer= writer, arranger = arranger,
                             territories = territories, exclusive = exclusive, wholesale_price = wholesale_price,
                              download = download, sales_start_date = sales_start_date, sales_end_date = sales_end_date,
                               error = error, processed_day = today)
                        new_track.save()
                        new_master = Master_DRMSYS(arranger = new_track.arranger, artist = new_track.artist,
                         data_type = "Track", display_upc = new_track.display_upc, download = new_track.download,
                          error = new_track.error, exclusive = new_track.exclusive, genre = new_track.genre,
                           hidden_track = new_track.hidden_track, isrc = new_track.isrc, label_name = new_track.label_name,
                            p_line = new_track.p_line, parental_advisory = new_track.parental_advisory,
                             preview_clip_duration = new_track.preview_clip_duration,
                             preview_clip_start_time = new_track.preview_clip_start_time,
                             producer = new_track.producer, publisher = new_track.publisher,
                             recording_artist = new_track.recording_artist, release_name = new_track.release_name,
                             sales_end_date = new_track.sales_end_date, sales_start_date = new_track.sales_start_date,
                              song_version = new_track.song_version, territories = new_track.territories,
                               title = new_track.title, track_duration = new_track.track_duration,
                                track_no = new_track.track_no, volume_no = new_track.volume_no,
                                 wholesale_price = new_track.wholesale_price,  writer = new_track.writer,
                                  zIDKey_UPCISRC = f"{check_album.product_upc}-{new_track.isrc}",
                                  track = new_track , processed_day=today)
                        new_master.save()
                    else:
                        new_album = Album(product_upc= display_upc, product_artist = artist, processed_day = today)
                        new_album.save()
                        new_track = Track(album = new_album, display_upc = display_upc, volume_no = volume_no, track_no = track_no,
                         hidden_track = hidden_track, title = title,song_version = song_version, genre = genre,
                          isrc = isrc, track_duration = track_duration, preview_clip_start_time = preview_clip_start_time,
                           preview_clip_duration = preview_clip_duration, p_line = p_line,
                            recording_artist = recording_artist, artist= artist, release_name = release_name,
                             parental_advisory = parental_advisory, label_name = label_name, producer = producer,
                              publisher= publisher, writer= writer, arranger = arranger, territories = territories,
                               exclusive = exclusive, wholesale_price = wholesale_price, download = download,
                                sales_start_date = sales_start_date, sales_end_date = sales_end_date, error = error,
                                 processed_day = today)
                        new_track.save()
                        new_master = Master_DRMSYS(arranger = new_track.arranger, artist = new_track.artist,
                         data_type = "Track", display_upc = new_track.display_upc,
                          download = new_track.download, error = new_track.error,
                           exclusive = new_track.exclusive, genre = new_track.genre,
                            hidden_track = new_track.hidden_track, isrc = new_track.isrc,
                             label_name = new_track.label_name, p_line = new_track.p_line,
                              parental_advisory = new_track.parental_advisory,
                               preview_clip_duration = new_track.preview_clip_duration,
                                preview_clip_start_time = new_track.preview_clip_start_time,
                                 producer = new_track.producer, publisher = new_track.publisher,
                                  recording_artist = new_track.recording_artist,
                                   release_name = new_track.release_name,
                                    sales_end_date = new_track.sales_end_date,
                                     sales_start_date = new_track.sales_start_date,
                                      song_version = new_track.song_version,
                                       territories = new_track.territories, title = new_track.title,
                                        track_duration = new_track.track_duration,
                                         track_no = new_track.track_no, volume_no = new_track.volume_no,
                                           wholesale_price = new_track.wholesale_price,
                                             writer = new_track.writer, zIDKey_UPCISRC = f"{new_album.product_upc}-{new_track.isrc}" , track = new_track , processed_day=today)
                        new_master.save()
            messages.info(self.request, "The track record has been added") 
            return HttpResponseRedirect(reverse("core:track-view"))
        except ObjectDoesNotExist:
            pass          
#############
############
def update_track(request):
    track_id = request.POST.get('id','')
    track_type = request.POST.get('type','')
    track_value = request.POST.get('value','')
    track = Track.objects.get(id=track_id)
    master = Master_DRMSYS.objects.get(data_type="Track", track=track)
    if track_type == 'volume_no':
        track.volume_no = track_value
        master.volume_no = track_value
    if track_type == 'track_no':
        track.track_no = track_value
        master.track_no = track_value
    if track_type == 'hidden_track':
        track.hidden_track = track_value
        master.hidden_track = track_value
    if track_type == 'title':
        track.title = track_value
        master.title = track_value
    if track_type == 'song_version':
        track.song_version = track_value
        master.song_version = track_value
    if track_type == 'genre':
        track.genre = track_value
        master.genre = track_value
    if track_type == 'p_line':
        track.p_line = track_value
        master.p_line = track_value
    if track_type == 'recording_artist':
        track.recording_artist = track_value
        master.recording_artist = track_value
    if track_type == 'artist':
        track.artist = track_value
        master.artist = track_value
    if track_type == 'release_name':
        track.release_name = track_value
        master.release_name = track_value
    if track_type == 'parental_advisory':
        track.parental_advisory = track_value
        master.parental_advisory = track_value
    if track_type == 'label_name':
        track.label_name = track_value
        master.label_name = track_value
    if track_type == 'producer':
        track.producer = track_value
        master.producer = track_value
    if track_type == 'publisher':
        track.publisher = track_value
        master.publisher = track_value
    if track_type == 'writer':
        track.writer = track_value
        master.writer = track_value
    if track_type == 'arranger':
        track.arranger = track_value
        master.arranger = track_value
    if track_type == 'territories':
        track.territories = track_value
        master.territories = track_value
    if track_type == 'exclusive':
        track.exclusive = track_value
        master.exclusive = track_value
    if track_type == 'wholesale_price':
        track.wholesale_price = track_value
        master.wholesale_price = track_value
    if track_type == 'download':
        track.download = track_value
        master.download = track_value
    track.save()
    master.save()
    return JsonResponse({"success":"Updated"})
############################
#     MASTER DRMSYS        #
############################

class MasterView(View):
    def get(self, *args, **kwargs):
        template = "core/master_drmsys.html"
        master = Master_DRMSYS.objects.all().order_by('-updated_time')[:5000]
        context = {
            'master': master
        }
        return render(self.request,template,context)
############################
#     BMI                  #
############################
class VendorBMI(View):
    def get(self, *args, **kwargs):
        template = 'core/bmi.html'
        metadata_form = UploadBMIAccountFormMetadata()
        accounting_form = UploadBMIAccountFormAccounting()
        docs = ProcessedDocument.objects.filter(which_doc="BMI").all()
        records = Accounting.objects.filter(vendor="BMI").all()[:400]
        context = {
            'metadata_form': metadata_form,
            'accounting_form': accounting_form,
            'docs':docs,
            'records':records
        }
        return render(self.request,template, context)
    #######
    def post(self, *args, **kwargs):
        metadata_form = UploadBMIAccountFormMetadata(self.request.POST, self.request.FILES or None)
        accounting_form = UploadBMIAccountFormAccounting(self.request.POST, self.request.FILES or None)
        try:
            # if metadata_form.is_valid():
            #     bmi_metadata_file = metadata_form.cleaned_data['bmi_metadata_file']
            #     if not bmi_metadata_file.name.endswith('.xlsx'):
            #         messages.info(self.request, "Incorrect file format, File must be a xlsx format")
            #         return HttpResponseRedirect(reverse("core:bmi"))
            #     processed_doc = ProcessedDocument.objects.filter(file_name=bmi_metadata_file.name).first()
            #     if processed_doc:
            #         messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
            #         return HttpResponseRedirect(reverse("core:bmi"))
            #     else:
            #         new_processed_doc = ProcessedDocument(file_name=bmi_metadata_file.name,file_doc = bmi_metadata_file, which_doc = "BMI")
            #         new_processed_doc.save()
            #         process_bmi.delay(new_processed_doc.file_name)
            #         messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
            #     return HttpResponseRedirect(reverse("core:bmi"))
            if accounting_form.is_valid():
                bmi_accounting_file = accounting_form.cleaned_data['bmi_accounting_file']
                if not bmi_accounting_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:bmi"))
                processed_doc = ProcessedDocument.objects.filter(file_name=bmi_accounting_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:bmi"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=bmi_accounting_file.name,file_doc = bmi_accounting_file, which_doc = "BMI")
                    new_processed_doc.save()
                    process_bmi_account.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:bmi"))
            else:  
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:bmi"))
        except ObjectDoesNotExist:
            pass
#############
def update_bmi_account(request):
    record_id = request.POST.get('id','')
    record_type = request.POST.get('type','')
    record_value = request.POST.get('value','')
    record = Accounting.objects.get(id=record_id)
    if record_type == "period":
        record.period = record_value
    if record_type == "activity_period":
        record.activity_period = record_value
    if record_type == "territory":
        record.territory = record_value
    if record_type == "orchard_UPC":
        record.orchard_UPC = record_value
    if record_type == "product_code":
        record.product_code = record_value
    if record_type == "artist_name":
        record.artist_name = record_value
    if record_type == "product_name":
        record.product_name = record_value
    if record_type == "track_name":
        record.track_name = record_value
    if record_type == "track_artist":
        record.track_artist = record_value
    if record_type == "unit_price":
        record.unit_price = record_value
    if record_type == "actual_price":
        record.actual_price = record_value
    if record_type == "quantity":
        record.quantity = record_value
    if record_type == "total":
        record.total = record_value
    if record_type == "label_share_net_receipts":
        record.label_share_net_receipts = record_value
    record.save()
    return JsonResponse({"success":"Updated"})
############################
#     DITTO                #
############################
class VendorDitto(View):
    def get(self, *args, **kwargs):
        template = 'core/ditto.html'
        accounting_form = UploadDittoAccountFormAccounting()
        metadata_form = UploadDittoAccountFormMetadata()
        docs = ProcessedDocument.objects.filter(which_doc="DITTO").all()
        records = Accounting.objects.filter(vendor="DITTO").all()[:400]
        context = {
            'metadata_form': metadata_form,
            'accounting_form': accounting_form,
            'docs':docs,
            'records':records
        }
        return render(self.request,template, context)
    ##########
    def post(self, *args, **kwargs):
        metadata_form = UploadDittoAccountFormMetadata(self.request.POST, self.request.FILES or None)
        accounting_form = UploadDittoAccountFormAccounting(self.request.POST, self.request.FILES or None)
        try:
            if metadata_form.is_valid():
                ditto_metadata_file = metadata_form.cleaned_data['ditto_metadata_file']
                if not ditto_metadata_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:ditto"))
                processed_doc = ProcessedDocument.objects.filter(file_name=ditto_metadata_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:ditto"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=ditto_metadata_file.name,file_doc = ditto_metadata_file, which_doc = "DITTO")
                    new_processed_doc.save()
                    process_ditto.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:ditto"))
            elif accounting_form.is_valid():
                ditto_account_file = accounting_form.cleaned_data['ditto_account_file']
                if not ditto_account_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:ditto"))
                processed_doc = ProcessedDocument.objects.filter(file_name=ditto_account_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:ditto"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=ditto_account_file.name,file_doc = ditto_account_file, which_doc = "DITTO")
                    new_processed_doc.save()
                    process_ditto_account.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:ditto"))
            else:  
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:ditto"))
        except ObjectDoesNotExist:
            pass     
#############
def update_ditto_account(request):
    record_id = request.POST.get('id','')
    record_type = request.POST.get('type','')
    record_value = request.POST.get('value','')
    record = Accounting.objects.get(id=record_id)
    if record_type == "period":
        record.period = record_value
    if record_type == "activity_period":
        record.activity_period = record_value
    if record_type == "territory":
        record.territory = record_value
    if record_type == "orchard_UPC":
        record.orchard_UPC = record_value
    if record_type == "product_code":
        record.product_code = record_value
    if record_type == "artist_name":
        record.artist_name = record_value
    if record_type == "product_name":
        record.product_name = record_value
    if record_type == "track_name":
        record.track_name = record_value
    if record_type == "track_artist":
        record.track_artist = record_value
    if record_type == "unit_price":
        record.unit_price = record_value
    if record_type == "actual_price":
        record.actual_price = record_value
    if record_type == "quantity":
        record.quantity = record_value
    if record_type == "total":
        record.total = record_value
    if record_type == "label_share_net_receipts":
        record.label_share_net_receipts = record_value
    record.save()
    return JsonResponse({"success":"Updated"})
############################
#     MCPS                 #
############################
class VendorMCPS(View): 
    def get(self, *args, **kwargs):
        template = 'core/mcps.html'
        metadata_form = UploadMCPSAccountFormMetadata()
        accounting_form = UploadMCPSAccountFormAccounting()
        docs = ProcessedDocument.objects.filter(which_doc="MCPS").all()
        records = Accounting.objects.filter(vendor="MCPS").all()[:400]
        context = {
            'metadata_form': metadata_form,
            'accounting_form': accounting_form,
            'docs':docs,
            'records':records
        }
        return render(self.request,template, context)
    def post(self, *args, **kwargs):
        metadata_form = UploadMCPSAccountFormMetadata(self.request.POST, self.request.FILES or None)
        accounting_form = UploadMCPSAccountFormAccounting(self.request.POST, self.request.FILES or None)
        try:
            # if metadata_form.is_valid():
            #     mcps_metadata_file = metadata_form.cleaned_data['mcps_metadata_file']
            #     if not mcps_metadata_file.name.endswith('.xlsx'):
            #         messages.info(self.request, "Incorrect file format, File must be a xlsx format")
            #         return HttpResponseRedirect(reverse("core:mcps"))
            #     processed_doc = ProcessedDocument.objects.filter(file_name=mcps_metadata_file.name).first()
            #     if processed_doc:
            #         messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
            #         return HttpResponseRedirect(reverse("core:mcps"))
            #     else:
            #         new_processed_doc = ProcessedDocument(file_name=mcps_metadata_file.name,file_doc = mcps_metadata_file, which_doc = "MCPS")
            #         new_processed_doc.save()
            #         process_mcps.delay(new_processed_doc.file_name)
            #         messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
            #     return HttpResponseRedirect(reverse("core:mcps"))
            if accounting_form.is_valid():
                mcps_account_file = accounting_form.cleaned_data['mcps_account_file']
                if not mcps_account_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:mcps"))
                processed_doc = ProcessedDocument.objects.filter(file_name=mcps_account_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:mcps"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=mcps_account_file.name,file_doc = mcps_account_file, which_doc = "MCPS")
                    new_processed_doc.save()
                    process_mcps_account.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:mcps"))
            else:  
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:mcps"))
        except ObjectDoesNotExist:
            pass
###############
def update_mcps_account(request):
    record_id = request.POST.get('id','')
    record_type = request.POST.get('type','')
    record_value = request.POST.get('value','')
    record = Accounting.objects.get(id=record_id)
    if record_type == "period":
        record.period = record_value
    if record_type == "activity_period":
        record.activity_period = record_value
    if record_type == "territory":
        record.territory = record_value
    if record_type == "orchard_UPC":
        record.orchard_UPC = record_value
    if record_type == "product_code":
        record.product_code = record_value
    if record_type == "artist_name":
        record.artist_name = record_value
    if record_type == "product_name":
        record.product_name = record_value
    if record_type == "track_name":
        record.track_name = record_value
    if record_type == "track_artist":
        record.track_artist = record_value
    if record_type == "discount":
        record.discount = record_value
    if record_type == "quantity":
        record.quantity = record_value
    if record_type == "total":
        record.total = record_value
    if record_type == "adjusted_total":
        record.adjusted_total = record_value
    record.save()
    return JsonResponse({"success":"Updated"})
############################
#     PPL                  #
############################
class VendorPPL(View):
    def get(self, *args, **kwargs):
        template = 'core/ppl.html'
        label_check = UploadPPLForLabelCheck()
        accounting_form = UploadPPLAccountFormAccounting()
        docs = ProcessedDocument.objects.filter(which_doc="PPL").all()
        records = Accounting.objects.filter(vendor="PPL").all()[:400]
        context = {
            'label_check': label_check,
            'accounting_form': accounting_form,
            'docs': docs,
            'records': records
        }
        return render(self.request,template, context)
    def post(self, *args, **kwargs):
        # label_check = UploadPPLForLabelCheck(self.request.POST, self.request.FILES or None)
        accounting_form = UploadPPLAccountFormAccounting(self.request.POST, self.request.FILES or None)
        try:
            # if label_check.is_valid():
            #     label_check_file = label_check.cleaned_data['ppl_file']
            #     if not label_check_file.name.endswith('.xlsx'):
            #         messages.info(self.request, "Incorrect file format, File must be a xlsx format")
            #         return HttpResponseRedirect(reverse("core:ppl"))
            #     processed_doc = ProcessedDocument.objects.filter(file_name=label_check_file.name).first()
            #     if processed_doc:
            #         messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
            #         return HttpResponseRedirect(reverse("core:ppl"))
            #     else:
            #         new_processed_doc = ProcessedDocument(file_name=label_check_file.name,file_doc = label_check_file, which_doc = "PPL")
            #         new_processed_doc.save()
            #         process_ppl_label(new_processed_doc.pk)
            #         messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
            #     return HttpResponseRedirect(reverse("core:ppl"))
            if accounting_form.is_valid():
                ppl_account_file = accounting_form.cleaned_data['ppl_account_file']
                if not ppl_account_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:ppl"))
                processed_doc = ProcessedDocument.objects.filter(file_name=ppl_account_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:ppl"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=ppl_account_file.name,file_doc = ppl_account_file, which_doc = "PPL")
                    new_processed_doc.save()
                    # process_ppl_account_test(new_processed_doc.pk)
                    # process_ppl_account(new_processed_doc.pk)
                    process_ppl_new_account.delay(new_processed_doc.pk)
                    process_ppl_artist.delay(new_processed_doc.pk)
                    #process_ppl_labels(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:ppl"))
            else:  
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:ppl"))
        except ObjectDoesNotExist:
            pass     
############
def update_ppl_account(request):
    record_id = request.POST.get('id','')
    record_type = request.POST.get('type','')
    record_value = request.POST.get('value','')
    record = Accounting.objects.get(id=record_id)
    if record_type == "period":
        record.period = record_value
    if record_type == "activity_period":
        record.activity_period = record_value
    if record_type == "retailer":
        record.retailer = record_value
    if record_type == "territory":
        record.territory = record_value
    if record_type == "orchard_UPC":
        record.orchard_UPC = record_value
    if record_type == "manufacturer_UPC":
        record.manufacturer_UPC = record_value
    if record_type == "imprint_label":
        record.imprint_label = record_value
    if record_type == "artist_name":
        record.artist_name = record_value
    if record_type == "product_name":
        record.product_name = record_value
    if record_type == "track_name":
        record.track_name = record_value
    if record_type == "track_artist":
        record.track_artist = record_value
    if record_type == "adjusted_total":
        record.adjusted_total = record_value
    if record_type == "total":
        record.total = record_value
    if record_type == "label_share_net_receipts":
        record.label_share_net_receipts = record_value
    record.save()
    return JsonResponse({"success":"Updated"})
############################
#     HORUS                #
############################
class VendorHorus(View):
    def get(self, *args, **kwargs):
        template = 'core/horus.html'
        metadata_form = UploadHorusAccountFormMetadata()
        accounting_form = UploadHorusAccountFormAccounting()
        docs = ProcessedDocument.objects.filter(which_doc="HORUS").all()
        records = Accounting.objects.filter(vendor="HORUS").all()[:400]
        context = {
            'metadata_form': metadata_form,
            'accounting_form': accounting_form,
            'docs': docs,
            'records': records
        }
        return render(self.request,template, context)
    def post(self, *args, **kwargs):
        metadata_form = UploadHorusAccountFormMetadata(self.request.POST, self.request.FILES or None)
        accounting_form = UploadHorusAccountFormAccounting(self.request.POST, self.request.FILES or None)
        try:
            if metadata_form.is_valid():
                horus_file = metadata_form.cleaned_data['horus_metadata_file']
                if not horus_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:horus")) 
                processed_doc = ProcessedDocument.objects.filter(file_name=horus_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:horus"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=horus_file.name,file_doc = horus_file, which_doc = "HORUS")
                    new_processed_doc.save()
                    process_horus.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:horus"))
            elif accounting_form.is_valid():
                horus_file = accounting_form.cleaned_data['horus_account_file']
                if not horus_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:horus")) 
                processed_doc = ProcessedDocument.objects.filter(file_name=horus_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:horus"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=horus_file.name,file_doc = horus_file, which_doc = "HORUS")
                    new_processed_doc.save()
                    process_horus_artist.delay(new_processed_doc.pk)
                    process_horus_account.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:horus"))
            else:  
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:horus"))
        except ObjectDoesNotExist:
            pass     
############################
#     WARNER ADA           #
############################
class VendorWarnerAda(View):
    def get(self, *args, **kwargs):
        template = 'core/warnerada.html'
        form = UploadWarnerAccountForm()
        context = {
            'form': form
        }
        return render(self.request,template, context)
    def post(self, *args, **kwargs):
        form = UploadWarnerAccountForm(self.request.POST, self.request.FILES or None)
        try:
            if form.is_valid():
                warner_file = form.cleaned_data['warner_file']
                if not warner_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:warner"))
                processed_doc = ProcessedDocument.objects.filter(file_name=warner_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:warner"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=warner_file.name,file_doc = warner_file, vendor = "WARNER")
                    new_processed_doc.save()
                    process_warner.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:warner"))
            else:
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:warner"))
        except ObjectDoesNotExist:
            pass  
############################
#     ORCHID                #
############################
class VendorOrchid(View):
    def get(self, *args, **kwargs):
        template = 'core/orchid.html'
        tform = UploadTrackForm()
        aform = UploadAlbumForm()
        context = {
            'tform': tform,
            'aform':aform
        }
        return render(self.request,template, context)
    def post(self, *args, **kwargs):
        tform = UploadTrackForm(self.request.POST, self.request.FILES or None)
        aform = UploadAlbumForm(self.request.POST, self.request.FILES or None)
        try:
            if tform.is_valid():
                track_file = tform.cleaned_data['track_file']
                if not track_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:orchid"))
                processed_doc = ProcessedDocument.objects.filter(file_name=track_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:orchid"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=track_file.name,file_doc = track_file, which_doc = "ORCHID-TRACK")
                    new_processed_doc.save()
                    process_track.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:orchid"))
            elif aform.is_valid():
                album_file = aform.cleaned_data['album_file']
                if not album_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:orchid"))
                processed_doc = ProcessedDocument.objects.filter(file_name=album_file.name).first()
                if processed_doc:
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:orchid"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=album_file.name,file_doc = album_file, which_doc = "ORCHID-ALBUM")
                    new_processed_doc.save()
                    process_album.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:orchid"))
            else:  
                messages.info(self.request, "Form is not valid!")
            return HttpResponseRedirect(reverse("core:orchid"))
        except ObjectDoesNotExist:
            pass
############################
#     ORCHARD           #
############################
class VendorOrchard(View):
    def get(self, *args, **kwargs):
        template = 'core/orchard.html'
        metadata_form = UploadOrchardMetadataForm()
        docs = ProcessedDocument.objects.filter(which_doc="ORCHARD").all()
        records = Accounting.objects.filter(vendor="ORCHARD").all()[:400]
        context = {
            'metadata_form': metadata_form, 
            'docs': docs,
            'records': records
        }
        return render(self.request,template, context)
    def post(self, *args, **kwargs):
        metadata_form = UploadOrchardMetadataForm(self.request.POST, self.request.FILES or None)
        try:
            if metadata_form.is_valid():
                orchard_metadata_file = metadata_form.cleaned_data['orchard_metadata_file']
                if not orchard_metadata_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:orchad"))
                processed_doc = ProcessedDocument.objects.filter(file_name=orchard_metadata_file.name).first()
                if processed_doc: 
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:orchad"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=orchard_metadata_file.name,file_doc = orchard_metadata_file, which_doc="ORCHARD")
                    new_processed_doc.save()
                    #process_orchard.delay(new_processed_doc.pk)
                    process_drmsys_import.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:orchad"))
            else:
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:orchad"))
        except ObjectDoesNotExist:
            pass  
#############
#############
class ImportVendor(View):
    def get(self, *args, **kwargs):
        template = 'core/orchard.html'
        metadata_form = UploadOrchardMetadataForm()
        docs = ProcessedDocument.objects.filter(which_doc="ORCHARD").all()
        records = Accounting.objects.filter(vendor="ORCHARD").all()[:400]
        context = {
            'metadata_form': metadata_form, 
            'docs': docs,
            'records': records
        }
        return render(self.request,template, context)
    def post(self, *args, **kwargs):
        metadata_form = UploadOrchardMetadataForm(self.request.POST, self.request.FILES or None)
        try:
            if metadata_form.is_valid():
                orchard_metadata_file = metadata_form.cleaned_data['orchard_metadata_file']
                if not orchard_metadata_file.name.endswith('.xlsx'):
                    messages.info(self.request, "Incorrect file format, File must be a xlsx format")
                    return HttpResponseRedirect(reverse("core:orchard"))
                processed_doc = ProcessedDocument.objects.filter(file_name=orchard_metadata_file.name).first()
                if processed_doc: 
                    messages.info(self.request, "This metadata file has been processed earlier. Please confirm !")
                    return HttpResponseRedirect(reverse("core:orchard"))
                else:
                    new_processed_doc = ProcessedDocument(file_name=orchard_metadata_file.name,file_doc = orchard_metadata_file, which_doc="ORCHARD")
                    new_processed_doc.save()
                    orchard = pd.read_excel(new_processed_doc.file_doc, Index=None).fillna('')
                    orchard.columns = orchard.columns.str.strip().str.lower()
                    print(orchard.columns)
                    # process_orchard.delay(new_processed_doc.pk)
                    messages.info(self.request, "Metadata sent for processing, you will get a prompt after processing! ")
                return HttpResponseRedirect(reverse("core:orchard"))
            else:
                messages.info(self.request, "Form is not valid  !")
            return HttpResponseRedirect(reverse("core:orchard"))
        except ObjectDoesNotExist:
            pass  
##### SPECIAL SCRIPT FUNCTION ######
SPOTIFY_CLIENT_ID = '31b05321124d421bb3fec23acb508501'
SPOTIFY_CLIENT_SECRET = '724bffca57fb4d0aaf22f95b2b35766b'
def run_special_script(request):
    # run_clean_up_script() 
    auth_manager = SpotifyClientCredentials(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    track = sp.search(q="LAGOS SISI", type="track")
    print(track)

    return HttpResponseRedirect(reverse("core:dashboard"))
            





