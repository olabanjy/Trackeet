from django.db import models
from django.conf import settings
from django.utils import timezone




# Create your models here.
class Album(models.Model):
    product_title = models.CharField(null=True, blank=True, max_length=500)
    product_upc = models.CharField(max_length=500)
    genre = models.CharField(null=True, blank=True, max_length=500)
    release_date = models.CharField(null=True, blank=True, max_length=500)
    total_volume = models.CharField(null=True, blank=True, max_length=200)
    imprint_label = models.CharField(null=True, blank=True, max_length=600)
    product_artist = models.CharField(null=True, blank=True, max_length=500)
    artist_url = models.CharField(null=True, blank=True, max_length=500)
    catalog_number = models.CharField(null=True, blank=True, max_length=500)
    manufacture_upc = models.CharField(null=True, blank=True, max_length=500)
    deleted = models.CharField(null=True, blank=True, max_length=200)
    total_tracks = models.CharField(null=True, blank=True, max_length=200)
    cline = models.TextField(null=True, blank=True)
    territories_carveouts = models.TextField(null=True, blank=True)
    master_carveouts = models.TextField(null=True, blank=True)
    processed_day = models.CharField(blank=True, null=True,max_length=500)
    updated_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{str(self.product_upc)} - {self.product_title}"


class Track(models.Model):
    album = models.ForeignKey(Album, on_delete=models.CASCADE, blank=True, null=True)
    display_upc = models.CharField(max_length=500,null=True, blank=True)
    volume_no = models.CharField(null=True, blank=True,max_length=200)
    track_no = models.CharField(null=True,blank=True,max_length=200)
    hidden_track = models.CharField(null=True, blank=True, max_length=500)
    title = models.CharField(null=True, blank=True, max_length=500)
    song_version = models.CharField(null=True, blank=True, max_length=500)
    genre = models.CharField(null=True, blank=True, max_length=500)
    isrc = models.CharField(max_length=500)
    track_duration = models.CharField(null=True, blank=True, max_length=200)
    preview_clip_start_time = models.CharField(null=True, blank=True, max_length=200)
    preview_clip_duration = models.CharField(null=True, blank=True, max_length=200)
    p_line = models.TextField(null=True, blank=True)
    recording_artist = models.CharField(null=True, blank=True, max_length=500)
    artist = models.CharField(null=True, blank=True, max_length=500)
    release_name = models.CharField(null=True, blank=True, max_length=500)
    parental_advisory = models.CharField(null=True, blank=True, max_length=200)
    label_name = models.CharField(null=True, blank=True, max_length=500)
    producer = models.CharField(null=True, blank=True, max_length=500)
    publisher = models.CharField(null=True, blank=True, max_length=500)
    writer = models.CharField(null=True, blank=True, max_length=500)
    arranger = models.TextField(null=True, blank=True)
    territories = models.TextField(null=True, blank=True)
    exclusive = models.CharField(null=True, blank=True, max_length=200)
    wholesale_price = models.CharField(null=True, blank=True,max_length=500)
    download = models.CharField(null=True, blank=True, max_length=200)
    sales_start_date = models.CharField(null=True, blank=True, max_length=200)
    sales_end_date = models.CharField(null=True, blank=True, max_length=200)
    error = models.CharField(null=True, blank=True, max_length=500)
    processed_day = models.CharField(blank=True, null=True,max_length=500)
    updated_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return str(self.isrc)


class Master_DRMSYS(models.Model):
    arranger = models.TextField(null=True, blank=True)
    artist = models.CharField(null=True, blank=True, max_length=600)
    artist_url = models.CharField(null=True, blank=True, max_length=600)
    bpm = models.CharField(null=True, blank=True, max_length=600)
    c_line = models.TextField(null=True, blank=True)
    data_type = models.CharField(null=True, blank=True, max_length=200)
    deleted = models.CharField(null=True, blank=True, max_length=200)
    display_upc = models.CharField(max_length=500,null=True, blank=True)
    download = models.CharField(null=True, blank=True, max_length=200)
    error = models.CharField(null=True, blank=True, max_length=600)
    exclusive = models.CharField(null=True, blank=True, max_length=600)
    genre = models.CharField(null=True, blank=True, max_length=600)
    genre_alt = models.CharField(null=True, blank=True, max_length=600)
    genre_sub = models.CharField(null=True, blank=True, max_length=600)
    genre_subalt = models.CharField(null=True, blank=True, max_length=600)
    hidden_track = models.CharField(null=True, blank=True, max_length=600)
    isrc = models.CharField(null=True, blank=True, max_length=600)
    itunes_link = models.CharField(null=True, blank=True, max_length=600)
    label_catalogno = models.CharField(null=True, blank=True, max_length=600)
    label_name = models.CharField(null=True, blank=True, max_length=600)
    language = models.TextField(null=True, blank=True)
    manufacturer_upc = models.CharField(null=True, blank=True, max_length=600)
    master_carveouts = models.TextField(null=True, blank=True)
    p_line = models.TextField(null=True, blank=True)
    parental_advisory = models.CharField(null=True, blank=True, max_length=600)
    preview_clip_duration = models.CharField(null=True, blank=True, max_length=600)
    preview_clip_start_time = models.CharField(null=True, blank=True, max_length=600)
    price_band = models.CharField(null=True, blank=True, max_length=600)
    producer = models.CharField(null=True, blank=True, max_length=600)
    publisher = models.CharField(null=True, blank=True, max_length=600)
    recording_artist = models.CharField(null=True, blank=True, max_length=600)
    release_date = models.CharField(null=True, blank=True, max_length=500)
    release_name = models.CharField(null=True, blank=True, max_length=500)
    royalty_rate = models.CharField(null=True, blank=True, max_length=200)
    sales_end_date = models.CharField(null=True, blank=True, max_length=200)
    sales_start_date = models.CharField(null=True, blank=True, max_length=200)
    song_version = models.CharField(null=True, blank=True, max_length=200)
    territories = models.TextField(null=True, blank=True)
    territories_carveouts = models.TextField(null=True, blank=True)
    title = models.CharField(null=True, blank=True, max_length=600)
    total_tracks = models.CharField(null=True, blank=True, max_length=600)
    total_volumes = models.CharField(null=True, blank=True, max_length=600)
    track_duration = models.CharField(null=True, blank=True, max_length=600)
    track_no = models.CharField(null=True, blank=True, max_length=200)
    vendor_name = models.TextField(null=True, blank=True)
    volume_no = models.CharField(null=True, blank=True, max_length=200)
    wholesale_price = models.CharField(null=True, blank=True, max_length=200)
    writer = models.CharField(null=True, blank=True, max_length=600)
    zIDKey_UPCISRC = models.CharField(null=True, blank=True, max_length=600)
    track = models.ForeignKey(Track,on_delete=models.CASCADE,blank=True, null=True)
    album = models.ForeignKey(Album, on_delete=models.CASCADE, blank=True, null=True)
    processed_day = models.CharField(blank=True, null=True,max_length=500)
    updated_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return str(self.display_upc)
 

class Accounting(models.Model):
    
    period = models.CharField(null=True, blank=True, max_length=600)
    activity_period  = models.CharField(null=True, blank=True, max_length=600)
    retailer = models.CharField(null=True, blank=True, max_length=600)
    territory = models.CharField(null=True, blank=True, max_length=600)
    orchard_UPC = models.CharField(null=True, blank=True, max_length=600)
    manufacturer_UPC = models.CharField(null=True, blank=True, max_length=600)	
    project_code = models.CharField(null=True, blank=True, max_length=600)
    product_code = models.CharField(null=True, blank=True, max_length=600)
    subaccount = models.CharField(null=True, blank=True, max_length=600)
    imprint_label = models.CharField(null=True, blank=True, max_length=600)
    artist_name = models.CharField(null=True, blank=True, max_length=600)
    product_name = models.CharField(null=True, blank=True, max_length=600)
    track_name = models.CharField(null=True, blank=True, max_length=600)
    track_artist = models.CharField(null=True, blank=True, max_length=600)
    isrc = models.CharField(null=True, blank=True, max_length=600)
    volume = models.CharField(null=True, blank=True, max_length=600)
    track = models.CharField(null=True, blank=True, max_length=600)
    trans_type = models.CharField(null=True, blank=True, max_length=600)
    trans_type_description = models.CharField(null=True, blank=True, max_length=600)
    unit_price = models.CharField(null=True, blank=True, max_length=600)
    discount = models.CharField(null=True, blank=True, max_length=600)
    actual_price = models.CharField(null=True, blank=True, max_length=600)
    quantity = models.CharField(null=True, blank=True, max_length=600)
    total = models.CharField(null=True, blank=True, max_length=600)
    adjusted_total = models.CharField(null=True, blank=True, max_length=600)
    split_rate = models.CharField(null=True, blank=True, max_length=600)
    label_share_net_receipts = models.CharField(null=True, blank=True, max_length=600)
    ringtone_publishing = models.CharField(null=True, blank=True, max_length=600)
    cloud_publishing = models.CharField(null=True, blank=True, max_length=600)
    publishing = models.CharField(null=True, blank=True, max_length=600)
    mech_administrative_fee = models.CharField(null=True, blank=True, max_length=600)
    preferred_currency = models.CharField(null=True, blank=True, max_length=600)
    vendor = models.CharField(null=True, blank=True, max_length=600)
    processed_day = models.CharField(blank=True, null=True,max_length=500)
    updated_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return self.product_name


class ProcessedDocument(models.Model):
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/', blank=True)
    process_time = models.DateTimeField(default=timezone.now)
    which_doc = models.CharField(null=True, blank=True, max_length=600)
    processed = models.BooleanField(default=False)

    def __str__(self):
        return self.file_name

class ProcessedTrackFile(models.Model):
    document = models.ForeignKey(ProcessedDocument, on_delete=models.CASCADE,blank=True, null=True)
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/processed_tracks', blank=True)
    file_name_txt = models.CharField(null=True, blank=True, max_length=600)
    file_doc_txt  = models.FileField(upload_to='documents/txt/processed_tracks', blank=True, null=True)
    process_time = models.DateTimeField(default=timezone.now)
    which_doc = models.CharField(null=True, blank=True, max_length=600)

    def __str__(self):
        return self.file_name

class ProcessedAlbumFile(models.Model):
    document = models.ForeignKey(ProcessedDocument, on_delete=models.CASCADE,blank=True, null=True)
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/processed_albums', blank=True, null=True)
    file_name_txt = models.CharField(null=True, blank=True, max_length=600)
    file_doc_txt  = models.FileField(upload_to='documents/txt/processed_albums', blank=True)
    process_time = models.DateTimeField(default=timezone.now)
    which_doc = models.CharField(null=True, blank=True, max_length=600)

    def __str__(self):
        return self.file_name

class ProcessedAccountFile(models.Model):
    document = models.ForeignKey(ProcessedDocument, on_delete=models.CASCADE,blank=True, null=True)
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/processed_account', blank=True)
    file_name_txt = models.CharField(null=True, blank=True, max_length=600)
    file_doc_txt  = models.FileField(upload_to='documents/txt/processed_albums', blank=True, null=True)
    process_time = models.DateTimeField(default=timezone.now)
    which_doc = models.CharField(null=True, blank=True, max_length=600)

    def __str__(self):
        return self.file_name


class VerifiedLabels(models.Model):
    label_name = models.CharField(null=True, blank=True, max_length=600)
    label_percentage = models.IntegerField(null=True, blank=True)
    vendor = models.CharField(null=True, blank=True, max_length=200)

    def __str__(self):
        return f"{self.label_name} - {self.label_percentage}" 

class WrongLabelDocs(models.Model):
    document = models.ForeignKey(ProcessedDocument, on_delete=models.CASCADE,blank=True, null=True)
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/wrong_labels/', blank=True)
    process_time = models.DateTimeField(default=timezone.now)

class MissingRecordsDoc(models.Model):
    document = models.ForeignKey(ProcessedDocument, on_delete=models.CASCADE,blank=True, null=True)
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/missing_records/', blank=True)
    process_time = models.DateTimeField(default=timezone.now)


class ProcessedArtistFile(models.Model):
    document = models.ForeignKey(ProcessedDocument, on_delete=models.CASCADE,blank=True, null=True)
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/processed_artist', blank=True)
    file_name_txt = models.CharField(null=True, blank=True, max_length=600)
    file_doc_txt  = models.FileField(upload_to='documents/txt/processed_artist', blank=True, null=True)
    process_time = models.DateTimeField(default=timezone.now)
    which_doc = models.CharField(null=True, blank=True, max_length=600)

    def __str__(self):
        return self.file_name

class ProcessedLabelFile(models.Model):
    document = models.ForeignKey(ProcessedDocument, on_delete=models.CASCADE,blank=True, null=True)
    file_name = models.CharField(null=True, blank=True, max_length=600)
    file_doc  = models.FileField(upload_to='documents/processed_label', blank=True)
    file_name_txt = models.CharField(null=True, blank=True, max_length=600)
    file_doc_txt  = models.FileField(upload_to='documents/txt/processed_label', blank=True, null=True)
    process_time = models.DateTimeField(default=timezone.now)
    which_doc = models.CharField(null=True, blank=True, max_length=600)

    def __str__(self):
        return f"{self.pk}"