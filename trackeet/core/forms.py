from django import forms


####### ORCHID TRACK ##########
class UploadTrackForm(forms.Form):
    track_file = forms.FileField(widget=forms.FileInput(attrs={
        'class':'form-control'
    }))




####### ORCHID ALBUM #########
class UploadAlbumForm(forms.Form):
    album_file = forms.FileField(widget=forms.FileInput(attrs={
        'class':'form-control'
    }))


########### BMI ##########
class UploadBMIAccountFormMetadata(forms.Form):
    bmi_metadata_file = forms.FileField(widget=forms.FileInput())

class UploadBMIAccountFormAccounting(forms.Form):
    bmi_accounting_file = forms.FileField(widget=forms.FileInput())


###   DITTO #####
class UploadDittoAccountFormMetadata(forms.Form):
    ditto_metadata_file = forms.FileField(widget=forms.FileInput())

class UploadDittoAccountFormAccounting(forms.Form):
    ditto_account_file = forms.FileField(widget=forms.FileInput())



######## MCPS ###########
class UploadMCPSAccountFormMetadata(forms.Form):
    mcps_metadata_file = forms.FileField(widget=forms.FileInput())

class UploadMCPSAccountFormAccounting(forms.Form):
    mcps_account_file = forms.FileField(widget=forms.FileInput())
    



##### PPL #########
class UploadPPLForLabelCheck(forms.Form):
    ppl_file = forms.FileField(widget=forms.FileInput())

class UploadPPLAccountFormAccounting(forms.Form):
    ppl_account_file = forms.FileField(widget=forms.FileInput())


######## WARNER #########
class UploadWarnerAccountForm(forms.Form):
    warner_file = forms.FileField(widget=forms.FileInput(attrs={
        'class':'dropzone'
    }))


########## HORUS ############
class UploadHorusAccountFormMetadata(forms.Form):
    horus_metadata_file = forms.FileField(widget=forms.FileInput())

class UploadHorusAccountFormAccounting(forms.Form):
    horus_account_file = forms.FileField(widget=forms.FileInput())


######## ORCHARD #########
class UploadOrchardMetadataForm(forms.Form):
    orchard_metadata_file = forms.FileField(widget=forms.FileInput())


######## ADD SINGLE TRACK RECORD ###############
class AddTrackRecord(forms.Form):
    display_upc = forms.CharField(required=True, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    volume_no = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    track_no = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    hidden_track = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    title = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    song_version = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    genre = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    isrc = forms.CharField(required=True, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    track_duration = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    preview_clip_start_time = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    preview_clip_duration = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    p_line = forms.CharField(required=False, widget=forms.Textarea(attrs={
        'class':'form-control',
        'rows': '3'
    }))
    recording_artist = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    artist = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    release_name = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    parental_advisory = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    label_name = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    producer = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    publisher = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    writer = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    arranger = forms.CharField(required=False, widget=forms.Textarea(attrs={
        'class':'form-control',
        'rows': '3'
    }))
    territories = forms.CharField(required=False, widget=forms.Textarea(attrs={
        'class':'form-control',
        'rows': '3'
    }))
    exclusive = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    wholesale_price = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    download = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    sales_start_date = forms.CharField(required=False,widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    sales_end_date = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    error = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))





############ ADD SINGLE ALBUM RECORD #############
class AddAlbumRecord(forms.Form):
    product_title = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    product_upc = forms.CharField(required=True, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    genre = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    release_date = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    total_volume = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    imprint_label = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    product_artist = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    artist_url = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    catalog_number = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    manufacture_upc = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    deleted = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
    }))
    total_tracks = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'class':'form-control'
        
    }))
    cline = forms.CharField(required=False, widget=forms.Textarea(attrs={
        'class':'form-control',
        'rows': '3'
    }))
    territories_carveouts = forms.CharField(required=False, widget=forms.Textarea(attrs={
        'class':'form-control',
        'rows': '3'
    }))
    master_carveouts = forms.CharField(required=False, widget=forms.Textarea(attrs={
        'class':'form-control',
        'rows': '3'
    }))

    