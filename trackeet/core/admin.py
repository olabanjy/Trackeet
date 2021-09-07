from django.contrib import admin
from .models import * 

class AlbumManager(admin.ModelAdmin):
    list_display = ['product_title',
                    'product_upc',
                    'product_artist',
                    'catalog_number']
    
    search_fields = ['product_title','product_upc']
    

class TrackManager(admin.ModelAdmin):
    list_display = ['album',
                    'title',
                    'isrc',
                    'artist']
    
    search_fields = ['isrc','artist','title', 'display_upc']

class MasterManager(admin.ModelAdmin):
    list_display = ['title',
                    'artist',
                    'data_type']
    
    search_fields = ['title','artist']

    list_filter = ['data_type']

# Register your models here.

admin.site.register(Track,TrackManager)
admin.site.register(Album,AlbumManager)
admin.site.register(Master_DRMSYS,MasterManager)
admin.site.register(Accounting)
admin.site.register(ProcessedDocument)
admin.site.register(ProcessedTrackFile)
admin.site.register(ProcessedAlbumFile)
admin.site.register(ProcessedAccountFile)
admin.site.register(VerifiedLabels)
admin.site.register(WrongLabelDocs)
admin.site.register(MissingRecordsDoc)
admin.site.register(ProcessedArtistFile)
admin.site.register(ProcessedLabelFile)
