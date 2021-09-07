from .views import *
from django.urls import path

app_name = 'core'

urlpatterns = [
   path('', dashboard, name='dashboard'),
   path('track-view/', TrackView.as_view(), name='track-view'),
   path('album-view/', AlbumView.as_view(), name='album-view'),
   path('master-view/', MasterView.as_view(), name='master-view'),


   path('vendor/bmi/', VendorBMI.as_view(), name='bmi'),
   path('vendor/ditto/', VendorDitto.as_view(), name='ditto'),
   path('vendor/horus/', VendorHorus.as_view(), name='horus'),
   path('vendor/mcps/', VendorMCPS.as_view(), name='mcps'),
   path('vendor/warner/', VendorWarnerAda.as_view(), name='warner'),
   path('vendor/ppl/', VendorPPL.as_view(), name='ppl'),
   path('vendor/orchid/', VendorOrchid.as_view(), name='orchid'),
   path('vendor/orchad/', VendorOrchard.as_view(), name='orchad'),

   #path('vendor/orchard/', VendorOrchard.as_view(), name='orchard'),
   path('import_vendors/', ImportVendor.as_view(), name='orchard'),


   path('search_album/', search_album, name='search_album'),
   path('search_track/', search_track, name='search_track'),
   path('search_master/', search_master, name='search_master'),
   path('update_track/', update_track, name='update_track'),
   path('update_album/', update_album, name='update_album'),
   path('update_ditto_account/', update_ditto_account, name='update_ditto_account'),
   path('update_bmi_account/', update_bmi_account, name='update_bmi_account'),
   path('update_ppl_account/', update_ppl_account, name='update_ppl_account'),
   path('update_mcps_account/', update_mcps_account, name='update_mcps_account'),



   path('export_track_xls/', export_track_csv, name='export_track_xls'),
   path('export_album_xls/', export_album_csv, name='export_album_xls'),
   path('export_drmsys_xls/', export_drmsys_csv, name='export_drmsys_xls'),
   path('export_accounting_xls/', export_accounting_xls, name='export_accounting_xls'),
   path('export_accounting_csv/', export_accounting_csv, name='export_accounting_csv'),



   
   path('run_special_script/', run_special_script, name='run_special_script'),
   
]