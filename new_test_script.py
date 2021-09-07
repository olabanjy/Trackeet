import pandas as pd
import regex as re


horus = pd.read_excel('the_first_horus_statement.xlsx', Index=None).fillna('NA')
horus.columns = horus.columns.str.strip().str.lower()
#horus.columns = horus.columns.str.replace(' ', '').str.lower()
# print(horus.columns)

track_col = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
track_raw_data = {
    'DisplayUPC':[],'VolumeNo':[], 'TrackNo':[], 'HiddenTrack':[],'Title':[], 'SongVersion':[], 'Genre':[], 'ISRC':[], 'TrackDuration':[], 'PreviewClipStartTime':[], 'PreviewClipDuration':[], 'P_LINE':[], 'Recording Artist':[], 'Artist':[], 'Release Name':[], 'ParentalAdvisory':[], 'LabelName':[], 'Producer':[], 'Publisher':[],  'Writer':[], 'Arranger':[], 'Territories':[], 'Exclusive':[], 'WholesalePrice':[], 'Download':[], 'SalesStartDate':[], 'SalesEndDate':[], 'Error':[]
}



for index, row in horus.iterrows():
    isrc_row = horus.columns[horus.columns.str.contains(pat = 'isrc')].values[0]
    upc_row = horus.columns[horus.columns.str.contains("barcode|upc")].values[0]
    artist_row = horus.columns[horus.columns.str.contains(pat='artist')].values[0]
   
    title_row = horus.columns[horus.columns.str.contains('track title|asset title')].values[0]
    


   
    
   

    
    # print(row[title_row])
    # print(row[artist_row])

   

    

    # track_raw_data['ISRC'].append(row[isrc_row])
    # track_raw_data['DisplayUPC'].append(row[upc_row])
    # track_raw_data['Artist'].append(row[artist_row])
    # track_raw_data['Title'].append(row[title_row])


# print(track_raw_data)
# new_track_df = pd.DataFrame({key:pd.Series(value) for key, value in track_raw_data.items() }, columns=track_col)
# # trk_name = f'processed_horus_{today}_track.csv'
# track_file = new_track_df.to_csv('just_tested_anoda.csv', index = False, header=True)
    

