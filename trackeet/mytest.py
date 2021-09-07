import pandas as pd 



def process_track():
    col_list = ['DisplayUPC', 'VolumeNo', 'TrackNo','HiddenTrack', 'Title','SongVersion','Genre','ISRC','TrackDuration','PreviewClipStartTime','PreviewClipDuration','P_LINE','Recording Artist','Artist','Release Name','ParentalAdvisory','LabelName','Producer','Publisher','Writer','Arranger','Territories','Exclusive','WholesalePrice','Download','SalesStartDate','SalesEndDate','Error']
    track = pd.read_csv('track.csv',usecols=col_list, index_col=0).fillna('')
    # print(track.head())
    data = track.head()
    print(data['VolumeNo'])

def process_album():
    col_list = ['Product UPC', 'Total Volumes', 'Total Tracks', 'Product Title','Product Artist', 'Genre', 'CLINE', 'Release Date', 'Imprint Label', 'Artist URL', 'Catalog Number', 'Manufacturer UPC', 'Deleted?', 'Territories Carveouts', 'Master Carveouts']
    album = pd.read_excel('7931 sony_spec_album_converted.xlsx', sheet_name='7931 sony_spec_album', usecols=col_list, index_col=0).fillna('')
    album.to_excel('output.xlsx', sheet_name='7931 sony_spec_album')
    print(album.head())
    # album['Master Carveouts'].str.wrap(5)
    # album.to_csv('test2.csv')
    # print(album.columns)
    # for col in album.columns.tolist():
    #     print(col)
    # for row in album.iterrows():
    #     print(row['Product UPC'])
    #     print(row['Total Volumes'])
    #     print(row['Product Title'])
    #     print(row['CLINE'])
        
    # for row in album:
    #     print(row['Product UPC'])
        
    # print(album.head())
    

    # for row in album:
    #     print(len(row))




if __name__ == '__main__':
    process_album()