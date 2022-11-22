import base64
import datetime
import io
import uuid

import dash
from dash.dependencies import Input, Output, State
from dash import dcc
from dash import html
from dash import dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

import pandas as pd
import numpy as np

class nmdx_file_parser:
    """
    A class used to read raw data file(s) and convert to flat format.

    Methods
    -------
    scrapeFile(file=None, env=None)
        Scrapes data from one raw data file.
    """
    def __init__(self):
        self.file_data = {}

    def readChannelData(file, sheet, channel):

        channelData_all = pd.read_excel(io=file,sheet_name=sheet)
        if len(channelData_all) > 0:
            ChannelRawStart = channelData_all[channelData_all['Sample ID']=='Raw'].index.values[0] + 1
            ChannelRawEnd = channelData_all[channelData_all['Sample ID']=='Normalized'].index.values[0] - 2
            ChannelRaw = channelData_all.loc[ChannelRawStart:ChannelRawEnd]
            ChannelRaw['Processing Step'] = 'Raw'

            ChannelNormStart = channelData_all[channelData_all['Sample ID']=='Normalized'].index.values[0] + 1
            ChannelNormEnd = channelData_all[channelData_all['Sample ID']=='SecondDerivative'].index.values[0] - 2
            ChannelNorm = channelData_all.loc[ChannelNormStart:ChannelNormEnd]
            ChannelNorm['Processing Step'] = 'Normalized'

            Channel2ndStart = channelData_all[channelData_all['Sample ID']=='SecondDerivative'].index.values[0] + 1

            if 'Modulated' in channelData_all['Sample ID'].unique():
                Channel2ndEnd = channelData_all[channelData_all['Sample ID']=='Modulated'].index.values[0] - 2
                ChannelModulatedStart = channelData_all[channelData_all['Sample ID']=='Modulated'].index.values[0] + 1
                ChannelModulated = channelData_all.loc[ChannelModulatedStart:ChannelModulatedStart+len(ChannelRaw)]
                ChannelModulated['Processing Step'] = 'Modulated'
                Channel2nd = channelData_all.loc[Channel2ndStart:Channel2ndEnd]
                Channel2nd['Processing Step'] = '2nd'

                if len(ChannelRaw) == len(ChannelNorm) and len(ChannelRaw) == len(Channel2nd) and len(ChannelRaw) == len(ChannelModulated):

                    ChannelFinal = pd.concat([ChannelRaw, ChannelNorm, Channel2nd, ChannelModulated],axis=0)
                    ChannelFinal['Channel'] = channel
                    ChannelFinal.set_index(['Test Guid', 'Replicate Number'],inplace=True)
                else:
                    print("Error in parsing Datablocks")
            else:
                Channel2nd = channelData_all.loc[Channel2ndStart:Channel2ndStart+len(ChannelRaw)]
                Channel2nd['Processing Step'] = '2nd'
                #if len(ChannelRaw) == len(ChannelNorm) and len(ChannelRaw) == len(Channel2nd):
                ChannelFinal = pd.concat([ChannelRaw, ChannelNorm, Channel2nd],axis=0)
                ChannelFinal['Channel'] = channel
                ChannelFinal.set_index(['Test Guid', 'Replicate Number'],inplace=True)


        else:
            ChannelFinal = pd.DataFrame()



        return ChannelFinal
    
    def readRawData(file):
        channelDict = {'Green_470_510':'Green',
                    'Yellow_530_555':'Yellow',
                    'Orange_585_610':'Orange',
                    'Red_625_660':'Red',
                    'Far_Red_680_715':'Far_Red'}

        Summary_Tab = pd.read_excel(io=file,sheet_name='Summary',header=2)
        COC_Tab = pd.read_excel(io=file,sheet_name='Chain of Custody')
        Summary_COC_Data = Summary_Tab.set_index(['Test Guid', 'Replicate Number']).join(COC_Tab.set_index(['Test Guid', 'Replicate Number']).loc[:, [x for x in COC_Tab.columns if x not in Summary_Tab.columns]])


        channelDataDict = {}
        for channel in channelDict:
            channelDataDict[channel] = nmdx_file_parser.readChannelData(file, channel, channelDict[channel])
        channelDataFinal = pd.concat([channelDataDict[df] for df in channelDataDict if len(channelDataDict[df])>0],axis=0)

        
        channelDataFinal.set_index(['Target Result Guid', 'Processing Step', 'Channel'],append=True,inplace=True)
        for i in range(1,256):
            if "Readings "+ str(i) not in channelDataFinal.columns:
                channelDataFinal["Readings "+str(i)] = np.nan
        channelDataFinal_readings = channelDataFinal.loc[:, ['Readings '+str(i) for i in range(1,256)]]
        channelDataFinal_summary = channelDataFinal.swaplevel(3,0).swaplevel(3,1).swaplevel(3,2)
        channelDataFinal_summary = channelDataFinal_summary.loc['Raw'].drop(['Readings '+str(i) for i in range(1,256)],axis=1)

        return Summary_COC_Data, channelDataFinal_summary, channelDataFinal_readings
    
    def retrieveConsumableLots(data, consumable_types=['Pcr Cartridge', 'Capture Plate', 'Test Strip NeuMoDx', 'Buffer', 'Release Reagent', 'Wash Reagent']):
        """
        Retrieves Lot information for NMDX Consumables from Barcode String
        :param consumable_types: list-like List of Consumables to get Data For.
        """
    
        for consumable_type in consumable_types:
            data[consumable_type+" Lot"] = data[consumable_type+" Barcode"].str[18:24]

        return data

    def retrieveConsumableSerials(data, consumable_types=['Pcr Cartridge', 'Capture Plate', 'Test Strip NeuMoDx', 'Buffer', 'Release Reagent', 'Wash Reagent']):
        """
        Retrieves Consumable Serial information for NMDX Consumables from Barcode String
        :param consumable_types: list-like List of Consumables to get Data For
        """
        
        for consumable_type in consumable_types:
            data[consumable_type+" Serial"] = data[consumable_type+" Barcode"].str[27:32]

        return data

    def retrieveConsumableExpiration(data, consumable_types=['Pcr Cartridge', 'Capture Plate', 'Test Strip NeuMoDx', 'Buffer', 'Release Reagent', 'Wash Reagent']):
        """
        Retrieves Expiration Date information for NMDX Consumables from Barcode String
        :param consumable_types: list-like List of Consumables to get Data For.
        """
    
        for consumable_type in consumable_types:
            data[consumable_type+" EXP Date"] = data[consumable_type+" Barcode"].str[-6:].apply(lambda x: pd.to_datetime(arg=x, format="%y%m%d"))

        return data

    def scrapeFile(self, file, filename):
           
        #time = pd.Timestamp.now()

        summary_coc, channel_summary, channel_readings = nmdx_file_parser.readRawData(file)
        
        for col in channel_summary.columns:
            if 'Barcode' in col:
                channel_summary[col] = channel_summary[col].astype(str)
                channel_summary[col] = channel_summary[col].str.replace("_x001D_", " ")
        channel_summary = channel_summary.astype(object).where(pd.notna(channel_summary), None)


        for col in summary_coc.columns:
            if 'Barcode' in col:
                summary_coc[col] = summary_coc[col].astype(str)
                summary_coc[col] = summary_coc[col].str.replace("_x001D_", " ")
        summary_coc = summary_coc.astype(object).where(pd.notna(summary_coc), None)
        for col in summary_coc.loc[:, [col for col in summary_coc if 'Date' in col]].columns:
            summary_coc.loc[:, col] = summary_coc.loc[:, col].astype(str)
            summary_coc.loc[:, col] = summary_coc.loc[:, col].str.replace(' -04:00','').replace(' -05:00','')
            try:
                summary_coc.loc[:, col] = summary_coc.loc[:, col].astype('datetime64[ns]')
            except:
                summary_coc.loc[:, col] = np.nan


        channel_readings = channel_readings.astype(object).where(pd.notna(channel_readings), None)

        channel_summary['File Source'] = filename
        channel_readings['File Source'] = filename
        summary_coc['File Source'] = filename
        summary_coc.rename({'Flags':'Summary Flags'},axis=1,inplace=True)
        channel_summary.rename({'Flags':'Channel Flags'},axis=1,inplace=True)
        summary_coc = nmdx_file_parser.retrieveConsumableLots(summary_coc)
        summary_coc = nmdx_file_parser.retrieveConsumableSerials(summary_coc)
        summary_coc = nmdx_file_parser.retrieveConsumableExpiration(summary_coc)
        
        

        flat_data = summary_coc.join(channel_summary.loc[:, [x for x in channel_summary.columns if x not in summary_coc.columns]]).join(channel_readings.loc[:, [x for x in channel_readings.columns if x not in channel_summary.columns]])
        
        ##Add Target Result / Localized Result columns if not in flat_data columns
        if 'Localized Result' not in flat_data.columns:
            flat_data['Localized Result'] = np.nan
        
        if 'Target Result' not in flat_data.columns:
            flat_data['Target Result'] = np.nan

        flat_data['Localized Result'] = np.where(flat_data['Localized Result'].isnull(), flat_data['Target Result'], flat_data['Localized Result'])
        flat_data.drop(['Target Result'], axis=1, inplace=True)

        return flat_data.reset_index()


#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app = dash.Dash(__name__, external_stylesheets=[dbc.themes.YETI])
app = dash_app.server

def add_module_side(data):
    data['Left / Right Module Side'] = np.nan
    data['Left / Right Module Side'] = np.where(data['Pcr Cartridge Lane']<7, 'Right', 'Left')
    return data.reset_index().set_index(['Test Guid', 'Replicate Number', 'Target Result Guid', 'Channel', 'Processing Step'])

def getRawMinusBlankCheckReads(data):
        """
        A Function used to calculate the Difference between the First three Raw Readings and Blank Check Values for each target result included in dataset provided
        Parameters
        ----------
        data (pandas.DataFrame) = DataFrame to be used for Calculation.
        """
        RawReadsMinusBlankCheckFrame = data.reset_index()[['Processing Step', 'Test Guid', 'Replicate Number', 'Target Result Guid']+['Readings 1', 'Readings 2', 'Readings 3', 'Blank Reading']].copy()
        RawReadsMinusBlankCheckFrame.set_index(['Processing Step', 'Test Guid', 'Replicate Number', 'Target Result Guid'],inplace=True)
        RawReadsMinusBlankCheckFrame_Raw = RawReadsMinusBlankCheckFrame.loc['Raw']
        RawReadsMinusBlankCheckFrame_Raw['Blank Check - 1st 3 Reads'] = RawReadsMinusBlankCheckFrame_Raw[['Readings 1', 'Readings 2', 'Readings 3']].mean(axis=1) - RawReadsMinusBlankCheckFrame_Raw['Blank Reading']
        RawReadsMinusBlankCheckFrame = RawReadsMinusBlankCheckFrame.join(RawReadsMinusBlankCheckFrame_Raw[['Blank Check - 1st 3 Reads']])
        data['Blank Check - 1st 3 Reads'] = RawReadsMinusBlankCheckFrame['Blank Check - 1st 3 Reads'].values
        return data.reset_index().set_index(['Test Guid', 'Replicate Number', 'Target Result Guid', 'Channel', 'Processing Step'])

def channelParametersFlattener(data):
    """
    Retrieves Channel Specific stats and returns them all channels in one-dimmensional column.
    stats:  Which Stats to flatten.
    """
    stats=['Localized Result']
    channel_stats = data.reset_index().drop_duplicates(['Test Guid', 'Channel', 'Replicate Number']).set_index(['Test Guid', 'Replicate Number']).loc[:, stats+['Channel']].copy()
    channel_stats = channel_stats.reset_index().pivot(columns='Channel',values=stats,index=['Test Guid', 'Replicate Number'])
    channel_stats.columns = [y+" "+x for (x,y) in channel_stats.columns]
    data = data.reset_index().set_index(['Test Guid', 'Replicate Number']).join(channel_stats)
    return data.reset_index().set_index(['Test Guid', 'Replicate Number', 'Target Result Guid', 'Channel', 'Processing Step'])

dash_app.myParser = nmdx_file_parser()
dash_app.DataFrames = {}
dash_app.title = 'NMDX Raw Data File Flattener'

dash_app.annotation_functions = {'Add Left / Right Label': add_module_side,
                                 'Add First Three Raw Reads - Blank Check':getRawMinusBlankCheckReads,
                                 'Add Inline Localized Results':channelParametersFlattener}

def serve_layout():
    
    my_session = str(uuid.uuid4())
    
    return html.Div([dcc.Store(id='session-id', data=my_session), # this code section taken from Dash docs https://dash.plotly.com/dash-core-components/upload
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '95%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin-left': '2.5%'
        },
        # Allow multiple files to be uploaded
        multiple=True,
    ),
    
    html.Div(id='stored-data-description', style={'margin-left':'2.5%'}),
    html.Div(id='hidden-div'),

    html.Div([
            dcc.Checklist(
                id='my_checklist',                      # used to identify component in callback
                options=[
                         {'label': x, 'value': x, 'disabled':False}
                         for x in ['Add Left / Right Label', 'Add First Three Raw Reads - Blank Check', 'Add Inline Localized Results']
                ],
                value=[],    # values chosen by default

                className='my_box_container',           # class of the container (div)
                # style={'display':'flex'},             # style of the container (div)

                inputClassName='my_box_input',          # class of the <input> checkbox element
                # inputStyle={'cursor':'pointer'},      # style of the <input> checkbox element

                labelClassName='my_box_label',          # class of the <label> that wraps the checkbox input and the option's label
                labelStyle={
                            
                            'border-radius':'10rem',
                            'margin-left':'2.5%'},

                #persistence=True,                     # stores user's changes to dropdown in memory ( I go over this in detail in Dropdown video: https://youtu.be/UYH_dNSX1DM )
                #persistence_type='memory',                   # stores user's changes to dropdown in memory ( I go over this in detail in Dropdown video: https://youtu.be/UYH_dNSX1DM )
            ),
        ]),

    html.Div([dbc.Button(id='btn',
            children=[html.I(className="fa fa-download mr-1"), "Download DataFrame"],
            color="info",
            className="mt-1",
            n_clicks=0,
            style={
            'width': '22.5%',
            'height': '50px',
            'lineHeight': '25px',
            'borderWidth': '1px',
            'borderStyle': 'solid',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin-left': '25%',
            'vertical-align': 'center'
            },
        ),
        dbc.Button(id='clear_btn', children=["Clear DataFrame"], color="info", class_name="mt-1", n_clicks=0, style={
            'width': '22.5%',
            'height': '50px',
            'lineHeight': '25px',
            'borderWidth': '1px',
            'borderStyle': 'solid',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin-left': '5%',
            'vertical-align': 'center'
            })]),
    

    dcc.Download(id="download-component")
])

def parse_contents(contents, filename, session_id):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        df = dash_app.myParser.scrapeFile(io.BytesIO(decoded), filename)
        dash_app.DataFrames[session_id] = pd.concat([dash_app.DataFrames[session_id], df])
        dash_app.DataFrames[session_id].drop_duplicates(subset=['Test Guid', 'Replicate Number', 'Processing Step', 'Channel'],inplace=True)
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])
    return html.Div([
        html.H5([filename+" was read successfully   ", "Length of DataFrame: "+str(len(dash_app.DataFrames[session_id]))])
    ])

dash_app.layout = serve_layout

@dash_app.callback(Output('hidden-div', 'children'),
                    Input('session-id', 'data'))

def initialize_session(session_id):
    
    dash_app.DataFrames[session_id] = pd.DataFrame()

    if len(dash_app.DataFrames) > 5:
        dash_app.DataFrames.popitem()[0]

    
    return html.Div([])


@dash_app.callback(Output('stored-data-description', 'children'),
              Output('clear_btn', 'n_clicks'),
              Input('upload-data', 'contents'),
              Input('clear_btn', 'n_clicks'),
              State('upload-data', 'filename'),
              State('upload-data', 'last_modified'),
              Input('session-id', 'data'), prevent_initial_call=True)

def update_output(list_of_contents,  n_clicks, list_of_names, list_of_dates, session_id):
    
    if n_clicks==0 and list_of_contents is not None:
        n_clicks = 0
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, [session_id for x in list_of_contents])]
        return children, n_clicks

    
    if n_clicks==1:
        dash_app.DataFrames[session_id] = pd.DataFrame()
        n_clicks = 0
        return html.Div([
            html.H5("DataFrame was cleared successfully"),
            html.H5("Length of DataFrame: "+str(len(dash_app.DataFrames[session_id]))),
        ]), n_clicks


@dash_app.callback(Output("download-component", "data"),
    Input("btn", "n_clicks"),
    State(component_id='my_checklist', component_property='value'),
    Input("session-id", "data"),
    prevent_initial_call=True,
)
def download_function(n_clicks, options_chosen, session_id):
    
    data_output = dash_app.DataFrames[session_id].set_index(['Test Guid', 'Replicate Number', 'Target Result Guid', 'Channel', 'Processing Step']).copy()
    for option in options_chosen:
        data_output = dash_app.annotation_functions[option](data_output)
   
   
    return dcc.send_data_frame(data_output.reset_index().to_csv, "FlatData.csv", index=False)

if __name__ == '__main__':
    
    dash_app.run_server(debug=True)
