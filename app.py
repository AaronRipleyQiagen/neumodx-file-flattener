import base64
import datetime
import io

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
                print("Im in the right block")
                ChannelFinal = pd.concat([ChannelRaw, ChannelNorm, Channel2nd],axis=0)
                ChannelFinal['Channel'] = channel
                ChannelFinal.set_index(['Test Guid', 'Replicate Number'],inplace=True)

                #else:
                #print("Error in parsing Datablocks")

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

        print('Read Successful')

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

        return flat_data.reset_index()


#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app = dash.Dash(__name__, external_stylesheets=[dbc.themes.YETI])
app = dash_app.server
dash_app.myDataFrame = pd.DataFrame()
dash_app.myParser = nmdx_file_parser()
dash_app.layout = html.Div([ # this code section taken from Dash docs https://dash.plotly.com/dash-core-components/upload
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    
    html.Div(id='output-div'),
    html.Div(id='stored-data-description'),
    html.Div(id='output-datatable'),

    html.Div([
            dcc.Checklist(
                id='my_checklist',                      # used to identify component in callback
                options=[
                         {'label': x, 'value': x, 'disabled':False}
                         for x in ['Add Left / Right Label']
                ],
                value=[],    # values chosen by default

                className='my_box_container',           # class of the container (div)
                # style={'display':'flex'},             # style of the container (div)

                inputClassName='my_box_input',          # class of the <input> checkbox element
                # inputStyle={'cursor':'pointer'},      # style of the <input> checkbox element

                labelClassName='my_box_label',          # class of the <label> that wraps the checkbox input and the option's label
                labelStyle={
                            'padding':'0.5rem 1rem',
                            'border-radius':'10rem'},

                #persistence='',                        # stores user's changes to dropdown in memory ( I go over this in detail in Dropdown video: https://youtu.be/UYH_dNSX1DM )
                #persistence_type='',                   # stores user's changes to dropdown in memory ( I go over this in detail in Dropdown video: https://youtu.be/UYH_dNSX1DM )
            ),
        ]),

    html.Div([dbc.Button(id='btn',
            children=[html.I(className="fa fa-download mr-1"), "Download"],
            color="info",
            className="mt-1",
            style={
            'width': '50%',
            'height': '50px',
            'lineHeight': '25px',
            'borderWidth': '1px',
            'borderStyle': 'solid',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin-left': '25%',
            'vertical-align': 'center'
            },
        )]),
    

    dcc.Download(id="download-component"),

])


dash_app.title = 'NMDX Raw Data File Flattener'


def add_module_side():
    dash_app.myDataFrame['Left / Right Module Side'] = np.nan
    dash_app.myDataFrame['Left / Right Module Side'] = np.where(dash_app.myDataFrame['Pcr Cartridge Lane']<7, 'Right', 'Left')

def parse_contents(contents, filename, date):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        df = dash_app.myParser.scrapeFile(io.BytesIO(decoded), filename)
        dash_app.myDataFrame = pd.concat([dash_app.myDataFrame, df])
        dash_app.myDataFrame.drop_duplicates(subset=['Test Guid', 'Replicate Number', 'Processing Step', 'Channel'],inplace=True)
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])

    return html.Div([
        html.H5(filename+" was read successfully"),
        html.H5("Length of DataFrame: "+str(len(dash_app.myDataFrame))),
    ])



dash_app.annotation_functions = {'Add Left / Right Label': add_module_side}


@dash_app.callback(Output('output-datatable', 'children'),
              Input('upload-data', 'contents'),
              State('upload-data', 'filename'),
              State('upload-data', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children


# @dash_app.callback(Output('output-div', 'children'),
#               Input('submit-button','n_clicks'),
#               State('stored-data','data'),
#               State('xaxis-data','value'),
#               State('yaxis-data', 'value'))
# def make_graphs(n, data, x_data, y_data):
#     if n is None:
#         return dash.no_update
#     else:
#         bar_fig = px.bar(data, x=x_data, y=y_data)
#         # print(data)
#         return dcc.Graph(figure=bar_fig)


@dash_app.callback(
    Output("download-component", "data"),
    Input("btn", "n_clicks"),
    [State(component_id='my_checklist', component_property='value')],
    prevent_initial_call=True,
)
def func(n_clicks, options_chosen):
    
    data_output = dash_app.myDataFrame.copy()
    for option in options_chosen:
        dash_app.annotation_functions[option]()
    #return dict(content="Always remember, we're better together.", filename="hello.txt")
    return dcc.send_data_frame(data_output.to_csv, "FlatData.csv")
    #return dcc.send_data_frame(app.myDataFrame.to_excel, "mydf_excel.xlsx", sheet_name="Flat Raw Data")
    # return dcc.send_file("./assets/data_file.txt")
    # return dcc.send_file("./assets/bees-by-Lisa-from-Pexels.jpg")



if __name__ == '__main__':
    
    dash_app.run_server(debug=True)

