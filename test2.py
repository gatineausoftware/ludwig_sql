import dash
import dash_core_components as dcc
import dash_html_components as html

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def get_type_dropdown():
    dd = dcc.Dropdown(
    options=[
        {'label': 'New York City', 'value': 'NYC'},
        {'label': 'Montréal', 'value': 'MTL'},
        {'label': 'San Francisco', 'value': 'SF'}
    ],
    value='MTL')
    return dd






def generate_table(headers, columns):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in headers])] +

        # Body
        [html.Tr([
            html.Td(col['name']), html.Td(get_type_dropdown())]) for col in columns])


headers = ['Column', 'Type']
columns = [{'name': 'Age'}, {'name': 'Class'}, {'name': 'Fare'}]

app.layout = html.Div(children=[
     generate_table(headers, columns)])




if __name__ == '__main__':
    app.run_server(debug=True)