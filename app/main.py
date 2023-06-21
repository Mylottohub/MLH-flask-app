from flask import Flask, g, request, jsonify, make_response
import sqlite3, os

# for the automated ML cronjob
import numpy as np
import pandas as pd
import datetime, requests, logging
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_data():
    """
    Function to retrieve data from the database, with dates
    ranging from 2004 to the present day.
    INPUT
        None
    OUTPUT
        the database as a Pandas DataFrame
    """
    start_date = "2004-01-01"
    end_date = str(datetime.datetime.now()).split()[0]
    payload = {"date_from": start_date,
               "date_to": end_date}
    auth = ('algo', 'mixjuice33')
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
    }
    r = requests.post("https://www.mylottohub.com/api/get_result",
                      auth=auth, headers=headers, json=payload)

    # converting the json data to Pandas DataFrame
    database = pd.DataFrame(r.json())
    # converting the date column to datetime dtype
    database.date = pd.to_datetime(database.date)
    # removing the columns with `machine` in their names
    machine_columns = database.columns[database.columns.str.contains(
        'machine')]
    database.drop(machine_columns, 1, inplace=True)

    return database


def select_game_data(game_no, database):
    """
    Function to extract only the data of the game specified.
    INPUT
        game_no     the ID of the specific game
        database    the entire database
    OUTPUT
        data        the data of the particular game
    """
    game_no = str(game_no)
    data = database.loc[database.game == game_no]

#   # creating additional features from date column
#     df['day'] = df.date.dt.day
#     df['weekday'] = df.date.dt.weekday
#     df['yearday'] = df.date.dt.dayofyear

    return data.drop(['id', 'winning_number', 'date', ], 1)


def create_features(data):
    """
    Function to create new features needed for preprocessing
    INPUT
        data        the unpreprocessed data
        data        the preprocessed data with new features/columns
    """
    if data.winning_num6.isna().any():
        data.drop('winning_num6', 1, inplace=True)
        winning_numbers = ['winning_num1', 'winning_num2', 'winning_num3',
                           'winning_num4', 'winning_num5', ]
    else:
        winning_numbers = ['winning_num1', 'winning_num2', 'winning_num3',
                           'winning_num4', 'winning_num5', 'winning_num6']
    data = data.astype(int)

    # creating the isodd columns & split left and right
    for number in winning_numbers:
        data['isodd_' +
             number] = data[number].apply(lambda x: 1 if x % 2 else 0)
        data[number + '_left'] = data[number].astype(str).apply(lambda x: x[0])
        data[number + '_right'] = data[number].astype(
            str) .apply(lambda x: x[1] if len(x) > 1 else -1)

    # creating the odd count column
    def odd_count(data):
        data['isodd_count'] = 0
        for number in winning_numbers:
            if data['isodd_' + number]:
                data.isodd_count += 1
        return data.isodd_count
    data['count_is_odd'] = data.apply(odd_count, 1)

    # creating count of winning total column
    count_of_sum = data.winning_total.value_counts()
    data['count_winning_total'] = data.winning_total.map(count_of_sum)

    return data


def create_bucket(data):
    """
    Function to create a bucket of the most-likely-to-occur numbers
    INPUT
        data            the data
        bucket_size     the preferred total of numbers in the bucket
    OUTPUT
        bucket          bucket of numbers
    """
    # concatenating all the winning numbers as a single column of data
    try:
        freq_table = pd.concat([data.winning_num1,
                                data.winning_num2,
                                data.winning_num3,
                                data.winning_num4,
                                data.winning_num5,
                                data.winning_num6])
    except:
        freq_table = pd.concat([data.winning_num1,
                                data.winning_num2,
                                data.winning_num3,
                                data.winning_num4,
                                data.winning_num5])

    # Creating a frequency of numbers by the number of times they've occurred
    freq_report = pd.DataFrame(
        freq_table.value_counts()).reset_index() .rename(
        columns={
            'index': 'Number',
            0: 'Hits'})
    freq_report['Percentage'] = round(
        (freq_report.Hits / data.shape[0]) * 100, 2)
    freq_of_percentage = freq_report.Percentage.value_counts()
    freq_report['freq_of_percentage'] = freq_report.Percentage.map(
        freq_of_percentage)
    freq_report.sort_values(by='freq_of_percentage',
                            ascending=False, inplace=True)

    freq_table = freq_report.Number.tolist()
    try:
        top_numbers = freq_table[:13]   # incase the game doesn't have numbers up to length of 13
    except:
        return tuple(freq_table)        # return the entire list of numbers if it is not up to length of 13
    differentials = freq_table[-7:]

    return str(list(dict.fromkeys(top_numbers + differentials)))


def save_to_sql(result_dict):
    # truncate the former file
    with open('results.sql', 'w') as f:
        pass
        
    # write new file
    for game in result_dict.keys():
        columns = "`GAME`, `BUCKET`"
        values = f"'{game}', '{result_dict[game]}'"
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('results', columns, values)
        
        f = open("results.sql", "a")
        f.write(sql + '\n')


def get_bucket():
    """
    The main function to run; all other functions are ran automatically
    by this function.
    The function will load the data, select the specified game, carry out 
    preprocessing steps, and retrieve the bucket of the length 20 for all the games.
    INPUT:
        None
    OUTPUT
        bucket        bucket of length 20
    """
    database = load_data()
    result = {}
    for game_id in database.game.unique().tolist():
        data = select_game_data(game_no=game_id, database=database)
        data = create_features(data)
        result[game_id] = create_bucket(data)
    return save_to_sql(result)


def init_db():
    get_bucket()
    # truncate the former db
    with open('results.db', 'w') as f:
        pass 
    
    # create a new schema
    with app.app_context():
        db = get_db() 
        db.cursor().execute("CREATE TABLE results(GAME text, BUCKET text);")
        with app.open_resource(os.path.join(BASE_DIR, 'results.sql'), mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db_path = os.path.join(BASE_DIR, 'results.db')
        db = g._database = sqlite3.connect(db_path)
    db.row_factory = make_dicts
    return db 


def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))


@app.teardown_appcontext 
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(init_db, 'interval', days=14)
scheduler.start()

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

                
# main app
@app.route('/api/results', methods=['GET'])
def get_results():
    if not request.json or not 'GAME' in request.json:
        return bad_request(400)

    game = request.json.get('GAME')
    query = f"SELECT * FROM results WHERE GAME={game};"
    
    c = get_db().cursor()
    result = c.execute(query).fetchall()
    
    if not result:
        return not_found(404)
    return jsonify(result), 201



# defining the error-handler functions
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Game not found'}), 404)

@app.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({'error': 'Bad request, not JSON'}), 400)
