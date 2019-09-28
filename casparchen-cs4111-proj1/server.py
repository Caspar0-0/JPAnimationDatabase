#!/usr/bin/env python2.7

"""
Columbia's COMS W4111.001 Introduction to Databases
Example Webserver

To run locally:

        python server.py

Go to http://localhost:8111 in your browser.

A debugger such as "pdb" may be helpful for debugging.
Read about it online.
"""

import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)


#
# The following is a dummy URI that does not connect to a valid database. You will need to modify it to connect to your Part 2 database in order to use the data.
#
# XXX: The URI should be in the format of:
#
#         postgresql://USER:PASSWORD@104.196.18.7/w4111
#
# For example, if you had username biliris and password foobar, then the following line would be:
#
#         DATABASEURI = "postgresql://biliris:foobar@104.196.18.7/w4111"
#
DATABASEURI = "postgresql://jc5067:5777@34.73.21.127/proj1part2"


#
# This line creates a database engine that knows how to connect to the URI above.
#
engine = create_engine(DATABASEURI)

#
# Example of running queries in your database
# Note that this will probably not work if you already have a table named 'test' in your database, containing meaningful data. This is only an example showing you how to run queries in your database using SQLAlchemy.
#
#engine.execute("""CREATE TABLE IF NOT EXISTS test (
#    id serial,
#    name text
#);""")
#engine.execute("""INSERT INTO test(name) VALUES ('grace hopper'), ('alan turing'), ('ada lovelace');""")


@app.before_request
def before_request():
    """
    This function is run at the beginning of every web request
    (every time you enter an address in the web browser).
    We use it to setup a database connection that can be used throughout the request.

    The variable g is globally accessible.
    """
    try:
        g.conn = engine.connect()
    except:
        print("uh oh, problem connecting to database")
        import traceback; traceback.print_exc()
        g.conn = None

@app.teardown_request
def teardown_request(exception):
    """
    At the end of the web request, this makes sure to close the database connection.
    If you don't, the database could run out of memory!
    """
    try:
        g.conn.close()
    except Exception as e:
        pass


#
# @app.route is a decorator around index() that means:
#     run index() whenever the user tries to access the "/" path using a GET request
#
# If you wanted the user to go to, for example, localhost:8111/foobar/ with POST or GET then you could use:
#
#             @app.route("/foobar/", methods=["POST", "GET"])
#
# PROTIP: (the trailing / in the path is important)
#
# see for routing: http://flask.pocoo.org/docs/0.10/quickstart/#routing
# see for decorators: http://simeonfranklin.com/blog/2012/jul/1/python-decorators-in-12-steps/
#
@app.route('/')
def index():
        return render_template("index.html")


@app.route('/anime')
def anime():
    return render_template('anime.html')


@app.route('/anime_search_result', methods=['POST'])
def anime_search_result():
    startdate = request.form['startdate']
    enddate = request.form['enddate']
    airing = request.form['air']
    producer = request.form.getlist('producer')
    licensor = request.form.getlist('licensor')
    if startdate == '': startdate = '1900-01-01'
    if enddate == '': enddate = '2019-01-01'
    query = '''
        SELECT DISTINCT A.title
        FROM anime_created_by A, producer P, anime_status_updated_by B
        WHERE A.anime_id = B.anime_id AND A.producer_id = P.producer_id AND
        A.date > DATE ('{}') AND    A.date < DATE ('{}')
        '''.format(startdate, enddate)
    if airing == 'yes': query += ' AND B.airing = True'
    elif airing == 'no': query += ' AND B.airing = False'
    if 'All' not in producer:
        producer = list(map(lambda x: "'"+x+"'", producer))
        query += ' AND (P.studio = ' + ' OR P.studio = '.join(producer) + ')'
    if 'All' not in licensor:
        licensor = list(map(lambda x:"'"+x+"'", licensor))
        query += ' AND (P.licensor = ' + ' OR P.licensor = '.join(licensor) + ')'
    #print(query)
    try:
        cursor = g.conn.execute(query)
    except Exception as e:
        return render_template("error.html",error = e)
    animeOutput = []
    for anime in cursor:
        animeOutput.append(anime[0])
    cursor.close()
    if animeOutput == []:
        return render_template('noresult.html')
    else:
        anime_score = dict()
        anime_sub = dict()
        anime_view = dict()
        for anime in animeOutput:
            query = '''     
            SELECT P.score, P.subscriptions, P.num_viewed, AVG(P.score), AVG(P.subscriptions), AVG(P.num_viewed)
            FROM popularity P, rated_by R, anime_created_by A
            WHERE P.rank = R.rank AND R.anime_id = A.anime_id AND A.title = '{}'
            GROUP BY P.score, P.subscriptions, P.num_viewed
            '''.format(anime)
            cursor = g.conn.execute(query)
            for r in cursor:
                anime_score[anime] = r[0]
                anime_sub[anime] = r[1]
                anime_view[anime] = r[2]
                avg_score = r[3]
                avg_sub = r[4]
                avg_num_viewed = r[5]
            cursor.close()
        context = dict(animeOutput = animeOutput, anime_score = anime_score, anime_sub = anime_sub, anime_view = anime_view, avg_score = avg_score, avg_sub = avg_sub, avg_num_viewed = avg_num_viewed)
        return render_template("anime_search_result.html", **context)

    
@app.route('/anime_stat')
def anime_stat():
    query = '''
        SELECT MAX(P.score), MAX(P.subscriptions), MAX(P.num_viewed)
        FROM anime_created_by A, rated_by R, popularity P
        WHERE A.anime_id = R.anime_id AND R.rank = P.rank;
        '''
    cursor = g.conn.execute(query)
    for r in cursor:
        max_score = r[0]
        max_sub = r[1]
        max_view = r[2]
    cursor.close()
    query = '''
        SELECT A.title
        FROM anime_created_by A, rated_by R, popularity P
        WHERE A.anime_id = R.anime_id AND R.rank = P.rank AND P.score = '{}';
        '''.format(max_score)
    #print(query)
    cursor = g.conn.execute(query)
    for r in cursor:
        max_score_anime = r[0]
    cursor.close()
    query = '''
        SELECT A.title
        FROM anime_created_by A, rated_by R, popularity P
        WHERE A.anime_id = R.anime_id AND R.rank = P.rank AND P.subscriptions = '{}';
        '''.format(max_sub)
    cursor = g.conn.execute(query)
    for r in cursor:
        max_sub_anime = r[0]
    cursor.close()
    query = '''
        SELECT A.title
        FROM anime_created_by A, rated_by R, popularity P
        WHERE A.anime_id = R.anime_id AND R.rank = P.rank AND P.num_viewed = '{}';
        '''.format(max_view)
    cursor = g.conn.execute(query)
    for r in cursor:
        max_view_anime = r[0]
    cursor.close()
    
    highest_score = [max_score_anime, max_score]
    largest_sub = [max_sub_anime, max_sub]
    largest_view = [max_view_anime, max_view]
    context = dict(highest_score = highest_score, largest_sub = largest_sub, largest_view = largest_view)
    return render_template('anime_stat.html', **context)


@app.route('/anime_stat_search_result', methods=['POST'])
def anime_stat_search():
    low_score = request.form['low_score']
    sub_level = request.form['sub_level']
    view_level = request.form['view_level']
    query = '''
        SELECT DISTINCT A.title
        FROM popularity P, rated_by R, anime_created_by A
        WHERE P.rank = R.rank AND R.anime_id = A.anime_id
        '''
    if not low_score == '':
        low_score = float(low_score)
        query += ' AND P.score > {}'.format(low_score)
    if not sub_level == '':
        sub_level = int(sub_level)
        query += ' AND P.subscriptions > {}'.format(sub_level)
    if not view_level == '':
        view_level = int(view_level)
        query += ' AND P.num_viewed > {}'.format(view_level)
    try:
        cursor = g.conn.execute(query)
    except Exception as e:
        return render_template("error.html",error = e)
    animeOutput = []
    for anime in cursor:
        animeOutput.append(anime[0])
    cursor.close()
    if animeOutput == []:
        return render_template('stat_noresult.html')
    else:
        anime_score = dict()
        anime_sub = dict()
        anime_view = dict()
        for anime in animeOutput:
            query = '''     
            SELECT P.score, P.subscriptions, P.num_viewed, AVG(P.score), AVG(P.subscriptions), AVG(P.num_viewed)
            FROM popularity P, rated_by R, anime_created_by A
            WHERE P.rank = R.rank AND R.anime_id = A.anime_id AND A.title = '{}'
            GROUP BY P.score, P.subscriptions, P.num_viewed
            '''.format(anime)
            cursor = g.conn.execute(query)
            for r in cursor:
                anime_score[anime] = r[0]
                anime_sub[anime] = r[1]
                anime_view[anime] = r[2]
                avg_score = r[3]
                avg_sub = r[4]
                avg_num_viewed = r[5]
            cursor.close()
        context = dict(animeOutput = animeOutput, anime_score = anime_score, anime_sub = anime_sub, anime_view = anime_view, avg_score = avg_score, avg_sub = avg_sub, avg_num_viewed = avg_num_viewed)
        return render_template("anime_stat_search_result.html", **context)
    
@app.route('/user')
def user():
    return render_template("user.html")
    
@app.route('/result2',methods =['POST'])
def result2():
    country = request.form['country']
    gender = request.form['gender']
    birth1 = request.form['birth1']
    birth2 = request.form['birth2']
    air = request.form['air']
    access_date = request.form['access_date']
    query = '''        
        SELECT U.username
        FROM user_lives_at U, location L, access C, anime_status_updated_by A1
        WHERE U.user_id = C.user_id AND L.location_id = U.location_id AND A1.anime_id = C.anime_id
        '''
    if not country == 'All':
        query += '''
         AND L.country = '{}'
        '''.format(country)
    if not gender == 'All':
        query += '''
         AND U.gender = '{}'
        '''.format(gender)
    if not air == 'All':
        query += '''
         AND A1.airing = '{}'
        '''.format(air)
    if not access_date == '':
        query += '''
          AND C.since > '{}'
         '''.format(access_date)
    if not birth1 == '':
        query += '''
         AND U.birthdate > DATE '{}'
        '''.format(birth1)
    if not birth2 == '':
        query += '''
         AND U.birthdate < DATE '{}'
        '''.format(birth2)
    query += ' GROUP BY U.username'
    #print(query)
    try:
        cursor = g.conn.execute(query)
    except Exception as e:
        return render_template("error.html",error = e)
    users = []
    for result in cursor:
        users.append(result[0])
    #print(users)
    cursor.close()
    if users == []:
        context = dict(num_user='No User', avg_anime='')
        #print('DASHFGIAYEFGIEYA')
    else:
        num_anime = 0
        num_user = len(users)
        for user in users:
            query = '''        
            SELECT COUNT(C.anime_id)
            FROM user_lives_at U, access C
            WHERE U.user_id = C.user_id AND U.username = '{}'
            '''.format(user)
            cursor = g.conn.execute(query)
            for r in cursor:
                num_anime += int(r[0])
        context = dict(num_user='Number of users: ' + str(num_user), avg_anime='Average number of animes watched:'+str(num_anime / num_user))
    return render_template("user.html", **context)


@app.route('/explore')
def explore():
    cursor = g.conn.execute('''
        SELECT DISTINCT A.title
        FROM anime_created_by A, producer P
        WHERE A.producer_id = P.producer_id AND
        A.date > DATE ('2008-11-11') AND (P.studio = 'Satelight');
        ''')
    a = []
    for result in cursor:
        a.append(result[0])
    cursor.close()
    context1 = dict(data1 = a)

    cursor = g.conn.execute('''
        SELECT COUNT(DISTINCT user_id)
        FROM access U JOIN anime_status_updated_by A ON U.anime_id = A.anime_id
        WHERE A.airing = FALSE;
        ''')
    b = []
    for result in cursor:
        b.append(result[0])
    cursor.close()
    context2 = dict(data2 = b)

    cursor = g.conn.execute('''
        SELECT AVG(P.score)
        FROM popularity P JOIN rated_by R ON P.rank = R.rank
        WHERE P.num_viewed > 10000;
        ''')
    c = []
    for result in cursor:
        c.append(result[0])
    cursor.close()
    context3 = dict(data3 = c)
    return render_template("explore.html", **context1, **context2, **context3)



# Example of adding new data to the database
@app.route('/add', methods=['POST'])
def add():
    name = request.form['name']
    g.conn.execute('INSERT INTO test VALUES (NULL, ?)', name)
    return redirect('/')


@app.route('/login')
def login():
        abort(401)
        this_is_never_executed()


if __name__ == "__main__":
    import click

    @click.command()
    @click.option('--debug', is_flag=True)
    @click.option('--threaded', is_flag=True)
    @click.argument('HOST', default='0.0.0.0')
    @click.argument('PORT', default=8111, type=int)
    def run(debug, threaded, host, port):
        """
        This function handles command line parameters.
        Run the server using:

                python server.py

        Show the help text using:

                python server.py --help

        """

        HOST, PORT = host, port
        print("running on %s:%d" % (HOST, PORT))
        app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)


    run()
