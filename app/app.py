from flask import Flask, request, redirect
from flask_mysqldb import MySQL
from flask_apscheduler import APScheduler
from datetime import datetime, timedelta
import random, string, re
import redis

app = Flask(__name__)

app.config['MYSQL_HOST'] = 'mysql-db'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'superSecretETMLPassword'
app.config['MYSQL_DB'] = 'urldb'
domain = "localhost:5000"
url_regex = "^https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)$"

mysql = MySQL(app)
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()


def generate_short_id(size=6):
    chars = string.ascii_lowercase + string.digits
    # Generate Random id
    while True:
        short_id = ''.join(random.choice(chars) for _ in range(size))
        # Check random id not already exist
        if not r.exists(f"url:{short_id}"):
            cursor = mysql.connection.cursor()
            cursor.execute("SELECT 1 FROM url_table WHERE shortUrl LIKE %s", (f"%/{short_id}",))
            exists = cursor.fetchone()
            cursor.close()
            if not exists:
                return short_id

@app.route('/', methods=['POST'])
def create_short_url () :
    # Check url format
    url = request.form.get('url')

    expire_time = request.form.get('expire', type=int)
    if expire_time is None or expire_time <= 0:
        expire_date = None
    else:
        expire_date = datetime.now() + timedelta(seconds=expire_time)

    if not re.match(url_regex, url) :
        return "Url invalid, please retry."
    
    # Generate short url
    short_id = generate_short_id()
    short_url = f'http://{domain}/{short_id}'

    # Store url
    cursor = mysql.connection.cursor()
    cursor.execute('INSERT INTO url_table (originalUrl, shortUrl, expireDate) VALUES (%s, %s, %s)', (url, short_url, expire_date))
    mysql.connection.commit()
    cursor.close()

    # Add url to cache (redis)
    r.hset(f"url:{short_id}", mapping={"originalUrl": url, "clicks": 0})
    if expire_date:
        r.expireat(f"url:{short_id}", int(expire_date.timestamp()))

    return short_url

@app.route('/<short_id>')
def redirector (short_id) :
    # Check Redis cache
    key = f"url:{short_id}"
    if r.exists(key):
        original_url = r.hget(key, "originalUrl")
        # Redirect
        r.hincrby(key, "clicks", 1)
        return redirect(original_url)
    # Check mysql db
    cursor = mysql.connection.cursor()
    cursor.execute("SELECT originalUrl, clickCounter, expireDate FROM url_table WHERE shortUrl LIKE %s", (f"%/{short_id}",))
    exists = cursor.fetchone()
    cursor.close()
    if exists and (exists[2] is None or exists[2] > datetime.now()):
        original_url = exists[0]
        click_counter = exists[1]
        expire_date = exists[2]
        expire_timestamp = int(expire_date.timestamp()) if expire_date else None

        # Update redis cache
        r.hset(key, mapping={"originalUrl": original_url, "clicks": click_counter})
        if expire_timestamp:
            r.expireat(key, expire_timestamp)
        r.hincrby(key, "clicks", 1)
        # Redirect
        return redirect(original_url)
    elif exists and (exists[2] < datetime.now()) :
        cursor = mysql.connection.cursor()
        cursor.execute("DELETE FROM url_table WHERE shortUrl LIKE %s", (f"%/{short_id}",))
        mysql.connection.commit()
        cursor.close()
        return "URL expired", 404
    return "URL not found", 404


@scheduler.task('interval', id='do_snyc_stats', hours=1)
def sync_stats():
    with app.app_context():
        # Get stats from redis
        for key in r.scan_iter('url:*'):
            click_counter = r.hget(key, 'clicks')
            short_id = key.split(":")[1]
            # Store stats to mysql
            cursor = mysql.connection.cursor()
            cursor.execute(
                "UPDATE url_table SET clickCounter = %s WHERE shortUrl LIKE %s",
                (click_counter, f"%/{short_id}")
            )
            mysql.connection.commit()
            cursor.close()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
