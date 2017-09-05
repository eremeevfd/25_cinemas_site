from flask import Flask, render_template
from werkzeug.contrib.cache import FileSystemCache
from cinemas import get_top_films
import os
import threading


app = Flask(__name__)
lock = threading.Lock()
app.config['DEBUG'] = True


@app.route('/')
def films_list():
    current_dir = os.path.dirname(__file__)
    cwd = os.getcwd()
    cache_dir = (current_dir or cwd) + '/cache/'
    cached_films = FileSystemCache(cache_dir=cache_dir, default_timeout=12 * 60 * 60)
    if not cached_films.get('cinemas'):
        with lock:
            cinemas = get_top_films()
        cached_films.set('cinemas', cinemas)
    return render_template('films_list.html', cinemas=cached_films.get('cinemas'))

if __name__ == "__main__":
    empty_list_info = []
    app.run()
