import requests
from bs4 import BeautifulSoup
import logging
import re
from collections import defaultdict
from requests.exceptions import Timeout, ConnectionError
import sys
from threading import Thread
from queue import Queue
import time


MIN_NUMBER_OF_CINEMA_SHOWS = 30
FILMS_COUNT = 10
NUMBER_OF_ASYNC_WORKERS = 10


logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)


class CinemaWorker(Thread):
    def __init__(self, queue, cinemas_list):
        Thread.__init__(self)
        self.queue = queue
        self.cinemas_list = cinemas_list

    def run(self):
        while True:
            film = self.queue.get()
            if film_is_not_arthouse(film):
                film_info = get_film_rating_and_votes_number(film)
                logger.info(film_info)
                if film_info:
                    self.cinemas_list.append(film_info)
            self.queue.task_done()


def get_top_films():
    afisha_page = fetch_afisha_page()
    films_cinemas_list = parse_afisha_list(afisha_page)
    cinemas_list_info = []
    films = get_films_list(films_cinemas_list)
    logger.info(len(films))
    queue = Queue()
    for _ in range(NUMBER_OF_ASYNC_WORKERS):
        worker = CinemaWorker(queue, cinemas_list_info)
        worker.daemon = True
        worker.start()
    for film in films:
        queue.put(film)
    queue.join()
    cinemas_list_info = sort_films_by_rating(cinemas_list_info)
    return cinemas_list_info[:FILMS_COUNT]


def fetch_afisha_page():
    return requests.get('http://www.afisha.ru/msk/schedule_cinema/').content


def count_cinema_shows(film):
    return len(film.find_all('td', {'class': 'b-td-item'}))


def get_film_title(film):
    return film.find('h3', {'class': 'usetags'}).text


def parse_page(page):
    return BeautifulSoup(page, 'lxml')


def get_film_url(film):
    return film.find('h3', {'class': 'usetags'}).a['href']


def find_film_description(film):
    return film.find('div', {'class': 'm-disp-table'}).p.text


def parse_afisha_list(raw_html):
    film_cinemas = defaultdict(dict)
    parsed_afisha_page = parse_page(raw_html)
    films_list = parsed_afisha_page.find_all('div', {'class': 'object'})
    for film in films_list:
        film_title = get_film_title(film)
        cinema_shows = count_cinema_shows(film)
        film_cinemas[film_title]['cinema_shows'] = cinema_shows
        film_cinemas[film_title]['film_url'] = get_film_url(film)
        film_cinemas[film_title]['film_description'] = find_film_description(film)
    return film_cinemas


def find_film_id_in_search_response(search_response):
    film_id = re.search(r'(?<=/film/)(\d+)', search_response.url)
    if film_id:
        return film_id.group(0)
    else:
        return None


def find_film_poster(film_id):
    iphone_poster_url = 'https://st.kp.yandex.net/images/film_iphone/iphone360_%s.jpg' % film_id
    if 'None' in iphone_poster_url:
        large_poster_url = 'https://www.kinopoisk.ru/images/film_big/%s.jpg' % film_id
        return large_poster_url
    return iphone_poster_url
    # return 'https://st.kp.yandex.net/images/film_iphone/iphone360_%s.jpg' % film_id or\
    #        'https://www.kinopoisk.ru/images/film_big/%s.jpg' % film_id


def find_rating(parsed_rating_page):
    kp_rating = parsed_rating_page.find('kp_rating')
    if kp_rating:
        return kp_rating.text
    else:
        return 0


def find_votes_number(parsed_rating_page):
    kp_rating = parsed_rating_page.find('kp_rating')
    if kp_rating:
        return kp_rating.get('num_vote')
    else:
        return None


def fetch_movie_rating_and_votes_number(kinopoisk_session, film_id):
    rating_page = kinopoisk_session.get('https://rating.kinopoisk.ru/{film_id}.xml'.format(film_id=film_id)).content
    parsed_rating_page = parse_page(rating_page)
    kp_rating = find_rating(parsed_rating_page)
    number_of_votes = find_votes_number(parsed_rating_page)
    return {'rating': kp_rating, 'votes_number': number_of_votes}


def fetch_movie_info(movie_title):
    kinopoisk_session = requests.Session()
    payload = {'first': 'yes', 'kp_query': movie_title}
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
    try:
        search_response = kinopoisk_session.get('https://www.kinopoisk.ru/index.php', params=payload, headers=headers,
                                                timeout=10)
    except (Timeout, ConnectionError):
        return
    film_id = find_film_id_in_search_response(search_response)
    film_stats = fetch_movie_rating_and_votes_number(kinopoisk_session, film_id)
    poster_url = find_film_poster(film_id)
    film_stats['poster_url'] = poster_url
    return film_stats


def get_film_rating_and_votes_number(film):
    rating_and_votes_number = fetch_movie_info(film[0])
    if rating_and_votes_number is None:
        return
    film[1]['rating'] = rating_and_votes_number['rating']
    film[1]['votes_number'] = rating_and_votes_number['votes_number']
    film[1]['poster_url'] = rating_and_votes_number['poster_url']
    return film


def get_films_list(films):
    films_list = []
    for film in films.items():
        films_list.append(film)
    return films_list


def get_films_rating_and_votes_number(films):
    for film_title in list(films.keys()):
        rating_and_votes_number = fetch_movie_info(film_title)
        films[film_title]['rating'] = rating_and_votes_number['rating']
        films[film_title]['votes_number'] = rating_and_votes_number['votes_number']
    return films


def sort_films_by_rating(films_list):
    return sorted(films_list, key=lambda film: float(film[1]['rating']), reverse=True)


def film_is_not_arthouse(film):
    return film[1]['cinema_shows'] > MIN_NUMBER_OF_CINEMA_SHOWS


def exclude_arthouse_films(films_list):
    films_list_without_arthouse = []
    for film in films_list:
        if film_is_not_arthouse(film):
            films_list_without_arthouse.append(film)
    return films_list_without_arthouse


def output_movies_to_console(films):
    for film in films[:FILMS_COUNT]:
        print('Title: {0} | Rating: {1} | Votes number: {2} | Cinema shows: {3}'.format(film[0],
                                                                                        film[1]['rating'],
                                                                                        film[1]['votes_number'],
                                                                                        film[1]['cinema_shows']))


if __name__ == '__main__':
    start_time = time.time()
    afisha_page = fetch_afisha_page()
    films_cinemas_list = parse_afisha_list(afisha_page)
    films_list = get_films_list(films_cinemas_list)
    list_info = sort_films_by_rating(get_top_films())
    print(time.time() - start_time)

