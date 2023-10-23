import uuid
import random

movies = [{
    'id': str(uuid.uuid4()),
    'imdb_rating': round(random.random() * 10, 1),
    'genre': [
        {'id': '789', 'name': 'Action'},
        {'id': '546', 'name': 'Music Story'}
    ],
    'title': 'The Star',
    'description': 'New World',
    'directors': [
        {'id': '1', 'name': 'Jack Jones'},
        {'id': '2', 'name': 'Steven Spielberg'}
    ],
    'actors_names': ['Jack Jones', 'Robbie Williams'],
    'writers_names': ['Jack Jones', 'Serena Williams'],
    'actors': [
        {'id': '1', 'name': 'Jack Jones'},
        {'id': '3', 'name': 'Robbie Williams'}
    ],
    'writers': [
        {'id': '1', 'name': 'Jack Jones'},
        {'id': '4', 'name': 'Serena Williams'}
    ],
} for _ in range(60)]

genres = [
    {
        'id': '789',
        'name': 'Action',
        'description': 'Action description'
    },
    {
        'id': '123',
        'name': 'Fantasy',
        'description': 'Fantasy description!'
    },
    {
        'id': '456',
        'name': 'Music Story',
        'description': 'Music description'
    }
]

persons = [
    {
        'id': '1',
        'full_name': 'Jack Jones',
    },
    {
        'id': '2',
        'full_name': 'Steven Spielberg',
    },
    {
        'id': '3',
        'full_name': 'Robbie Williams'
    },
    {
        'id': '4',
        'full_name': 'Serena Williams'
    },
    {
        'id': '5',
        'full_name': 'Lev Tolstoj'
    },
    {
        'id': '6',
        'full_name': 'Gabriel Garcia Markes'
    }
]

data = {'movies': movies, 'genres': genres, 'persons': persons}

